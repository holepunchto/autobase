const { EventEmitter } = require('events')
const debounce = require('debounceify')
const safetyCatch = require('safety-catch')
const c = require('compact-encoding')

const KeyCompressor = require('./compression')
const { OutputNode } = require('./nodes')
const { Node: NodeSchema } = require('./nodes/messages')

const promises = Symbol.for('hypercore.promises')

class Linearizer {
  constructor (autobase, output, opts = {}) {
    this.autobase = autobase
    this.output = output
    this.outputLength = output.length

    this.committed = []
    this.pending = []
    this.outputOffset = 0
    this.status = {
      added: 0,
      removed: 0
    }
    this.invalidated = false

    this._header = opts.header
    this._apply = opts.apply
    this._persist = opts.persist
    this._keyCompressor = new KeyCompressor(this.output)
  }

  get committedLength () {
    return this.outputLength + this.committed.length - this.outputOffset
  }

  get length () {
    return this.committedLength + this.pending.length
  }

  _onTruncate (length) {
    if (this.output.writable || length >= this.outputLength) return
    this.invalidated = true
  }

  async get (seq, opts) {
    if (seq < 0 || seq > (this.length - 1)) return null

    let node = null
    if (seq < this.committedLength) {
      const offset = this.outputLength - this.outputOffset
      if (seq > offset - 1) {
        node = this.committed[seq - offset]
      } else {
        node = await this.output.get(seq, { ...opts, valueEncoding: null })
      }
    } else {
      node = this.pending[seq - this.committedLength]
    }

    if (!Buffer.isBuffer(node)) return node

    const state = { start: 0, end: node.length, buffer: node }
    const decoded = NodeSchema.decode(state)
    const [clock, change] = await Promise.all([
      this._keyCompressor.decompress(decoded.clock, seq),
      this._keyCompressor.resolvePointer(decoded.change)
    ])
    decoded.clock = clock
    decoded.change = change

    return new OutputNode(decoded)
  }

  async _update () {
    const latestClock = await this.autobase.latest()
    if (this.invalidated) return null

    if (!this._keyCompressor.initialized) {
      await this.get(this.ouputLength - 1) // This will initialize the compressor
      if (this.invalidated) return null
    }

    const nodes = []
    let removed = 0

    for await (const node of this.autobase.createCausalStream()) {
      if (this.invalidated) return null
      if (this.committedLength === 0) {
        nodes.push(node)
        continue
      }

      let head = await this.get(this.committedLength - removed - 1)
      if (this.invalidated) return null
      if (head && node.eq(head)) break

      while (head && !node.eq(head) && (head.contains(node) || !latestClock.has(head.id))) {
        head = await this.get(this.committedLength - ++removed - 1)
        if (this.invalidated) return null
      }
      if (head && node.eq(head)) break

      nodes.push(node)
    }

    if (removed > 0) {
      if (removed < this.committed.length) {
        this.committed = this.committed.slice(0, this.committed.length - removed)
      } else {
        this.outputOffset += removed - this.committed.length
        this.committed = []
      }
    }

    const status = {
      added: nodes.length,
      removed
    }
    return { nodes, status }
  }

  async _appendCommitted () {
    const batch = []
    for (let i = 0; i < this.committed.length; i++) {
      const node = this.committed[i]
      const { keys, clock } = await this._keyCompressor.compress(node.clock, this.output.length + i)
      node.keys = keys
      node.clock = clock
      const resolvedChange = this._keyCompressor.resolveKey(node.change)
      if (resolvedChange) {
        node.change = resolvedChange
      } else {
        node.keys.push(node.change)
        node.change = { seq: this.output.length + i, offset: node.keys.length }
      }
      batch.push(c.encode(NodeSchema, node))
    }
    await this.output.append(batch)
    this.outputLength = this.output.length
    this.committed = []
  }

  async update () {
    const updated = await this._update()
    if (!updated) return null

    const { status, nodes } = updated
    let batch = []

    while (nodes.length) {
      const node = nodes.pop()
      batch.push(node)
      if (node.batch[1] > 0) continue
      this._applying = node

      // TODO: Make sure the input clock is the right one to pass to _apply
      const inputNode = await this.autobase._getInputNode(node.change, node.seq)
      if (this.invalidated) return null

      const clocks = {
        local: inputNode.clock,
        global: this._applying.clock
      }
      this._applying = node
      const start = this.pending.length

      try {
        await this._apply(batch, clocks, node.change, this)
      } catch (err) {
        safetyCatch(err)
      }
      if (this.invalidated) return null

      for (let j = start; j < this.pending.length; j++) {
        const change = this.pending[j]
        change.batch[0] = j - start
        change.batch[1] = this.pending.length - j - 1
      }

      this._applying = null
      batch = []
    }
    if (batch.length) throw new Error('Cannot rebase: partial batch in index')

    if ((this.outputOffset === this.outputLength) && this.pending.length) {
      this.pending[0].header = this._header
    }

    this.committed.push(...this.pending)
    this.pending = []

    if (this._persist && this.output.writable) {
      if (this.outputOffset > 0) {
        await this.output.truncate(this.output.length - this.outputOffset)
        this._keyCompressor.truncate(this.output.length)
        this.outputOffset = 0
      }
      await this._appendCommitted()
    }

    this.status = status
  }

  async append (block) {
    if (!Array.isArray(block)) block = [block]
    const nodes = []
    for (const val of block) {
      const node = new OutputNode({
        value: val,
        batch: [0, 0],
        clock: this._applying.clock,
        change: this._applying.change
      })
      nodes.push(node)
    }
    this.pending.push(...nodes)
  }

  reset () {
    this.committed = []
    this.pending = []
  }
}

module.exports = class LinearizedView extends EventEmitter {
  constructor (autobase, opts = {}) {
    super()
    this[promises] = true
    this.autobase = autobase

    this._apply = opts.apply || defaultApply
    this._header = opts.header
    this._unwrap = opts.unwrap
    this._applying = null

    this._linearizersByOutput = new Map()
    this._activeOutputs = new Set()
    this._bestLinearizer = null
    this._updating = false
    this.update = debounce(this._update.bind(this))

    this.ready = () => this.autobase.ready()
  }

  get status () {
    return this._bestLinearizer && this._bestLinearizer.status
  }

  get length () {
    return this._bestLinearizer ? this._bestLinearizer.length : 0
  }

  get byteLength () {
    // TODO: This is hard and probably not worth it to implement
    return 0
  }

  get writable () {
    return !!this.autobase.localOutput
  }

  _onOutputTruncated (output, length) {
    const linearizer = this._linearizersByOutput.get(output)
    if (!linearizer) return
    linearizer._onTruncate(length)
  }

  _refreshLinearizers () {
    if (this.writable) {
      if (this._linearizersByOutput.has(this.autobase.localOutput)) return
      this._linearizersByOutput.set(this.autobase.localOutput, new Linearizer(this.autobase, this.autobase.localOutput, {
        header: this._header,
        apply: this._apply,
        persist: true
      }))
      return
    }

    const activeOutputs = new Set()
    for (const output of this.autobase.outputs) {
      activeOutputs.add(output)

      const existing = this._linearizersByOutput.get(output)
      if (existing && !existing.invalidated && existing.length > output.length) continue

      this._linearizersByOutput.set(output, new Linearizer(this.autobase, output, {
        header: this._header,
        apply: this._apply,
        persist: false
      }))
    }

    for (const output of this._activeOutputs) {
      if (activeOutputs.has(output)) continue
      this._linearizersByOutput.delete(output)
    }
    this._activeOutputs = activeOutputs
  }

  async _update () {
    await this.ready()

    await Promise.all(this.autobase.outputs.map(i => i.update()))
    await this._refreshLinearizers()

    let bestOutput = this.autobase.localOutput
    if (!bestOutput) {
      for (const output of this.autobase.outputs) {
        if (!bestOutput || (output.length > bestOutput.length)) {
          bestOutput = output
        }
      }
    }
    if (!bestOutput) return

    this._updating = true
    this._bestLinearizer = this._linearizersByOutput.get(bestOutput)
    await this._bestLinearizer.update()
    this._updating = false

    if (this._bestLinearizer.invalidated) return this._update()

    for (const linearizer of this._linearizersByOutput.values()) {
      if (linearizer === this._bestLinearizer) continue
      linearizer.reset()
    }
  }

  async get (seq, opts) {
    await this.ready()
    if (!this._bestLinearizer) await this.update(opts)

    const block = await this._bestLinearizer.get(seq, opts)
    if (!this._unwrap) return block

    return (opts && opts.valueEncoding) ? opts.valueEncoding.decode(block.value) : block.value
  }

  async append (block) {
    await this.ready()
    if (!this._updating) throw new Error('Cannot append to a RebasedHypercore outside of an update operation.')
    await this._bestLinearizer.append(block)
    return this.length
  }
}

function defaultApply (batch, clock, change, output) {
  return output.append(batch.map(b => b.value))
}
