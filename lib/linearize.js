const { EventEmitter } = require('events')
const debounce = require('debounceify')
const safetyCatch = require('safety-catch')

const { OutputNode } = require('./nodes')
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
    this.closed = false
    this.invalidated = false

    this._ontruncate = this._onTruncate.bind(this)
    this.output.on('truncate', this._ontruncate)

    this._header = opts.header
    this._apply = opts.apply
    this._persist = opts.persist
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

    return Buffer.isBuffer(node) ? OutputNode.decode(node) : node
  }

  async _update () {
    const latestClock = await this.autobase.latest()
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
        this.outputOffset = 0
      }
      await this.output.append(this.committed.map(OutputNode.encode))
      this.outputLength = this.output.length
      this.committed = []
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

  close () {
    if (this.closed) return
    this.closed = true
    this.output.removeListener('truncate', this._ontruncate)
  }
}

module.exports = class LinearizedView extends EventEmitter {
  constructor (autobase, outputs, opts = {}) {
    super()
    this[promises] = true
    this.autobase = autobase
    this.outputs = null
    this.writable = false

    this._outputs = outputs
    this._apply = opts.apply || defaultApply
    this._unwrap = !!opts.unwrap
    this._autocommit = opts.autocommit
    this._header = opts.header
    this._applying = null

    this._linearizersByOutput = new Map()
    this._activeOutputs = new Set()
    this._bestLinearizer = null
    this._updating = false

    this.update = debounce(this._update.bind(this))

    this.closed = false
    this.opened = false
    this._opening = null
    this._opening = this.ready()
    this._opening.catch(noop)
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

  async ready () {
    if (this._opening) return this._opening

    this.outputs = await this._outputs
    if (!Array.isArray(this.outputs)) this.outputs = [this.outputs]
    await Promise.all(this.outputs.map(i => i.ready()))

    for (const output of this.outputs) {
      if (output.writable && this._autocommit !== false) {
        this.outputs = [output] // If you pass in a writable output, remote ones are ignored.
        this.writable = true
        this._autocommit = true
        break
      }
    }
    if (this._autocommit === undefined) this._autocommit = false

    this.opened = true
  }

  _refreshLinearizers () {
    const activeOutputs = new Set()

    for (const output of this.outputs) {
      activeOutputs.add(output)

      const existing = this._linearizersByOutput.get(output)
      if (existing && !existing.invalidated && existing.length > output.length) continue
      if (existing) existing.close()

      this._linearizersByOutput.set(output, new Linearizer(this.autobase, output, {
        header: this._header,
        apply: this._apply,
        persist: this._autocommit
      }))
    }

    for (const output of this._activeOutputs) {
      if (activeOutputs.has(output)) continue
      const linearizer = this._linearizersByOutput.get(output)
      linearizer.close()
      this._linearizersByOutput.delete(output)
    }
    this._activeOutputs = activeOutputs
  }

  async _update () {
    if (!this.opened) await this._opening

    await Promise.all(this.outputs.map(i => i.update()))
    await this._refreshLinearizers()

    let bestOutput = null
    for (const output of this.outputs) {
      if (!bestOutput || (output.length > bestOutput.length)) {
        bestOutput = output
      }
    }

    this._bestLinearizer = this._linearizersByOutput.get(bestOutput)
    for (const linearizer of this._linearizersByOutput.values()) {
      if (linearizer === this._bestLinearizer) continue
      linearizer.reset()
    }

    this._updating = true
    await this._bestLinearizer.update()
    this._updating = false

    if (this._bestLinearizer.invalidated) return this._update()
  }

  async get (seq, opts) {
    if (!this.opened) await this._opening
    if (!this._bestLinearizer) await this.update(opts)

    const block = await this._bestLinearizer.get(seq, opts)
    if (!this._unwrap) return block

    return (opts && opts.valueEncoding) ? opts.valueEncoding.decode(block.value) : block.value
  }

  async append (block) {
    if (!this.opened) await this._opening
    if (!this._updating) throw new Error('Cannot append to a RebasedHypercore outside of an update operation.')
    await this._bestLinearizer.append(block)
    return this.length
  }

  close () {
    if (this.closed) return
    this.closed = true
    for (const linearizer of this._linearizersByOutput.values()) {
      linearizer.close()
    }
    this.emit('close')
  }
}

function defaultApply (batch, clock, change, output) {
  return output.append(batch.map(b => b.value))
}

function noop () { }
