const debounce = require('debounceify')
const safetyCatch = require('safety-catch')

const { OutputNode } = require('./nodes')
const promises = Symbol.for('hypercore.promises')

class Linearizer {
  constructor (output) {
    this.output = output
    this.added = 0
    this.removed = 0
    this.changes = []
    this.baseLength = output.length
  }

  async _head () {
    const length = this.baseLength - this.removed
    const node = length > 0 ? await this.output.get(length - 1) : null
    if (Buffer.isBuffer(node)) return OutputNode.decode(node)
    return node
  }

  async update (node, latestClock) {
    if (this.baseLength === 0) {
      this.added++
      this.changes.push(node)
      return false
    }

    let head = await this._head()
    if (head && node.eq(head)) { // Already indexed
      return true
    }

    while (head && !node.eq(head) && (head.contains(node) || !latestClock.has(head.id))) {
      this.removed++
      head = await this._head()
    }

    if (head && node.eq(head)) return true

    this.added++
    this.changes.push(node)

    return false
  }
}

module.exports = class LinearizedView {
  constructor (autobase, outputs, opts = {}) {
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

    this._bestOutput = null
    this._bestOutputLength = 0
    this._lastLinearizer = null
    this._changes = []

    this.update = debounce(this._update.bind(this))

    this.opened = false
    this._opening = null
    this._opening = this.ready()
    this._opening.catch(noop)
  }

  get status () {
    if (!this._lastLinearizer) return {}
    return {
      added: this._lastLinearizer.added,
      removed: this._lastLinearizer.removed
    }
  }

  get length () {
    return this._bestOutputLength + this._changes.length
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
        this._autocommit = true
        this.writable = true
        break
      }
    }

    if (this._autocommit === undefined) this._autocommit = false

    for (const output of this.outputs) {
      if (!this._bestOutput || this._bestOutputLength < output.length) {
        this._bestOutput = output
        this._bestOutputLength = output.length
      }
    }

    this.opened = true
  }

  async _update () {
    if (!this.opened) await this._opening
    await Promise.all(this.outputs.map(i => i.update()))

    const latestClock = await this.autobase.latest()

    // TODO: Short-circuit if no work to do

    const linearizers = []
    // If we're not autocommmitting, include this index because it's a memory-only view.
    const outputs = this._autocommit ? this.outputs : [...this.outputs, this]

    for (const output of outputs) {
      linearizers.push(new Linearizer(output))
    }

    this._lastLinearizer = await bestLinearizer(this.autobase.createCausalStream(), linearizers, latestClock)
    this._bestOutput = this._lastLinearizer.output
    this._bestOutputLength = this._bestOutput.length - this._lastLinearizer.removed

    this._changes = []
    let batch = []

    for (let i = this._lastLinearizer.changes.length - 1; i >= 0; i--) {
      const node = this._lastLinearizer.changes[i]
      batch.push(node)
      if (node.batch[1] > 0) continue
      this._applying = batch[batch.length - 1]

      // TODO: Make sure the input clock is the right one to pass to _apply
      const inputNode = await this.autobase._getInputNode(node.change, this._applying.seq)
      const clocks = {
        local: inputNode.clock,
        global: this._applying.clock
      }

      const start = this._changes.length

      try {
        await this._apply(batch, clocks, node.change, this)
      } catch (err) {
        safetyCatch(err)
      }

      for (let j = start; j < this._changes.length; j++) {
        const change = this._changes[j]
        change.batch[0] = j - start
        change.batch[1] = this._changes.length - j - 1
      }

      this._applying = null
      batch = []
    }
    if (batch.length) throw new Error('Cannot rebase: partial batch in index')

    if (this._autocommit) return this.commit()
  }

  async _get (seq, opts) {
    return (seq < this._bestOutputLength)
      ? OutputNode.decode(await this._bestOutput.get(seq, { ...opts, valueEncoding: null }))
      : this._changes[seq - this._bestOutputLength]
  }

  async get (seq, opts) {
    if (!this.opened) await this._opening
    if (!this._bestOutput) await this.update(opts)

    let block = await this._get(seq, opts)

    // TODO: support OOB gets
    if (!block) throw new Error('Out of bounds gets are currently not supported')

    if (!this._unwrap) return block
    block = block.value

    if (opts && opts.valueEncoding) block = opts.valueEncoding.decode(block)

    return block
  }

  async append (block) {
    if (!this.opened) await this._opening
    if (!this._applying) throw new Error('Cannot append to a RebasedHypercore outside of an update operation.')
    if (!Array.isArray(block)) block = [block]

    for (const val of block) {
      const node = new OutputNode({
        value: val,
        batch: [0, 0],
        clock: this._applying.clock,
        change: this._applying.change
      })
      this._changes.push(node)
    }

    return this.length
  }

  // TODO: This should all be atomic
  async commit () {
    if (!this._bestOutput.writable) throw new Error('Can only commit to a writable index')
    if (!this.opened) await this._opening

    if (this._bestOutputLength < this._bestOutput.length) {
      await this._bestOutput.truncate(this._bestOutput.length - this._lastLinearizer.removed)
    }
    if (this._bestOutput.length === 0 && this._changes.length) {
      this._changes[0].header = this._header
    }

    await this._bestOutput.append(this._changes.map(OutputNode.encode))

    this._bestOutputLength = this._bestOutput.length
    this._changes = []
  }
}

function defaultApply (batch, clock, change, output) {
  return output.append(batch.map(b => b.value))
}

async function bestLinearizer (causalStream, linearizers, latestClock) {
  for await (const inputNode of causalStream) {
    for (const linearizer of linearizers) {
      if (await linearizer.update(inputNode, latestClock)) return linearizer
    }
  }
  return linearizers[0]
}

function noop () { }
