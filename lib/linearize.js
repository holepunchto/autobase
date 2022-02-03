const { EventEmitter } = require('events')
const debounce = require('debounceify')
const safetyCatch = require('safety-catch')

const { OutputNode } = require('./nodes')
const { eq, lte, length } = require('./clock')
const promises = Symbol.for('hypercore.promises')

class OutputAncestors {
  constructor (output) {
    this.state = Ancestors.UNINITIALIZED
    this.output = output
    this.length = 0
    this.head = null
    this.opened = false
    this._snapshot = null
  }

  async open () {
    if (this.opened) return
    this._snapshot = this.output.snapshot()
    this.head = await this._snapshot.get(this._snapshot.length - 1)
    this.opened = true
  }

  get done () {
    return this.state === Ancestors.VALID || this.state === Ancestors.INVALID
  }

  get (seq, opts) {
    if (!this.done || (seq < 0 || seq >= this.length)) return null
    return this._snapshot.get(seq, opts)
  }

  close () {
    if (!this._snapshot) return Promise.resolve()
    return this._snapshot.close()
  }

  async update (target) {
    if (this.done) return
    if (eq(this._head.clock, target)) {
      this.length = this._snapshot.length
      this.state = Ancestors.VALID
    } else if (!lte(target, this._head.clock)) {
      this.state = Ancestors.IN_PROGRESS
    } else {
      const intersection = length(target)
      if (intersection === 0) {
        this.state = Ancestors.INVALID
        return
      }
      const node = await this._snapshot.get(intersection - 1)
      this.head = node
      if (eq(node.clock, target)) {
        this.length = intersection
        this.state = Ancestors.VALID
      } else {
        this.state = Ancestors.IN_PROGRESS
      }
    }
  }
}

class Ancestors {
  constructor (outputs) {
    this.state = Ancestors.UNINITIALIZED
    this._allAncestors = outputs.map(o => new OutputAncestors(o))
    this._bestAncestors = null
  }

  get done () {
    return this.state === Ancestors.VALID || this.state === Ancestors.INVALID
  }

  get length () {
    return this._bestAncestors ? this._bestAncestors.length : 0
  }

  get truncationLength () {
    if (!this._bestAncestors) return 0
    return this._bestAncestors.output.length - this._bestAncestors.length
  }

  open () {
    return Promise.all(this._allAncestors.map(a => a.open()))
  }

  close () {
    return Promise.all(this._allAncestors.map(a => a.close()))
  }

  async get (seq, opts = {}) {
    if (!this._bestAncestors || seq < 0 || seq >= this.length) return null
    return this._bestAncestors.get(seq, opts)
  }

  isHead (clock) {
    for (const ancestor of this._allAncestors) {
      if (eq(ancestor.head, clock)) return true
    }
    return false
  }

  async update (target) {
    if (this.done) return
    await Promise.all(this._allAncestors.map(a => a.update(target)))
    for (const ancestors of this._allAncestors) {
      if (ancestors.state === Ancestors.VALID) {
        this.state = Ancestors.VALID
        this._bestAncestors = ancestors
        break
      }
      if (ancestors.state === Ancestors.IN_PROGRESS) {
        this.state = Ancestors.IN_PROGRESS
      } else if (ancestors.state === Ancestors.INVALID) {
        if (this.state !== Ancestors.IN_PROGRESS) {
          this.state = Ancestors.INVALID
        }
      }
    }
  }
}
Ancestors.UNINITIALIZED = 0
Ancestors.IN_PROGRESS = 1
Ancestors.VALID = 2
Ancestors.INVALID = 3

module.exports = class LinearizedCore extends EventEmitter {
  constructor (autobase, opts = {}) {
    super()
    this[promises] = true
    this.autobase = autobase
    this.view = opts.view ? opts.view(this) : this
    this._rebuildDebounced = debounce(this._rebuild.bind(this))

    this._view = opts.view
    this._snapshot = opts.snapshot
    this._clock = this._snapshot
    this._ancestors = null

    this._apply = opts.apply

    this._nodes = []
    this._pending = []
    this._applying = null

    this.status = {
      added: 0,
      removed: 0
    }
  }

  get byteLength () {
    return 0 // TODO: Implement?
  }

  get length () {
    return this._ancestors ? this._ancestors.length + this._nodes.length + this._pending.length : 0
  }

  _selectOutputs () {
    if (!this._snapshot) return this.autobase.localOutput ? [this.autobase.localOutput] : this.autobase.outputs
    return this.autobase.localOutput ? [this.autobase.localOutput, ...this.autobase.outputs] : this.autobase.outputs
  }

  async _flush () {
    if (this._snapshot || !this.autobase.localOutput || !this._nodes.length) return

    // TODO: These two steps should ideally be atomic
    if (this.autobase.localOutput.length !== this._ancestors.length) {
      await this.autobase.localOutput.truncate(this._ancestors.length)
    }
    await this.autobase.localOutput.append(this._nodes)

    this._nodes = []
    await this._rebuild() // TODO: Better way to update this._ancestors?
  }

  async update () {
    await Promise.all(this.autobase.outputs.map(o => o.update()))
    if (this._snapshot) return

    const latest = await this.autobase.latest()
    if (eq(this._clock, latest)) return

    this._clock = latest
    await this._rebuildDebounced()
  }

  async _applyPending (pending) {
    const batches = []
    let batch = []
    while (pending.length) {
      const node = pending.pop()
      batch.push(node)
      if (node.batch[1] > 0) continue
      batches.push(batch)
      batch = []
    }
    if (pending.length) throw new Error('Cannot rebase: partial batch in index')

    const batchClocks = await Promise.all(batches.map(b => {
      const node = b[b.length - 1]
      const inputNode = this.autobase._getInputNode(node.change, node.seq)
      return {
        local: inputNode.clock,
        global: node.clock
      }
    }))

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i]
      const clocks = batchClocks[i]
      const node = batch[batch.length - 1]

      const start = this._pending.length
      this._applying = node
      try {
        await this._apply(this._view, batch, clocks, node.change)
      } catch (err) {
        safetyCatch(err)
      }
      for (let j = start; j < this._pending.length; j++) {
        const change = this._pending[j]
        change.batch[0] = j - start
        change.batch[1] = this._pending.length - j - 1
      }
      this._applying = null
    }

    if (this._pending.length && !this._ancestors.length && !this._nodes.length) {
      this._pending[0].header = this._header
    }
  }

  async _rebuildMemory (ancestors, pending) {
    const head = this._nodes[0]
    if (ancestors.isHead(head.clock)) {
      this._nodes = []
      return this._rebuild()
    }
    for await (const node of this.autobase.createCausalStream({ clock: this._clock })) {
      const head = this._nodes[this._nodes.length - 1]
      if (eq(head.clock, node.clock)) break
      if (!lte(head.clock, node.clock)) {
        this.status.removed++
        this._nodes.pop()
      }
      if (!this._nodes.length) return this._rebuild()
      this.status.added++
      pending.push(node)
    }
  }

  async _rebuildAncestors (ancestors, pending) {
    for await (const node of this.autobase.createCausalStream({ clock: this._clock })) {
      await ancestors.update(node.clock)
      if (ancestors.state === Ancestors.VALID) break
      this.status.added++
      pending.push(node)
    }
  }

  async _rebuild () {
    this.status = { added: 0, removed: 0 }
    const ancestors = new Ancestors(this._selectOutputs())
    await ancestors.open()

    const pending = []
    if (this._nodes.length) {
      await this._rebuildMemory(ancestors, pending)
    } else {
      await this._rebuildAncestors(ancestors, pending)
    }

    if (this._ancestors) await this._ancestors.close()
    this._ancestors = ancestors
    this.status.removed += this._ancestors.truncationLength

    // TODO: Run nodes through apply before pushing into nodes
    await this._applyPending(pending)
    this._nodes.push(...this._pending)
    this._pending = []

    await this._flush()
  }

  async _get (seq, opts) {
    if (seq >= (this._ancestors.length + this._nodes.length)) {
      return this._pending[seq - this._ancestors.length - this._nodes.length]
    }
    if (seq >= this._ancestors.length) {
      return this._nodes[seq - this._ancestors.length]
    }

    // If the remote outputs have truncated, this might be null -- rebuild and try again
    const block = await this._ancestors.get(seq)
    if (block) return block

    this._nodes = []
    this._pending = []
    await this._rebuildDebounced()
    return this._get(seq, opts)
  }

  async get (seq, opts = {}) {
    if (!this._ancestors) await this._rebuildDebounced()

    let block = await this._get(seq, opts)
    if (block && this._unwrap) block = block.value

    return (opts && opts.valueEncoding) ? opts.valueEncoding.decode(block) : block
  }

  append (block) {
    if (!this._applying) throw new Error('Cannot append to a LinearizedCore outside of an update operation.')
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

  unwrap () {
    return new LinearizedCore(this.autobase, {
      unwrap: false
    })
  }

  wrap () {
    return new LinearizedCore(this.autobase, {
      unwrap: true
    })
  }

  snapshot () {
    return new LinearizedCore(this.autobase, {
      snapshot: this.clock
    })
  }

  close () {

  }
}
