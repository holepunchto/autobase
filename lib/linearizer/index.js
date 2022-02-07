const { EventEmitter } = require('events')
const debounce = require('debounceify')
const safetyCatch = require('safety-catch')

const { OutputNode } = require('./nodes')
const { eq, lte, length } = require('./clock')
const promises = Symbol.for('hypercore.promises')

const INIT = 0
const IN_PROGRESS = 1
const VALID = 2
const INVALID = 3

class CheckoutFinder {
  constructor (ancestors) {
    this.state = INIT
    this.length = ancestors.snapshot.length
    this.head = ancestors.head
    this.ancestors = ancestors
  }

  get done () {
    return this.state === VALID || this.state === INVALID
  }

  async update (target) {
    if (this.done) return
    if (!this.head) {
      this.state = INVALID
      return
    }
    if (eq(this.head.clock, target)) {
      this.state = VALID
    } else if (!lte(target, this.head.clock)) {
      this.state = IN_PROGRESS
    } else {
      const intersection = length(target)
      if (intersection === 0) {
        this.state = INVALID
        return
      }
      const node = await this.ancestors.snapshot.get(intersection - 1)
      this.head = node
      if (eq(node.clock, target)) {
        this.length = intersection
        this.state = VALID
      } else {
        this.state = IN_PROGRESS
      }
    }
  }

  commit () {
    this.ancestors.length = this.length
    this.ancestors.head = this.head
  }
}

class Ancestors {
  constructor (output) {
    this.output = output
    this.length = 0
    this.head = null
    this.opened = false
    this.snapshot = null
  }

  async open () {
    if (this.opened) return
    this.snapshot = this.output.snapshot()
    this.head = this.snapshot.length ? await this.snapshot.get(this.snapshot.length - 1) : null
    this.opened = true
  }

  get (seq, opts) {
    if (!this.done || (seq < 0 || seq >= this.length)) return null
    return this.snapshot.get(seq, opts)
  }

  close () {
    if (!this.snapshot) return Promise.resolve()
    return this.snapshot.close()
  }

  fastCheckout (target) {
    if (!eq(this.head.clock, target)) return false
    this.length = this.snapshot.length
    return true
  }

  slowCheckout (target) {
    return new CheckoutFinder(this)
  }
}

class MultiCheckoutFinder {
  constructor (multiAncestors) {
    this.state = INIT
    this.slowCheckouts = multiAncestors.map(a => a.slowCheckout())
    this.multiAncestors = multiAncestors
    this.bestCheckout = null
  }

  get done () {
    return this.state === VALID || this.state === INVALID
  }

  async update (target) {
    if (this.done) return
    await Promise.all(this.slowCheckouts.map(c => c.update(target)))
    for (const checkout of this.slowCheckouts) {
      if (checkout.state === VALID) {
        this.state = VALID
        this.bestCheckout = checkout
        break
      }
      if (checkout.state === IN_PROGRESS) {
        this.state = IN_PROGRESS
      } else if (checkout.state === INVALID) {
        if (this.state !== IN_PROGRESS) {
          this.state = INVALID
        }
      }
    }
  }

  commit () {
    this.multiAncestors.bestAncestors = this.bestCheckout.ancestors
    this.bestCheckout.commit()
  }
}

class MultiAncestors {
  constructor (outputs) {
    this.outputs = outputs
    this.allAncestors = outputs.map(o => new Ancestors(o))
    this.bestAncestors = null
  }

  get length () {
    return this.bestAncestors ? this.bestAncestors.length : 0
  }

  open () {
    return Promise.all(this.allAncestors.map(a => a.open()))
  }

  close () {
    return Promise.all(this.allAncestors.map(a => a.close()))
  }

  async get (seq, opts = {}) {
    if (!this.bestAncestors || seq < 0 || seq >= this.length) return null
    return this.bestAncestors.get(seq, opts)
  }

  fastCheckout (target) {
    for (const ancestors of this.allAncestors) {
      if (ancestors.fastCheckout(target)) {
        this.bestAncestors = ancestors
        return true
      }
    }
    return false
  }

  slowCheckout () {
    return new MultiCheckoutFinder(this)
  }
}

module.exports = class LinearizedCore extends EventEmitter {
  constructor (autobase, opts = {}) {
    super()
    this[promises] = true
    this.autobase = autobase
    this.view = opts.view ? opts.view(this) : this
    this._rebuildDebounced = debounce(this._rebuild.bind(this))

    this._header = opts.header
    this._view = opts.view
    this._snapshot = opts.snapshot

    this._lastUpdate = {
      ancestors: new MultiAncestors([]),
      clock: new Map()
    }
    this._clock = null
    this._apply = opts.apply || defaultApply

    this._nodes = []
    this._pending = []
    this._applying = null
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
        await this._apply(this.view, batch, clocks, node.change)
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
    if (this._clock && eq(this._clock, latest)) return

    this._clock = latest
    await this._rebuildDebounced()

    return status
  }

  async _rebuildFastForward (update) {
    if (!this._lastUpdate) return
    const checkout = update.ancestors.slowCheckout()
    for await (const node of this.autobase.createCausalStream({ clock: this._lastUpdate.clock })) {
      await checkout.update(node.clock)
      if (checkout.state === VALID) break
      update.removed++
    }
    update.added = update.ancestors.length - checkout.length
    update.nodes = []
  }

  async _rebuildSynchronizeMemory (update) {
    const causal = new CausalThing(this._lastUpdate, update, this._clock) // Causal stream that starts random access
    const memView = new MemoryView(this._lastUpdate.clock) // Causal stream with last entry in memory - pop moves stream forward
    for await (const node of causal) {
      let head = memView.head
      if (head && node.eq(head)) break
      while (head && !node.eq(head) && head.contains(node)) {
        await memView.pop()
        head = memView.head
      }
      if (head && node.eq(head)) break
    }

    const oldLength = length(this._lastUpdate.clock)
    const newLength = length(this._clock)
    const intersectionLength = oldLength - memView.popped

    update.added = newLength - intersectionLength
    update.removed = memView.popped

    update.pending = []
    let delta = 0
    if (update.ancestors.length >= intersectionLength) {
      update.nodes = []
      delta = newLength - update.ancestors.length
    } else {
      update.nodes = this._lastUpdate.nodes.slice(0, intersectionLength)
      delta = newLength - intersectionLength
    }
    for await (const node of this.autobase.createCausalStream({ clock: this._clock })) {
      update.pending.push(node)
      if (update.pending.length >= delta) break
    }
  }

  async _rebuild (status) {
    const ancestors = new MultiAncestors(this._selectOutputs())
    await ancestors.open()

    const update = {
      clock: this._clock,
      ancestors,
      nodes: null,
      pending: null,
      added: 0,
      removed: 0
    }

    if (ancestors.fastCheckout(this._clock)) {
      await this._rebuildFastForward(update)
    } else {
      await this._rebuildSynchronizeMemory(update)
    }

    await this._lastUpdate.ancestors.close()
    this._lastUpdate = update

    // TODO: Run nodes through apply before pushing into nodes
    await this._applyPending(pending)
    this._nodes.push(...this._pending)
    this._pending = []

    await this._flush()

    return status
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
    this._pending.push(...nodes)
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
    if (!this._ancestors) return
    return this._ancestors.close()
  }
}

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}
