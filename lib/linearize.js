const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')

const { OutputNode } = require('./nodes')
const { eq, lte, greatestCommonAncestor } = require('./clock')
const promises = Symbol.for('hypercore.promises')

class AppliedBranch {
  constructor (autobase, baseLength, nodes) {
    this.autobase = autobase
    this.nodes = nodes
    this.baseLength = baseLength
    this.truncated = 0
    this.head = null
    this.opened = false

    const clock = this.nodes.length ? this.nodes[0].clock : null
    this._ite = this.autobase.createCausalStream({ clock })[Symbol.asyncIterator]()
  }

  get length () {
    return this.baseLength + this.nodes.length - this.truncated
  }

  get operations () {
    if (!this.head) return 0
    return this.head.operations
  }

  async _open () {
    if (this.nodes.length) {
      this.head = this.nodes[this.nodes.length - 1]
    } else {
      const next = await this._ite.next()
      this.head = next.value
    }
    this.opened = true
  }

  async pop () {
    this.truncated++
    if (this.truncated < this.nodes.length) {
      this.head = this.nodes[this.nodes.length - this.truncated - 1]
    } else {
      const next = await this._ite.next()
      this.head = next.value
    }
    if (this.head && this.head.batch[0] === 0) return null
    return this.pop()
  }

  async update (node) {
    if (!this.opened) await this._open()
    while (this.head && !this.head.contains(node)) {
      if (eq(this.head, node)) return { length: this.length, operations: this.head.operations }
      await this.pop()
    }
    return false
  }

  slice () {
    return this.nodes.slice(0, this.nodes.length - this.truncated)
  }
}

class OutputsBranch {
  constructor (cores) {
    this.cores = cores
    this.opened = false
    this.snapshots = null
    this.intersections = new Map()
    this.heads = new Map()
    this.length = 0
    this.operations = 0
  }

  async _open () {
    await Promise.all(this.cores.map(c => c.update()))
    this.snapshots = this.cores.map(c => c.snapshot())

    const badSnapshots = []
    const heads = await Promise.allSettled(this.snapshots.map(s => s.get(s.length - 1)))
    for (let i = 0; i < heads.length; i++) {
      const { status, value } = heads[i]
      if (status !== 'rejected') {
        this.heads.set(this.snapshots[i], value)
      } else {
        badSnapshots.push(snapshots[i])
      }
    }
    for (const snapshot of badSnapshots) {
      await this._closeSnapshot(snapshot)
    }

    this.opened = true
  }

  async _intersectSnapshot (snapshot, node) {
    // TODO: This needs to handle batches + should cache heads etc.
    const head = this.heads.get(snapshot)
    if (!head) return null
    if (head.operations < node.operations) return null

    const maybeIntersection = await snapshot.get(node.operations)
    if (!eq(maybeIntersection.clock, node.clock)) return null

    return {
      operations: node.operations,
      length: node.operations + 1 // TODO: Account for operations !== length (batches)
    }
  }

  async _closeSnapshot (snapshot) {
    await snapshot.close()
    this.snapshots.splice(this.snapshots.indexOf(snapshot, 1))
    this.intersections.delete(snapshot)
  }

  async fastForward (clock) {
    if (!this.opened) await this._open()
    for (const [snapshot, head] of this.heads) {
      if (lte(clock, head.clock)) {
        const intersection = {
          length: snapshot.length,
          operations: head.operations
        }
        intersections.set(snapshot, intersection)
        if (intersections.size === this.heads.length) {
          return intersection
        }
      }
    }
    return null
  }

  async update (node) {
    if (!this.opened) await this._open()
    const remaining = []
    for (const snapshot of this.snapshots) {
      if (this.intersections.has(snapshot)) continue
      remaining.push(snapshot)
    }
    const intersections = await Promise.allSettled(remaining.map(s => this._intersectSnapshot(s, node)))
    for (let i = 0; i < intersections.length; i++) {
      const { status, value: intersection } = intersections[i]
      const snapshot = this.snapshots[i]
      if (status === 'rejected') {
        await this._closeSnapshot(snapshot)
        continue
      }
      if (intersection) {
        this.intersections.set(snapshot, intersection)
        this.length = intersection.length
        this.operations = intersection.operations

        if (this.intersections.size === this.snapshots.length) {
          return intersection
        }
      }
    }
    return null
  }

  async intersect (node) {
    if (!this.opened) await this._open()
    const results = await Promise.allSettled(this.snapshots.map(s => this._intersectSnapshot(s, node)))
    for (let i = 0; i < results.length; i++) {
      const { status, value: intersection } = results[i]
      const snapshot = this.snapshots[i]
      if (status === 'rejected') continue
      if (intersection) return intersection
    }
    return null
  }

  async get (seq, opts) {
    for (const [snapshot, intersection] of this.intersections) {
      const { length, operations } = intersection
      if (length <= seq) continue
      try {
        const block = await snapshot.get(seq, opts)
      } catch (err) {
        // If the fork is no longer available, the snapshot is invalid
        if (err.code === 'FORK_NOT_AVAILABLE') {
          await this._closeSnapshot(snapshot)
          return this.get(seq, opts)
        }
        // TODO: If the block is not available, we should try other snapshots too
        throw err
      }
    }
    return null
  }

  close () {
    if (!this.snapshots) return
    return Promise.all(this.snapshots.map(s => s.close()))
  }
}

class Update {
  constructor (autobase, clock, applied, lastOutputs, latestOutputs) {
    this.autobase = autobase
    this.clock = clock
    this.applied = applied
    this.lastOutputs = lastOutputs
    this.latestOutputs = latestOutputs

    this.appended = 0
    this.truncated = 0
    this.nodes = null
    this.executed = false

    this._applying = null
  }

  get length () {
    return this.latestOutputs.length + this.nodes.length
  }

  async _applyPending (pending, view, apply) {
    let batch = []
    while (pending.length) {
      const node = pending.pop()
      batch.push(node)
      if (node.batch[1] > 0) continue
      this._applying = node

      const inputNode = await this.autobase._getInputNode(node.change, node.seq)
      const clocks = {
        local: inputNode.clock,
        global: this._applying.clock
      }
      const start = pending.length

      try {
        await apply(view, batch, clocks, node.change)
      } catch (err) {
        safetyCatch(err)
      }

      for (let j = start; j < pending.length; j++) {
        const change = pending[j]
        change.batch[0] = j - start
        change.batch[1] = pending.length - j - 1
      }

      this._applying = null
      batch = []
    }
    if (batch.length) throw new Error('Cannot rebase a partial batch')
  }

  async execute ({ view, apply }) {
    // P1: Found intersection between the new branch and the old branch
    // P2: Found intersection between the new branch and the outputs
    if (this.executed) return
    const pending = []
    let p1 = null
    let p2 = null

    // First attempt to fast-forward to the outputs if possible
    p2 = await this.latestOutputs.fastForward(this.clock)

    // If a fast-forward was possible, find the closest intersection (either P1 or P2)
    // Any nodes we need to use in branch updates will subsequently be applied, so they're buffered.
    if (!p2) {
      for await (const node of this.autobase.createCausalStream({ clock: this.clock })) {
        if (!p1) p1 = await this.applied.update(node)
        if (!p2) p2 = await this.latestOutputs.update(node)
        if (p2 || p1) break
        pending.push(node)
      }
    }

    // P2 > P1 -> random-access back to fork point using outputs
    // just a faster way to find P1 vs continuing to unroll the causal stream
    if (p2 && !p1) {
      while (this.applied.head) {
        p1 = await this.latestOutputs.intersect(this.applied.head)
        if (p1) break
        await this.applied.pop()
      }
    }

    if (p1 && !p2) {
      // We found an intersection with the old branch before we found one with the latest outputs -- use the old outputs
      // TODO: Potentially roll the stream ~5 back because the output is probably there
      this.latestOutputs = this.lastOutputs
      p2 = {
        length: this.latestOutput.length,
        operations: this.latestOutput.operations
      }
    }

    this.nodes = this.applied.slice()
    await this._applyPending(pending, view, apply) // This will push directly into this.nodes

    this.appended = this.length - p1.length
    this.truncated = this.applied.truncated

    this.executed = true
  }

  get (seq, opts) {
    if (seq < 0 || seq > this.length - 1) return null
    if (seq < this.latestOutputs.length - 1) return this.latestOutputs.get(seq)
    return this.nodes[seq - this.latestOutputs.length]
  }

  append (values) {
    if (!this._applying) throw new Error('Can only append to a LinearizedCore inside of an apply function')
    if (!Array.isArray(block)) block = [block]
    const nodes = []
    for (const val of block) {
      const node = new OutputNode({
        value: val,
        batch: [0, 0],
        clock: this._applying.clock,
        change: this._applying.change,
        operations: this._applying.operations
      })
      nodes.push(node)
    }
    this.nodes.push(...nodes)
  }
}


module.exports = class LinearizedCore extends EventEmitter {
  constructor (autobase, opts = {}) {
    super()
    this[promises] = true
    this.autobase = autobase
    this.view = opts.view ? opts.view(this) : this
    this.byteLength = 0

    this._header = opts.header
    this._view = opts.view
    this._snapshot = opts.snapshot
    this._apply = opts.apply || defaultApply

    this._lastUpdate = null
    this._pendingUpdate =
  }

  get length () {
    return this._lastUpdate ? this._lastUpdate.length : 0
  }

  async update () {
    const outputsBranch = new OutputsBranch(this.autobase.outputs)
    const appliedBranch = new AppliedBranch(this.autobase, this._lastUpdate ? this._lastUpdate.nodes : [])
    const clock = this._snapshot || await this.autobase.latest()
    const update = new Update(this.autobase, clock, outputsBranch, appliedBranch)
    await update.execute() // TODO: This needs a view with an updating length
    this._lastUpdate = update // Atomically set the new update
  }

  get (seq, opts) {

  }

  append (blocks) {

  }

  unwrap () {

  }

  wrap () {

  }

  snapshot () {

  }

  close () {

  }
}

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}
