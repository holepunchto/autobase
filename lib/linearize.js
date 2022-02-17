const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')

const { OutputNode } = require('./nodes')
const { eq, lte } = require('./clock')
const promises = Symbol.for('hypercore.promises')

class AppliedBranch {
  constructor (autobase, baseLength, nodes) {
    this.autobase = autobase
    this.nodes = nodes
    this.baseLength = baseLength
    this.truncated = 0
    this.head = null
    this.opened = false

    this._ite = this.autobase.createCausalStream({
      clock: this.nodes.length ? this.nodes[0].clock : new Map()
    })[Symbol.asyncIterator]()
  }

  get length () {
    return this.baseLength + this.nodes.length - this.truncated
  }

  get operations () {
    if (!this.head) return 0
    return this.head.operations
  }

  async open () {
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
    if (!this.head) {
      this.truncated--
      return null
    }
    if (this.head && this.head.batch[0] === 0) return null
    return this.pop()
  }

  async update (node) {
    if (!this.opened) await this.open()
    if (this.head && eq(this.head.clock, node.clock)) return { length: this.length, operations: this.head.operations }
    while (this.head && !eq(this.head.clock, node.clock) && this.head.contains(node)) {
      await this.pop()
    }
    if (this.head && eq(this.head.clock, node.clock)) return { length: this.length, operations: this.head.operations }
    return null
  }

  slice () {
    return this.nodes.slice(0, this.nodes.length - this.truncated)
  }
}

class OutputsBranch {
  constructor (cores) {
    this.cores = cores
    this.heads = new Map()

    this.snapshots = null
    this.snapshot = null
    this.length = 0
    this.operations = 0
    this.opened = false
  }

  async _open () {
    await Promise.all(this.cores.map(c => c.update()))
    this.snapshots = this.cores.map(c => c.snapshot())

    const badSnapshots = []
    const heads = await Promise.allSettled(this.snapshots.map(s => s.length > 0 ? s.get(s.length - 1) : Promise.resolve(null)))
    for (let i = 0; i < heads.length; i++) {
      const { status, value } = heads[i]
      if (status !== 'rejected' && value) {
        this.heads.set(this.snapshots[i], value)
      } else {
        badSnapshots.push(this.snapshots[i])
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

    const maybeIntersection = head.operations === node.operations ? head : await snapshot.get(node.operations - 1)
    if (!eq(maybeIntersection.clock, node.clock)) return null

    return {
      operations: node.operations,
      length: node.operations // TODO: Account for operations !== length (batches)
    }
  }

  async _closeSnapshot (snapshot) {
    await snapshot.close()
    this.snapshots.splice(this.snapshots.indexOf(snapshot, 1))
  }

  async update (node) {
    if (!this.opened) await this._open()
    const intersections = await Promise.allSettled(this.snapshots.map(s => this._intersectSnapshot(s, node)))
    for (let i = 0; i < intersections.length; i++) {
      const { status, value: intersection } = intersections[i]
      const snapshot = this.snapshots[i]
      if (status === 'rejected') {
        await this._closeSnapshot(snapshot)
        continue
      }
      if (!intersection) continue
      this.length = intersection.length
      this.operations = intersection.operations
      this.snapshot = snapshot
      return intersection
    }
    return null
  }

  async intersect (node) {
    if (!this.opened) await this._open()
    const results = await Promise.allSettled(this.snapshots.map(s => this._intersectSnapshot(s, node)))
    for (let i = 0; i < results.length; i++) {
      const { status, value: intersection } = results[i]
      if (status === 'rejected') continue
      if (intersection) return intersection
    }
    return null
  }

  async get (seq, opts) {
    if (seq < 0 || seq >= this.length) return null
    try {
      return await this.snapshot.get(seq, opts)
    } catch (err) {
      // If the fork is no longer available, the snapshot is invalid
      if (err.code === 'FORK_NOT_AVAILABLE') {
        await this._closeSnapshot(this.snapshot)
        return this.get(seq, opts)
      }
      // TODO: If the block is not available, we should try other snapshots too
      throw err
    }
  }

  close () {
    if (!this.snapshots) return
    return Promise.all(this.snapshots.map(s => s.close()))
  }
}

class Update {
  constructor (autobase, view, applyFunction, clock, applied, lastOutputs, latestOutputs) {
    this.autobase = autobase
    this.clock = clock
    this.view = view
    this.applyFunction = applyFunction
    this.applied = applied
    this.lastOutputs = lastOutputs
    this.latestOutputs = latestOutputs

    this.p1 = null
    this.p2 = null
    this.appended = 0
    this.truncated = 0
    this.nodes = null
    this.executed = false

    this._executing = null
    this._applying = null
  }

  get length () {
    return this.latestOutputs.length + this.nodes.length
  }

  async _applyPending (pending) {
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
        await this.applyFunction(this.view, batch, clocks, node.change)
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

  async _findAncestors () {
    // P1: Found intersection between the new branch and the old branch
    // P2: Found intersection between the new branch and the outputs
    const pending = []
    let p1 = null
    let p2 = null

    for await (const node of this.autobase.createCausalStream({ clock: this.clock })) {
      if (!p1) p1 = await this.applied.update(node)
      if (!p2) p2 = await this.latestOutputs.update(node)
      if (p2 || p1) break
      pending.push(node)
    }

    // P2 > P1 -> random-access back to fork point using outputs
    // just a faster way to find P1 vs continuing to unroll the causal stream
    if (p2 && !p1) {
      if (!this.applied.opened) await this.applied.open()
      while (this.applied.head) {
        p1 = await this.latestOutputs.intersect(this.applied.head)
        if (p1) break
        await this.applied.pop()
      }
    }

    if (p1 && !p2) {
      // We found an intersection with the old branch before we found one with the latest outputs -- use the old outputs
      // TODO: Potentially roll the stream ~5 back because the output is probably there
      await this.latestOutputs.close()
      this.latestOutputs = this.lastOutputs
      p2 = {
        length: this.latestOutputs.length,
        operations: this.latestOutputs.operations
      }
    } else {
      if (this.lastOutputs) {
        await this.lastOutputs.close()
        this.lastOutputs = null
      }
    }

    if (!p1) p1 = { length: 0, intersection: 0 }
    if (!p2) p2 = { length: 0, intersection: 0 }

    return { pending, p1, p2 }
  }

  async _execute () {
    const { pending, p1, p2 } = await this._findAncestors()

    this.p1 = p1
    this.p2 = p2
    this.nodes = (p1.length > p2.length) ? this.applied.slice() : []

    await this._applyPending(pending) // This will push directly into this.nodes

    if (this.autobase.localOutput) {
      // If we're using a local output, then the old branch will always track the outputs, but P1 will never be located (the applied branch is always empty).
      this.appended = this.nodes.length
      this.truncated = p2 ? Math.max(this.autobase.localOutput.length - p2.length, 0) : this.autobase.localOutput.length
    } else {
      this.appended = this.length - Math.max(p1.length, p2.length)
      this.truncated = this.applied.truncated
    }

    this.executed = true
  }

  execute () {
    if (this.executed) return Promise.resolve()
    if (this._executing) return this._executing
    this._executing = this._execute()
    this._executing.catch(safetyCatch)
    return this._executing
  }

  get (seq, opts) {
    if (seq < 0 || seq > this.length - 1) return null
    if (seq < this.latestOutputs.length) return this.latestOutputs.get(seq)
    return this.nodes[seq - this.latestOutputs.length]
  }

  append (block) {
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

  async close () {
    if (this.lastOutputs) await this.lastOutputs.close()
    await this.latestOutputs.close()
  }
}

class Snapshot extends EventEmitter {
  constructor (autobase, key, clock, opts) {
    super()
    this.autobase = autobase
    this.key = key
    this.clock = clock
    this.references = 0

    this.nodes = []
    this.view = null // set in setView
    this.header = opts.header
    this.applyFunction = opts.apply || defaultApply

    this.lastUpdate = null
  }

  setView (view) {
    this.view = view
  }

  increment () {
    this.references++
  }

  async decrement () {
    if (--this.references > 0 || !this.lastUpdate) return Promise.resolve(false)
    await this.lastUpdate.close()
    return true
  }

  get length () {
    return this.lastUpdate ? this.lastUpdate.length : 0
  }

  get status () {
    if (!this.lastUpdate) return null
    return {
      appended: this.lastUpdate.appended,
      truncated: this.lastUpdate.truncated,
      p1: this.lastUpdate.p1,
      p2: this.lastUpdate.p2,
      nodes: this.lastUpdate.nodes
    }
  }

  async update () {
    if (!this.autobase.opened) await this.autobase.ready()
    const latestOutputs = new OutputsBranch(this.autobase.localOutput ? [this.autobase.localOutput] : this.autobase.outputs)
    const lastOutputs = this.lastUpdate ? this.lastUpdate.latestOutputs : null
    const clock = this.clock || await this.autobase.latest()

    const nodes = !this.autobase.localOutput ? this.nodes : []
    const baseLength = lastOutputs ? lastOutputs.length : 0
    const applied = new AppliedBranch(this.autobase, baseLength, nodes)

    this.lastUpdate = new Update(this.autobase, this.view, this.applyFunction, clock, applied, lastOutputs, latestOutputs)
    await this.lastUpdate.execute()
    this.nodes = this.lastUpdate.nodes

    const appended = this.lastUpdate.appended
    const truncated = this.lastUpdate.truncated

    if (this.autobase.localOutput) {
      const localOutput = this.autobase.localOutput
      const lastUpdate = this.lastUpdate
      if (localOutput.length === 0 && lastUpdate.nodes.length) {
        lastUpdate.nodes[0].header = this.header
      }
      if (truncated) {
        await localOutput.truncate(localOutput.length - truncated)
      }
      await localOutput.append(lastUpdate.nodes)
    }

    if (truncated) this.emit('truncate', this.lastUpdate.length - truncated)
    if (appended) this.emit('append', appended)

    return appended > 0
  }

  get (seq, opts = {}) {
    if (!this.lastUpdate) return null
    return this.lastUpdate.get(seq, opts)
  }

  append (blocks) {
    if (!this.lastUpdate) throw new Error('Can only append to a LinearizedCore inside of an apply function')
    return this.lastUpdate.append(blocks)
  }
}

module.exports = class LinearizedCore extends EventEmitter {
  constructor (autobase, opts = {}) {
    super()
    this[promises] = true
    this.autobase = autobase
    this.byteLength = 0
    this.writable = true
    this.opts = opts

    this._unwrap = opts.unwrap === true

    this._snapshotClock = opts.snapshot
    this._snapshots = opts._snapshots || new Map()

    const snapshotKey = clockKey(this._snapshotClock)
    this._snapshot = this._snapshots.get(snapshotKey)
    if (!this._snapshot) {
      this._snapshot = new Snapshot(this.autobase, snapshotKey, this._snapshotClock, opts)
      this._snapshots.set(snapshotKey, this._snapshot)
      this._snapshot.setView(opts.view ? opts.view(this) : this)
    }
    this._snapshot.increment()

    this._onappend = length => this.emit('append', length)
    this._ontruncate = length => this.emit('truncate', length)
    this._snapshot.on('append', this._onappend)
    this._snapshot.on('truncate', this._ontruncate)

    this.ready = () => this.autobase.ready()
  }

  _copy (opts) {
    return new LinearizedCore(this.autobase, {
      ...opts,
      ...this.opts,
      _snapshots: this._snapshots
    })
  }

  get length () {
    return this._snapshot.length
  }

  get view () {
    return this._snapshot.view
  }

  get status () {
    return this._snapshot.status
  }

  update () {
    return this._snapshot.update()
  }

  async get (seq, opts = {}) {
    let block = await this._snapshot.get(seq, opts)
    if (!block) return null
    if (this._unwrap) block = block.value
    return opts.valueEncoding ? opts.valueEncoding.decode(block) : block
  }

  append (blocks) {
    return this._snapshot.append(blocks)
  }

  snapshot () {
    if (!this._snapshot.lastUpdate) throw new Error('Can only snapshot after an update')
    return this._copy({ snapshot: this._snapshot.lastUpdate.clock })
  }

  unwrap () {
    return this._copy({ unwrap: true })
  }

  wrap () {
    return this._copy({ unwrap: false })
  }

  async close () {
    this._snapshot.removeListener('append', this._onappend)
    this._snapshot.removeListener('truncate', this._ontruncate)
    const shouldClose = await this._snapshot.decrement()
    if (!shouldClose) return
    this._snapshots.delete(this._snapshot.key)
  }
}

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}

function clockKey (clock) {
  if (!clock) return null
  const keys = [...clock.keys()]
  keys.sort()
  return keys.map(k => k + ':' + clock.get(k)).join(',')
}
