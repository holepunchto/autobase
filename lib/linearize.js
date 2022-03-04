const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')
const debounceify = require('debounceify')

const { OutputNode } = require('./nodes')
const { eq } = require('./clock')
const promises = Symbol.for('hypercore.promises')

const MAX_GET_RETRIES = 32

class AppliedBranch {
  constructor (autobase, nodes, oldLength, oldClock) {
    this.autobase = autobase
    this.nodes = nodes
    this.length = oldLength

    this.truncated = 0
    this.head = null
    this.opened = false

    this._ite = this.autobase.createCausalStream({
      clock: oldClock
    })[Symbol.asyncIterator]()
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
    if ((this.truncated + 1) < this.nodes.length) {
      this.head = this.nodes[this.nodes.length - this.truncated]
    } else {
      const next = await this._ite.next()
      this.head = next.value
    }
    if (this.head) {
      this.truncated++
      this.length--
    } else {
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
    if (!this.truncated) return this.nodes
    return this.nodes.slice(0, this.nodes.length - this.truncated)
  }
}

class OutputsBranch {
  constructor (cores) {
    this.cores = cores
    this.heads = new Map()

    this.invalid = false
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

  async _findIntersection (snapshot, clock, operations) {
    let seq = operations - 1
    let node = await snapshot.get(seq)

    while (node) {
      if (node.operations === operations) break
      if (node.operations < operations) {
        seq += (operations - node.operations)
      } else {
        seq -= (node.operations - operations)
      }
      node = await snapshot.get(seq)
    }
    if (!node) return null

    if (node.batch[1] > 0) {
      seq += node.batch[1]
      node = await snapshot.get(seq)
    }

    if (!eq(node.clock, clock)) return null

    return {
      operations: node.operations,
      length: seq + 1
    }
  }

  async _intersectSnapshot (snapshot, node) {
    // TODO: This needs to handle batches + should cache heads etc.
    const head = this.heads.get(snapshot)
    if (!head) return null
    if (head.operations < node.operations) return null

    if (head.operations === node.operations) {
      if (!eq(head.clock, node.clock)) return null
      return { operations: head.operations, length: snapshot.length }
    } else {
      return this._findIntersection(snapshot, node.clock, node.operations)
    }
  }

  async _closeSnapshot (snapshot) {
    const idx = this.snapshots.indexOf(snapshot)
    if (idx === -1) return
    await snapshot.close()
    this.snapshots.splice(idx, 1)
    if (!this.snapshots.length) {
      this.invalid = true
    }
  }

  async update (node) {
    if (!this.opened) await this._open()
    // TODO: This wastes work by throwing away valid intersections -- save them and use them for redundancy
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
    if (!this.snapshot || seq < 0 || seq >= this.length) return null
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
  constructor (autobase, clock, lastOutputs, latestOutputs, applied) {
    this.autobase = autobase
    this.clock = clock
    this.lastOutputs = lastOutputs
    this.latestOutputs = latestOutputs
    this.applied = applied
    this.executed = false

    this._executing = null
    this._applying = null
  }

  async _findRemoteAncestors () {
    // P1: Found intersection between the new branch and the old branch
    // P2: Found intersection between the new branch and the outputs
    const pending = []
    let p1 = null
    let p2 = null

    // TODO: Causal stream should treat this.clock as the "first" node so as to avoid over-reading blocks.
    // (this change would also provide fast-forward implicitly)
    for await (const node of this.autobase.createCausalStream({ clock: this.clock })) {
      let a = null
      let b = null
      if (!p1) a = this.applied.update(node)
      if (!p2) b = this.latestOutputs.update(node)
      const result = await Promise.all([a, b])
      if (a) p1 = result[0]
      if (b) p2 = result[1]
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

  async _findLocalAncestors () {
    const pending = []
    let p2 = null
    for await (const node of this.autobase.createCausalStream({ clock: this.clock })) {
      p2 = await this.latestOutputs.update(node)
      if (p2) break
      pending.push(node)
    }
    if (!p2) p2 = { length: 0, intersection: 0 }
    if (this.lastOutputs) {
      await this.lastOutputs.close()
      this.lastOutputs = null
    }
    return { pending, p2 }
  }

  async _executeRemote () {
    const oldLength = this.applied.length
    const { pending, p1, p2 } = await this._findRemoteAncestors()

    const nodes = (this.applied && (p1.length > p2.length)) ? this.applied.slice() : []
    const length = this.latestOutputs.length + nodes.length
    const appended = length - Math.max(p1.length, p2.length)
    const truncated = oldLength - p1.length

    return { pending, nodes, length, appended, truncated, p1, p2 }
  }

  async _executeLocal () {
    const oldLength = this.autobase.localOutput.length
    const { pending, p2 } = await this._findLocalAncestors()
    return {
      length: p2.length,
      truncated: oldLength - p2.length,
      appended: 0,
      nodes: [],
      pending,
      p1: p2,
      p2
    }
  }

  _execute () {
    return this.applied ? this._executeRemote() : this._executeLocal()
  }

  execute () {
    if (this.executed) return Promise.resolve()
    if (this._executing) return this._executing
    this._executing = this._execute()
    this._executing.catch(safetyCatch)
    return this._executing
  }

  async close () {
    if (this.lastOutputs) await this.lastOutputs.close()
    await this.latestOutputs.close()
  }
}

class BranchSnapshot {
  constructor (autobase, opts = {}) {
    this.autobase = autobase

    this.nodes = opts.nodes || []
    this.header = opts.header
    this.applyFunction = opts.apply || defaultApply
    this.viewFunction = opts.view
    this.view = null
    this.clock = null

    this.lastOutputs = opts.lastOutputs
    this.lastUpdate = opts.lastUpdate
    this.status = opts.status || null
    this.length = opts.length || 0

    this._applying = null
    this._sessions = []

    this.update = debounceify(this._update.bind(this))
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
      const start = this.nodes.length

      try {
        await this.applyFunction(this.view, batch, clocks, node.change)
      } catch (err) {
        while (this.nodes.length > start) {
          this.nodes.pop()
        }
        throw err
      }

      if (this.nodes.length === start) {
        throw new Error('For now, every apply call must append at least one value')
      }

      for (let j = start; j < this.nodes.length; j++) {
        const change = this.nodes[j]
        change.batch[0] = j - start
        change.batch[1] = this.nodes.length - j - 1
      }

      this._applying = null
      batch = []
    }
    if (batch.length) throw new Error('Cannot rebase a partial batch')
  }

  async _rebuild (clock) {
    if (!this.autobase.opened) await this.autobase.ready()
    const lastOutputs = this.lastUpdate ? this.lastUpdate.latestOutputs : null

    let applied = null
    if (!this.autobase.localOutput) {
      if (this.lastUpdate) {
        applied = new AppliedBranch(this.autobase, this.nodes, this.length, this.lastUpdate.clock)
      } else {
        applied = new AppliedBranch(this.autobase, [], 0, new Map()) // If we ever change the clock format, update this
      }
    }

    this.lastOutputs = new OutputsBranch(this.autobase.localOutput ? [this.autobase.localOutput] : this.autobase.outputs)
    this.lastUpdate = new Update(this.autobase, clock, lastOutputs, this.lastOutputs, applied)

    this.status = await this.lastUpdate.execute()
    await this._commitUpdate()

    return this.appended > 0
  }

  async _commitUpdate () {
    this.length = this.status.length
    this.nodes = this.status.nodes

    if (!this.view) {
      const session = this.session({ pin: true })
      this.view = this.viewFunction ? this.viewFunction(session) : session
    }

    try {
      await this._applyPending(this.status.pending)
    } catch (err) {
      safetyCatch(err)
      throw err
    } finally {
      if (this.nodes.length) {
        // If we have nodes, then the last applied clock is the clock (useful in the case of partial applies)
        // If apply was ever called, then this.nodes will be populated
        this.clock = this.nodes[this.nodes.length - 1].clock
      }
    }

    if (this.autobase.localOutput) {
      const localOutput = this.autobase.localOutput
      if (this.status.truncated) {
        await localOutput.truncate(localOutput.length - this.status.truncated)
      }
      if (localOutput.length === 0 && this.nodes.length) {
        this.nodes[0].header = this.header
      }
      await localOutput.append(this.nodes)
    }

    for (const session of this._sessions) {
      if (this.status.truncated) session.emit('truncate', this.lastUpdate.length - this.status.truncated)
      if (this.status.appended) session.emit('append')
    }
  }

  async _update () {
    return this._rebuild(await this.autobase.latest())
  }

  _get (seq, opts) {
    if (seq < 0 || seq > this.length - 1) return null
    if (seq >= this.lastOutputs.length) return this.nodes[seq - this.lastOutputs.length]
    return this.lastOutputs.get(seq, opts)
  }

  async get (seq, opts = {}) {
    if (!this.lastUpdate) await this.ready()
    let retries = MAX_GET_RETRIES
    let block = null
    while (retries-- > 0) {
      block = await this._get(seq, opts)
      if (block) return block
      await this._rebuild(this.lastUpdate.clock)
    }
    throw new Error(`Linearization could not be rebuilt after ${MAX_GET_RETRIES} attempts`)
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
    this.length += nodes.length
    this.status.appended += nodes.length
    this.nodes.push(...nodes)
  }

  _close () {
    if (!this.lastUpdate) return
    return this.lastUpdate.close()
  }

  closeSession (session) {
    this._sessions.splice(session._sessionIdx, 1)
    if (this._sessions.length) return
    return this._close()
  }

  session (opts = {}) {
    const session = new LinearizedCore(this, opts)
    session._sessionIdx = this._sessions.push(session) - 1
    return session
  }

  copy () {
    return new BranchSnapshot(this.autobase, {
      nodes: [...this.nodes], // Must make a copy of the nodes here as they are mutated in-place during updates
      header: this.header,
      apply: this.applyFunction,
      view: this.viewFunction,
      status: this.status,
      length: this.length,
      lastUpdate: this.lastUpdate,
      lastOutputs: this.lastOutputs
    })
  }
}

class LinearizedCore extends EventEmitter {
  constructor (snapshot, opts = {}) {
    super()
    this[promises] = true
    this.byteLength = 0
    this.writable = true
    this.opts = opts
    this.opened = false
    this.closed = false

    this._opening = null
    this._closing = null

    this._activeRequests = []
    this._checkout = opts.checkout || null
    this._clock = opts.clock || null
    this._unwrap = opts.unwrap === true
    this._pinned = opts.pin === true
    this._snapshot = snapshot
    this._sessionIdx = null // Set in BranchSnapshot.session

    this.ready = () => this.open()
  }

  async _open () {
    // TODO: The snapshot should do a local-only update here
    this.opened = true
  }

  async _close () {
    if (!this.opened) await this.ready()
    for (const req of this._activeRequests) {
      req.cancel()
    }
    this._activeRequests = []
    return this._snapshot.closeSession(this)
  }

  open () {
    if (this.opened) return Promise.resolve()
    this._opening = this._open()
    this._opening.catch(noop)
    return this._opening
  }

  close () {
    if (this.closed) return Promise.resolve()
    this._closing = this._close()
    this._closing.catch(noop)
    return this._closing
  }

  // LinearizedCore API

  get view () {
    return this._snapshot.view
  }

  get status () {
    return this._snapshot.status
  }

  unwrap () {
    return this.session({ ...this.opts, unwrap: true })
  }

  wrap () {
    return this.session({ ...this.opts, unwrap: false })
  }

  // Hypercore API

  get length () {
    return this._snapshot.length
  }

  session (opts = {}) {
    if (this._pinned || (!opts.snapshot && !opts.checkout)) return this._snapshot.session({ ...this.opts, ...opts })
    if (opts.snapshot) {
      // If we're creating a new snapshot, then it must be given a new underlying BranchSnapshot
      const branch = this._snapshot.copy()
      return branch.session({ ...this.opts, ...opts })
    }
    if (opts.checkout) {
      throw new Error('Checkout not yet implemented')
    }
  }

  snapshot () {
    return this.session({ snapshot: true })
  }

  checkout (clock) {
    return this.session({ checkout: clock })
  }

  update () {
    if (this._checkout) return
    return this._snapshot.update()
  }

  async append (blocks) {
    // Must only be called from within an apply function
    return this._snapshot.append(blocks)
  }

  async get (seq, opts = {}) {
    let block = await this._snapshot.get(seq, { ...opts, active: this._activeRequests })
    if (!block) return null
    if (this._unwrap) block = block.value
    return opts.valueEncoding ? opts.valueEncoding.decode(block) : block
  }
}

module.exports = BranchSnapshot

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}

function noop () {}
