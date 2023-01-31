const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')
const debounceify = require('debounceify')
const b = require('b4a')

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
  constructor (cores, opts = {}) {
    this.cores = cores
    this.heads = opts.heads || new Map()

    this.invalid = false
    this.snapshots = null
    this.snapshot = null
    this.length = 0
    this.operations = 0

    this._parent = opts.parent
    this._opened = false
    this._closed = false
    this._opening = this._open().catch(safetyCatch)
  }

  async _open () {
    if (this._parent && !this._opened) {
      if (!this._parent._opened) await this._parent._opening
      this._cloneFrom(this._parent)
      this._opened = true
      return
    }

    await Promise.all(this.cores.map(c => c.update()))
    this.snapshots = this.cores.map(c => c.snapshot())

    const badSnapshots = []
    const heads = await Promise.allSettled(this.snapshots.map(s => s.length > 0 ? s.get(s.length - 1) : Promise.resolve(null)))
    for (let i = 0; i < heads.length; i++) {
      const { status, value } = heads[i]
      if (status !== 'rejected' && value) {
        this.heads.set(b.toString(this.snapshots[i].key, 'hex'), value)
      } else {
        badSnapshots.push(this.snapshots[i])
      }
    }
    for (const snapshot of badSnapshots) {
      await this._closeSnapshot(snapshot)
    }

    this._opened = true
  }

  async _findIntersection (snapshot, clock, operations) {
    let seq = operations - 1
    let node = await snapshot.get(seq)

    // TODO: A galloping search would be optimal here, but this is good enough for the common cases.
    const cmp = operations - node.operations
    if (cmp > 0) {
      while (seq < snapshot.length && operations > node.operations) {
        seq += node.batch[1] + 1
        if (seq >= snapshot.length) break
        node = await snapshot.get(seq)
      }
    } else if (cmp < 0) {
      while (seq >= 0 && operations < node.operations) {
        seq -= node.batch[0] + 1
        if (seq < 0) break
        node = await snapshot.get(seq)
      }
    }
    if (!node || node.operations !== operations || !eq(node.clock, clock)) return null
    if (node.batch[1] > 0 && (seq + node.batch[1] >= snapshot.length)) return null

    if (node.batch[1] > 0) {
      seq += node.batch[1]
      node = await snapshot.get(seq)
    }

    return {
      operations: node.operations,
      length: seq + 1
    }
  }

  async _intersectSnapshot (snapshot, node) {
    // TODO: This needs to handle batches + should cache heads etc.
    const head = this.heads.get(b.toString(snapshot.key, 'hex'))
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
    this.snapshots.splice(idx, 1)
    await snapshot.close()
    if (!this.snapshots.length) {
      this.invalid = true
    }
  }

  async update (node) {
    if (!this._opened) await this._opening
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
    if (!this._opened) await this._open()
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

  _cloneFrom (other) {
    this.length = other.length
    this.operations = other.operations
    this.invalid = other.invalid
    this.snapshot = other.snapshot && other.snapshot.snapshot()
    this.snapshots = other.snapshots && other.snapshots.map(s => s.snapshot())
    this.heads = new Map([...other.heads])
  }

  clone () {
    return new OutputsBranch(this.cores, {
      parent: this
    })
  }

  async close () {
    await this._opening
    if (this.snapshots) await Promise.all(this.snapshots.map(s => s.close()))
  }
}

class Update {
  constructor (autobase, clock, lastOutputs, latestOutputs, applied, localOutput, opts = {}) {
    this.autobase = autobase
    this.clock = clock
    this.lastOutputs = lastOutputs
    this.latestOutputs = latestOutputs
    this.localOutput = localOutput
    this.applied = applied
    this.executed = !!opts.executed

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
      const latestOutputs = this.latestOutputs
      this.latestOutputs = this.lastOutputs
      p2 = {
        length: this.latestOutputs.length,
        operations: this.latestOutputs.operations
      }
      await latestOutputs.close()
    } else {
      if (this.lastOutputs) {
        const lastOutputs = this.lastOutputs
        this.lastOutputs = null
        await lastOutputs.close()
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
      const lastOutputs = this.lastOutputs
      this.lastOutputs = null
      await lastOutputs.close()
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

    return {
      oldLength,
      length,
      truncated,
      appended,
      nodes,
      pending,
      p1,
      p2
    }
  }

  async _executeLocal () {
    const oldLength = this.localOutput.length
    const { pending, p2 } = await this._findLocalAncestors()
    return {
      oldLength,
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
    if (this._executing) return this._executing
    this._executing = this._execute()
    this._executing.catch(safetyCatch)
    return this._executing
  }

  clone () {
    const latestOutputs = this.latestOutputs && this.latestOutputs.clone()
    const lastOutputs = this.lastOutputs ? this.lastOutputs.clone() : latestOutputs
    return new Update(this.autobase, this.clock, lastOutputs, latestOutputs, this.applied, this.localOutput, {
      executed: this.executed
    })
  }

  async close () {
    if (this.lastOutputs) await this.lastOutputs.close()
    await this.latestOutputs.close()
  }
}

class LinearizedCore {
  constructor (autobase, opts = {}) {
    this.autobase = autobase

    this.root = opts.root || this
    this.nodes = opts.nodes || []
    this.header = opts.header
    this.applyFunction = opts.apply || defaultApply
    this.viewFunction = opts.view
    this.view = null
    this.clock = null

    this.lastUpdate = opts.lastUpdate

    this.status = opts.status || { appended: 0, truncated: 0 }
    this.length = opts.length || 0

    this._writable = opts.writable !== false
    this._applying = null
    this._viewSession = null
    this._sessions = []
    this._closed = false

    this.update = debounceify(this._update.bind(this))
  }

  get isRoot () {
    return this.root === this
  }

  async _applyPending (pending) {
    let batch = []
    while (pending.length) {
      const node = pending.pop()
      batch.push(node)
      if (node.batch[1] > 0) continue
      this._applying = node

      if (!this.view) {
        const session = this.session({ pin: true })
        this._viewSession = session
        this.view = this.viewFunction ? this.viewFunction(session) : session
      }

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
    if (this._closed) return false

    const lastOutputs = this.lastUpdate ? this.lastUpdate.latestOutputs : null
    const localOutput = this.autobase.localOutput

    let applied = null
    if (!localOutput || !this._writable) {
      if (this.lastUpdate) {
        applied = new AppliedBranch(this.autobase, this.nodes, this.length, this.lastUpdate.clock)
      } else {
        applied = new AppliedBranch(this.autobase, [], 0, new Map()) // If we ever change the clock format, update this
      }
    }

    const outputs = new OutputsBranch(localOutput ? [localOutput] : this.autobase.outputs)
    this.lastUpdate = new Update(this.autobase, clock, lastOutputs, outputs, applied, localOutput)

    try {
      this.status = await this.lastUpdate.execute()
      await this._commitUpdate(localOutput)
      return this.appended > 0
    } catch (err) {
      safetyCatch(err)
      return false
    }
  }

  async _commitUpdate (localOutput) {
    this.length = this.status.length
    this.nodes = this.status.nodes

    const truncationLength = this.status.oldLength - this.status.truncated

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

    if (this._writable && localOutput) {
      if (this.status.truncated) {
        await localOutput.truncate(truncationLength)
      }
      if (localOutput.length === 0 && this.nodes.length) {
        this.nodes[0].header = this.header
      }
      await localOutput.append(this.nodes)
    }

    for (const session of this._sessions) {
      if (this.status.truncated) session.emit('truncate', truncationLength)
      if (this.status.appended) session.emit('append')
    }
  }

  async _update () {
    // First check if any work needs to be done
    // If we're building a local index, and the clock is the same, no work is needed.
    // (If we're not building a local index, the state of the remote outputs might have changed, so must update)
    const clock = await this.autobase.latest()

    if (this.autobase.localOutput && this.lastUpdate && eq(clock, this.lastUpdate.clock)) {
      this.status = { appended: 0, truncated: 0 }
      return
    }

    // Next check if any snapshot sessions need to be migrated to a root clone before the update
    if (this.isRoot) {
      const snapshots = this._sessions.filter(s => s._snapshotted)
      for (const snapshot of snapshots) {
        const clone = this.clone()
        migrateSession(this, clone, snapshot)
        // Unsure about this one, but basically, no one has ownership of the clone,
        // so we wanna tie the ownership to the snapshot.
        snapshot.on('close', () => clone._close())
      }
    }

    // Next perform the update
    await this._rebuild(clock)
  }

  async updateSession (session) {
    if (this.autobase._loadingInputsCount > 0) {
      await this.autobase._waitForInputs()
    }
    if (!session._snapshotted) return this.update()
    // If the session is a snapshot and this LinearizedCore is not the root, migrate the session to the root and update the root.
    await this.root.update()

    if (session._core === this.root) return

    migrateSession(session._core, this.root, session)

    if (this.root.status.truncated) session.emit('truncate', this.root.lastUpdate.length - this.root.status.truncated)
    if (this.root.status.appended) session.emit('append')

    if (!this._sessions.length) await this._close()
  }

  _get (seq, opts) {
    if (seq < 0 || seq > this.length - 1) return null

    const outputs = this.lastUpdate && this.lastUpdate.latestOutputs
    if (!outputs) return null

    if (seq >= outputs.length) return this.nodes[seq - outputs.length]
    return outputs.get(seq, opts)
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

  async _close () {
    this._closed = true

    if (this._viewSession) await this._viewSession.close()
    if (this.lastUpdate) await this.lastUpdate.close()
  }

  closeSession (session) {
    const head = this._sessions.pop()
    if (head !== session) {
      this._sessions[session._sessionIdx] = head
      head._sessionIdx = session._sessionIdx
    }
    if (this._sessions.length) return
    return this._close()
  }

  session (opts = {}) {
    const session = new LinearizedCoreSession(this, opts)
    session._sessionIdx = this._sessions.push(session) - 1
    return session
  }

  clone (opts) {
    return new LinearizedCore(this.autobase, {
      ...opts,
      root: this.root,
      nodes: [...this.nodes], // Must make a copy of the nodes here as they are mutated in-place during updates
      lastUpdate: this.lastUpdate && this.lastUpdate.clone(),
      header: this.header,
      apply: this.applyFunction,
      view: this.viewFunction,
      status: this.status,
      length: this.length
    })
  }
}

class LinearizedCoreSession extends EventEmitter {
  constructor (core, opts = {}) {
    super()
    this[promises] = true
    this.byteLength = 0
    this.writable = true
    this.opts = opts
    this.opened = false
    this.closed = false

    this._opening = null
    this._closing = null

    this._core = core
    this._activeRequests = []
    this._checkout = opts.checkout || null
    this._clock = opts.clock || null
    this._unwrap = opts.unwrap === true
    this._pinned = opts.pin === true
    this._snapshotted = opts.snapshot === true
    this._sessionIdx = -1 // Set in BranchSnapshot.session

    this._debugStack = (new Error()).stack

    this._snapshot = null
    this._snapshotIdx = -1
    this._snapshotSessions = []

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
    if (this._snapshotIdx !== -1) {
      const head = this._snapshot._snapshotSessions.pop()
      if (head !== this) {
        this._snapshot._snapshotSessions[this._snapshotIdx] = head
        head._snapshotIdx = this._snapshotIdx
      }
      this.emit('close')
      return
    }
    await this._core.closeSession(this)
    this.emit('close')
  }

  open () {
    if (this._opening) return this._opening
    this._opening = this._open()
    this._opening.catch(safetyCatch)
    return this._opening
  }

  close () {
    if (this._closing) return this._closing
    this._closing = this._close()
    this._closing.catch(safetyCatch)
    return this._closing
  }

  // LinearizedCoreSession API

  get view () {
    return this._core.view
  }

  get status () {
    return this._core.status
  }

  unwrap () {
    return this.session({ unwrap: true })
  }

  wrap () {
    return this.session({ unwrap: false })
  }

  // Hypercore API

  get length () {
    return this._core.length
  }

  snapshot (opts) {
    return this.session({ ...opts, snapshot: true })
  }

  checkout (clock) {
    return this.session({ checkout: clock })
  }

  session (opts) {
    if (this._snapshotted) {
      // If a session is made on a snapshot, it's always bound to that snapshot
      const session = new LinearizedCoreSession(this._core, opts)
      session._snapshotIdx = this._snapshotSessions.push(session) - 1
      session._snapshot = this
      return session
    }
    return this._core.session({ ...this.opts, ...opts })
  }

  update () {
    return this._core.updateSession(this)
  }

  async append (blocks) {
    // Must only be called from within an apply function
    return this._core.append(blocks)
  }

  async get (seq, opts = {}) {
    let block = await this._core.get(seq, { ...opts, active: this._activeRequests })
    if (!block) return null
    if (this._unwrap) block = block.value
    return opts.valueEncoding ? opts.valueEncoding.decode(block) : block
  }
}

module.exports = LinearizedCore

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}

function migrateSession (from, to, session) {
  const head = from._sessions.pop()
  if (head !== session) {
    from._sessions[session._sessionIdx] = head
    head._sessionIdx = session._sessionIdx
  }

  session._sessionIdx = to._sessions.push(session) - 1
  session._core = to
  if (session._snapshotSessions.length) {
    for (const s of session._snapshotSessions) {
      s._core = to
    }
  }
}
