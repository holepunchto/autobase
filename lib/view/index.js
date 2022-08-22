const safetyCatch = require('safety-catch')
const debounceify = require('debounceify')

const { eq } = require('../clock')
const { OutputNode } = require('../nodes')
const LinearizedCore = require('./core')
const MemoryTracker = require('./trackers/memory')
const { IntersectionStatus, OutputsTracker } = require('./trackers/outputs')

const MAX_GET_RETRIES = 32

class LinearizedView {
  constructor (autobase, opts = {}) {
    this.autobase = autobase
    this.root = opts.root || this
    this.clock = opts.clock || null
    this.outputs = opts.outputs || null
    this.nodes = opts.nodes || new Array(this.autobase._viewCount)
    this.lengths = opts.lengths || new Array(this.autobase._viewCount)
    this.header = opts.header

    this.applyFunction = opts.apply || defaultApply
    this.openFunction = opts.open
    this.userView = null

    this._isRoot = this.root === this
    this._sessions = new Array(this.autobase._viewCount)
    this._deltas = null

    this._writable = opts.writable === true
    this._applying = null

    this.cores = []
    for (let i = 0; i < this.autobase._viewCount; i++) {
      this.cores.push(new LinearizedCore(this, i))
    }

    this.userView = this._createUserView()
    this._pinnedView = this._createUserView({ pin: true })

    this.update = debounceify(this._update.bind(this))
  }

  _createUserView (opts = {}) {
    const cores = this.cores.map(c => c.session({ unwrap: true, pin: opts.pin }))
    if (this.openFunction) return this.openFunction(...cores)
    return cores
  }

  get _hasSnapshots () {
    // TODO: Make an index for this
    for (const sessions of this._sessions) {
      if (!sessions) continue
      for (const session of sessions) {
        if (session._snapshotted && !session._pinned) return true
      }
    }
    return false
  }

  _clone () {
    return new LinearizedView(this.autobase, {
      root: this.root,
      clock: this.clock,
      outputs: this.outputs,
      // TODO: Use a copy-on-write data structure to avoid needing to duplicate in-memory nodes here.
      lengths: [...this.lengths],
      nodes: [...this.nodes].map(nodes => [...nodes])
    })
  }

  _close () {
    if (!this.outputs) return
    return this._closeOutputs(this.outputs)
  }

  _migrateSnapshot (snapshot, clone) {
    if (snapshot._pinned) return
    if (!clone) {
      clone = this._clone()
    }
    let cloneSessions = clone._sessions[snapshot._id]
    if (!cloneSessions) {
      cloneSessions = []
      clone._sessions[id] = cloneSessions
    }
    const sessions = this._sessions[snapshot._id]
    const head = sessions.pop()
    if (head !== snapshot) {
      sessions[snapshot._sessionIdx] = head
      head._sessionIdx = snapshot._sessionIdx
    }
    if (!sessions.length) {
      this._sessions[snapshot._id] = null
    }
    snapshot._sessionIdx = cloneSessions.push(snapshot) - 1
    snapshot._view = clone
  }

  _migrateSnapshots () {
    const clone = this._clone()
    for (const sessions of this._sessions) {
      if (!sessions) continue
      for (let i = 0; i < sessions.length; i++) {
        const session = sessions[i]
        if (!session._snapshotted) continue
        this._migrateSnapshot(session, clone)
        i--
      }
    }
  }

  // LinearizedCore Interface

  _coreReady (session) {
    // TODO: What to do?
  }

  async _coreClose (session) {
    const sessions = this._sessions[session._id]
    const head = sessions.pop()
    if (head !== session) {
      sessions[session._sessionIdx] = head
      head._sessionIdx = session._sessionIdx
    }
    if (!sessions.length) {
      this._sessions[session._id] = null
    }
    for (const sessions of this._sessions) {
      if (sessions) return
    }
    return this._close()
  }

  _coreSession (session, opts = {}) {
    const newSession = new LinearizedCore(this, session._id, opts)
    let sessions = this._sessions[session._id]
    if (!sessions) {
      sessions = []
      this._sessions[session._id] = sessions
    }
    session._sessionIdx = sessions.push(newSession) - 1
    return newSession
  }

  async _coreUpdate (session) {
    if (session._pinned) return
    if (!session._snapshotted) return this.update()

    // If the session is a snapshot and this LinearizedView is not the root, migrate the session to the root and update the root.
    await this.root.update()

    if (session.view === this.root) return

    const oldLength = session.length
    this._migrateSnapshot(session, this.root)

    if (session.length < oldLength) {
      session.emit('truncate', session.length)
    }
    if (session.length > oldLength) {
      session.emit('append')
    }

    // If there are no sessions after the update, close the view
    for (const sessions of this._sessions) {
      if (sessions) return
    }
    await this._close()
  }

  _coreAppend (session, block, opts) {
    if (!this._applying) throw new Error('Append on a LinearizedCore can only be called within apply')
    let nodes = this.nodes[session._id]
    if (!nodes) {
      nodes = []
      this.nodes[session._id] = nodes
    }
    if (!Array.isArray(block)) block = [block]
    for (const val of block) {
      const node = new OutputNode({
        id: session._id,
        value: val,
        batch: [0, 0],
        change: this._applying.change,
        operations: this._applying.operations,
        clock: this._applying.clock,
        clocks: null // Will be created before the batch is appended
      })
      nodes.push(node)
    }
  }

  async _coreInnerGet (session, seq, opts) {
    const outputLength = (this.outputs && this.outputs.length(session._id)) || 0
    const nodes = this.nodes[session._id]

    const length = this._coreLength(session)
    if (seq < 0 || seq > length - 1) return null

    if (!this.outputs || seq >= outputLength) return nodes[seq - outputLength]
    return this.outputs.get(session._id, seq, opts)
  }

  async _coreGet (session, seq, opts) {
    let retries = MAX_GET_RETRIES
    let block = null
    while (retries-- > 0) {
      block = await this._coreInnerGet(session, seq, opts)
      if (block) return block
      await this._rebuild(this.clock)
    }
    throw new Error(`Linearization could not be rebuilt after ${MAX_GET_RETRIES} attempts`)
  }

  _coreLength (session) {
    const outputLength = this.outputs && this.outputs.length(session._id)
    const nodes = this.nodes[session._id]
    let length = 0
    if (nodes) {
      length += nodes.length
    }
    if (outputLength) {
      length += outputLength
    }
    console.log('RETURNING CORE LENGTH:', length)
    return length
  }

  _coreStatus (session) {
    return this._deltas[session._id]
  }

  // Update Mechanics

  async _update () {
    // First wait for the full input set to be available
    if (this.autobase._loadingInputsCount > 0) {
      await this.autobase._waitForInputs()
    }

    // Next check if any work needs to be done
    // If we're building a local index, and the clock is the same, no work is needed.
    // (If we're not building a local index, the state of the remote outputs might have changed, so must update)
    const clock = await this.autobase.latest()
    if (this.clock && eq(clock, this.clock)) {
      this._deltas = null
      return
    }

    // Next check if any snapshot sessions need to be migrated to a root clone before the update
    if (this._isRoot && this._hasSnapshots) {
      this._migrateSnapshots()
    }

    // Next perform the update
    console.log('BEFORE REBUILD')
    const result = await this._rebuild(clock)
    console.log('AT END OF UPDATE')
    return result
  }

  _emitUpdateEvents (deltas) {
    for (let id = 0; id < deltas.length; id++) {
      const delta = deltas[id]
      const sessions = this._sessions[id]
      if (!sessions) continue
      for (const session of sessions) {
        if (delta.truncated) session.emit('truncate', session.length - delta.truncated)
        if (delta.appended) session.emit('append')
      }
    }
  }

  async _applyBatch (batch, node) {
    this._applying = node

    const inputNode = await this.autobase._getInputNode(node.change, node.seq)
    const clocksArg = {
      local: inputNode.clock,
      global: this._applying.clock
    }

    let startLength = 0
    let endLength = 0
    const startLengths = new Array(this.autobase._viewCount)
    const endLengths = new Array(this.autobase._viewCount)
    const reuseClocks = (new Array(this.autobase._viewCount)).fill(false)

    for (let i = 0; i < this.nodes.length; i++) {
      const nodes = this.nodes[i]
      if (!nodes) continue
      startLength += nodes.length
      startLengths[i] = nodes.length
    }

    try {
      await this.applyFunction(...this._pinnedView, batch, clocksArg, node.change)
    } catch (err) {
      console.log('ERR IS:', err)
      this._rollback(startLengths)
      throw err
    }

    for (const nodes of this.nodes) {
      if (!nodes) continue
      endLength += nodes.length
    }
    if (endLength === startLength) {
      throw new Error('For now, every apply call must append at least one value')
    }

    // Do one pass to find the outputs that did not change
    for (let id = 0; id < this.nodes.length; id++) {
      reuseClocks[id] = startLengths[id] === endLengths[id]
    }

    // Compute the full clocks array using the list of stable outputs
    const clocks = new Array(this.autobase._viewCount)
    for (let id = 0; id < clocks.length; id++) {
      if (reuseClocks[id] && this.outputs) {
        clocks[id] = this.outputs.clocks(id)
      } else {
        clocks[id] = { clock: this._applying.clock, operations: this._applying.operations }
      }
    }

    // Finally set both the batch offsets and the full clocks array on all new nodes.
    for (let id = 0; id < this.nodes.length; id++) {
      const nodes = this.nodes[id]
      if (!nodes) continue
      const start = startLengths[id]
      const end = endLengths[id]
      if (start === end) continue
      for (let j = start; j < nodes.length; j++) {
        const node = nodes[j]
        node.batch[0] = j - start
        node.batch[1] = nodes.length - j - 1
        node.clocks = clocks
      }
    }

    this._applying = null
  }

  async _apply (pending) {
    if (!this.userView) {
      const cores = this.cores.map(c => c.session({ pin: true }))
      this.userView = this.openFunction(...cores)
    }
    let batch = []
    while (pending.length) {
      const node = pending.pop()
      batch.push(node)
      if (node.batch[1] > 0) continue
      await this._applyBatch(batch, node)
      batch = []
    }
    if (batch.length) throw new Error('Cannot rebase a partial batch')
  }

  async _closeOutputs (outputs) {
    if (!outputs) return
    await Promise.all([...outputs.values].map(v => v.map(c => c.close())).flatten())
  }

  _lengths () {
    const lengths = new Array(this.autobase._viewCount)
    for (let id = 0; id < this.autobase._viewCount; id++) {
      lengths[id] = this._coreLength(this.cores[id])
    }
    return lengths
  }

  _sameOutputForks (outputs1, outputs2) {
    // TODO: Implement
    return false
  }

  _computeTruncations (lengths, intersections) {
    console.log('computing truncations from lengths:', lengths, 'intersections:', intersections)
    const truncations = new Array(this.autobase._viewCount)
    for (let id = 0; id < this.autobase._viewCount; id++) {
      const currentLength = lengths[id]
      if (intersections.memory) {
        truncations[id] = currentLength - intersections.memory[id]
      } else if (intersections.outputs) {
        if (intersections.oldOutputs) {
          truncations[id] = currentLength - intersections.oldOutputs[id]
        } else {
          truncations[id] = currentLength - intersections.outputs[id]
        }
      } else {
        truncations[id] = currentLength
      }
    }
    return truncations
  }

  _computeDeltas (oldLengths, truncations) {
    const deltas = new Array(this.autobase._viewCount)
    for (let id = 0; id < this.autobase._viewCount; id++) {
      const truncated = truncations[id]
      const oldLength = oldLengths[id] || 0
      const core = this.cores[id]
      deltas[id] = {
        appended: this._coreLength(core) - oldLength + truncated,
        truncated
      }
    }
    return deltas
  }

  async _rebuild (clock) {
    if (!this.autobase.opened) await this.autobase.ready()
    // If indexing, compute the lengths before clearing out the memory state
    const lengths = this._lengths()

    // If we're indexing, clear out the old memory state.
    if (this._writable && this.autobase.isIndexing) {
      this.nodes = []
    }

    const outputsTracker = new OutputsTracker(this.autobase)
    const memoryTracker = new MemoryTracker(this.nodes, lengths)
    const causalStream = this.autobase.createCausalStream({ clock })
    const intersections = {
      oldOutputs: null,
      memory: null,
      outputs: null
    }
    const pending = []

    // First check if the causal stream nodes (the correct ordering) can be found either in:
    //  1) The memory state, in which case we should continue extending the memory state
    //  2) The outputs, in which case we should build on the most up-to-date output
    for await (const node of causalStream) {
      intersections.memory = memoryTracker.update(node)
      if (intersections.memory) break
      if (!outputsTracker.invalid) {
        intersections.outputs = await outputsTracker.update(node)
        if (intersections.outputs) break
      }
      if (intersections.outputs || intersections.memory) break
      pending.push(node)
    }

    // TODO: Implement the fast fork-check
    if (intersections.outputs) {
      if (this.head) {
        const { status, lengths } = await outputsTracker.intersect(this.head, { update: false })
        if (status === IntersectionStatus.Found) {
          intersections.oldOutputs = lengths
        }
      }
      this.outputs = outputsTracker
      console.log('SETTING OUTPUTS TO TRACKER HERE')
    } else if (intersections.memory) {
      // If we intersected with the memory branch first, just extend and reuse outputs
      await this._closeOutputs(intersections.outputs)
      await this._closeOutputs(intersections.oldOutputs)
    }

    // Truncations are computed before new nodes are appended during apply
    // This will also slice values out of this.nodes if necessary
    const truncations = this._computeTruncations(lengths, intersections)
    console.log('TRUNCATIONS:', truncations)

    // Update in-memory nodes according to truncations
    for (let id = 0; id < this.nodes.length; id++) {
      const nodes = this.nodes[id]
      if (!nodes || !truncations[id]) continue
      nodes.splice(0, truncations[id])
    }

    console.log('APPLYING PENDING:', pending.length)
    // Pending will be mutated in _apply
    const updated = pending.length > 0
    try {
      await this._apply(pending)
    } catch (err) {
      console.log('ERR IS:', err)
      safetyCatch(err)
      await this._rollback(this.clock)
    }

    console.log('computing deltas from truncations and old lengths:', truncations, lengths)
    const deltas = this._computeDeltas(lengths, truncations)

    console.log('PERSISTING')
    if (this._writable && this.autobase.isIndexing) {
      await this._persist(deltas)
    }
    console.log('PERSISTED')

    this.head = pending[0]
    this.clock = clock
    this._deltas = deltas
    this._emitUpdateEvents(deltas)
    console.log('DONE WITH REBUILD')

    return updated
  }

  async _persistDelta (id, core, delta) {
    const nodes = this.nodes[id]
    if (!nodes || !nodes.length) return
    if (delta.truncated > 0) {
      await core.truncate(core.length - delta.truncated)
    }
    if (nodes.length) {
      if (core.length === 0) {
        nodes[0].header = this.header
      }
      await core.append(nodes)
    }
  }

  _persist (deltas) {
    const promises = []
    for (let id = 0; id < deltas.length; id++) {
      const delta = deltas[id]
      const output = this.autobase.localOutputs[id]
      promises.push(this._persistDelta(id, output, delta))
    }
    return Promise.all(promises)
  }
}

module.exports = LinearizedView

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}
