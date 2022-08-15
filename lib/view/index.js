const safetyCatch = require('safety-catch')
const debounceify = require('debounceify')

const { eq } = require('../clock')
const { OutputNode } = require('../nodes')
const LinearizedCore = require('./core')
const MemoryTracker = require('./trackers/causal')
const OutputsTracker = require('./trackers/outputs')

const MAX_GET_RETRIES = 32

class LinearizedView {
  constructor (autobase, opts = {}) {
    this.autobase = autobase
    this.root = opts.root || this
    this.clock = opts.clock || null
    this.outputs = opts.outputs || null
    this.nodes = opts.nodes || new Map()
    this.lengths = opts.lengths || new Map()

    this.applyFunction = opts.apply || defaultApply
    this.openFunction = opts.open
    this.userView = null

    this._isRoot = this.root === this
    this._sessionsById = new Map()
    this._deltas = null

    this._writable = opts.writable === true
    this._applying = null

    this.cores = []
    for (let i = 0; i < (opts.views || 1); i++) {
      this.cores.push(new LinearizedCore(this, i))
    }

    this.update = debounceify(this._update.bind(this))
  }

  get _hasSnapshots () {
    // TODO: Make an index for this
    for (const sessions of this._sessionsById.values()) {
      for (const session of sessions) {
        if (session._snapshotted) return true
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
      lengths: new Map([...this.lengths]),
      nodes: new Map([...this.nodes].map(([id, nodes]) => [id, [...nodes]]))
    })
  }

  _migrateSnapshot (snapshot, clone) {
    if (!clone) {
      clone = this._clone()
    }
    let cloneSessions = clone._sessionsById.get(snapshot.id)
    if (!cloneSessions) {
      cloneSessions = []
      clone._sessionsById.set(id, cloneSessions)
    }
    const sessions = this._sessionsById.get(snapshot.id)
    const head = sessions.pop()
    if (head !== snapshot) {
      sessions[snapshot._sessionIdx] = head
      head._sessionIdx = snapshot._sessionIdx
    }
    if (!sessions.length) {
      this._sessionsById.delete(snapshot.id)
    }
    snapshot._sessionIdx = cloneSessions.push(snapshot) - 1
    snapshot._view = clone
  }

  _migrateSnapshots () {
    const clone = this._clone()
    for (const sessions of this._sessionsById.values()) {
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

  _coreClose (session) {
    const sessions = this._sessionsById.get(session.id)
    const head = sessions.pop()
    if (head !== session) {
      sessions[session._sessionIdx] = head
      head._sessionIdx = session._sessionIdx
    }
    if (!sessions.length) {
      this._sessionsById.delete(session.id)
    }
    if (this._sessionsById.size) return
    return this._close()
  }

  _coreSession (session, opts = {}) {
    const newSession = new LinearizedCore(this, session.id, opts)
    const sessions = this._sessionsById.get(session.id)
    session._sessionIdx = sessions.push(newSession) - 1
    return newSession
  }

  async _coreUpdate (session) {
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

    if (this._sessionsById.size) await this._close()
  }

  _coreAppend (session, block, opts) {
    if (!this._applying) throw new Error('Append on a LinearizedCore can only be called within apply')
    let nodes = this.nodes.get(session.id)
    if (!nodes) {
      nodes = []
      this.nodes.set(session.id, nodes)
    }
    if (!Array.isArray(block)) block = [block]
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
  }

  async _coreInnerGet (session, seq, opts) {
    const output = this.outputs.get(session.id)
    const nodes = this.nodes.get(session.id)
    if (!output) return null

    const length = this._coreLength(session)
    if (seq < 0 || seq > length - 1) return null

    if (seq >= output.length) return nodes[seq - output.length]
    return output.get(seq, opts)
  }

  async _coreGet (session, seq, opts) {
    let retries = MAX_GET_RETRIES
    let block = null
    while (retries-- > 0) {
      block = await this._coreInnerGet(seq, opts)
      if (block) return block
      await this._rebuild(this.clock)
    }
    throw new Error(`Linearization could not be rebuilt after ${MAX_GET_RETRIES} attempts`)
  }

  _coreLength (session) {
    const output = this.output.get(session.id)
    const nodes = this.nodes.get(session.id)
    if (!output) return 0
    if (!nodes) return output.length
    return output.length + nodes.length
  }

  _coreStatus (session) {
    return this._deltas.get(session.id)
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
    if (eq(clock, this.clock)) {
      this._deltas = null
      return
    }

    // Next check if any snapshot sessions need to be migrated to a root clone before the update
    if (this._isRoot && this._hasSnapshots) {
      this._migrateSnapshots()
    }

    // Next perform the update
    return this._rebuild(clock)
  }

  _emitUpdateEvents (deltas) {
    for (const [id, delta] of deltas) {
      const sessions = this._sessionsById.get(id)
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
    const clocks = {
      local: inputNode.clock,
      global: this._applying.clock
    }

    let startLength = 0
    const startLengths = new Map()
    for (const [id, nodes] of this.nodes) {
      startLength += nodes.length
      lengths.set(id, nodes.length)
    }

    try {
      await this.applyFunction(this.userView, batch, clocks, node.change)
    } catch (err) {
      this._rollback(startLengths)
      throw err
    }

    let endLength = 0
    for (const nodes of this.nodes.values()) {
      endLength += nodes.length
    }

    if (endLength === startLength) {
      throw new Error('For now, every apply call must append at least one value')
    }

    for (const [key, nodes] of this.nodes.values) {
      const start = startLengths.get(key)
      for (let j = start; j < nodes.length; j++) {
        const change = nodes[j]
        change.batch[0] = j - start
        change.batch[1] = nodes.length - j - 1
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

  async _openOutputs () {
    const updates = []
    const coreSnapshots = new Map()
    for (const cores of this._autobase._outputsByKey.values()) {
      updates.push(...cores.map(c => c.update()))
    }
    await Promise.all(updates)
    for (const [key, cores] of this._autobase._outputsByKey) {
      coreSnapshots.set(key, cores.map(c => c.snapshot()))
    }
    return coreSnapshots
  }

  async _closeOutputs (outputs) {
    await Promise.all([...outputs.values].map(v => v.map(c => c.close())).flatten())
  }

  _lengths () {
    const lengths = new Map()
    for (const core of this.cores) {
      lengths.set(core.id, this._coreLength(core))
    }
    return lengths
  }

  _sameOutputForks (outputs1, outputs2) {
    // TODO: Implement
    return false
  }

  _computeTruncations (lengths, intersections) {
    const truncations = new Map()
    for (let i = 0; i < this.cores.length; i++) {
      const id = this.cores[i].id
      const currentLength = lengths.get(id)

      if (intersections.memory) {
        truncations.set(id, currentLength - intersections.memory.get(id))
      } else if (intersections.outputs) {
        if (intersections.oldOutputs) {
          truncations.set(id, currentLength - intersections.oldOutputs.get(id))
        } else {
          truncations.set(id, 0)
        }
      }
    }
    return truncations
  }

  _computeDeltas (truncations) {
    const deltas = new Map()
    for (const [id, truncated] of truncations) {
      const core = this.cores.get(id)
      deltas.set(id, {
        appended: this._coreLength(core) - truncated,
        truncated
      })
    }
    return deltas
  }

  async _rebuild (clock) {
    if (!this.autobase.opened) await this.autobase.ready()

    const outputs = await this._openOutputs()
    const lengths = this._lengths()

    const outputsTracker = new OutputsTracker(outputs)
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
    if (intersections.output && this.head) {
      intersections.oldOutputs = await outputs.intersect(this.head)
    } else if (intersections.memory) {
      // If we intersected with the memory branch first, just extend and reuse outputs
      await this._closeOutputs(outputs)
    }

    // Truncations are computed before new nodes are appended during apply
    // This will also slice values out of this.nodes if necessary
    const truncations = this._computeTruncations(lengths, intersections)

    // Update in-memory nodes according to truncations
    for (const [id, nodes] of this.nodes) {
      nodes.splice(0, truncations.get(id))
    }

    // Pending will be mutated in _apply
    const updated = pending.length > 0
    try {
      await this._apply(pending)
    } catch (err) {
      safetyCatch(err)
      await this._rollback(this.clock)
    }

    const deltas = this._computeDeltas(truncations)

    if (this._writable && this.autobase.isIndexing) {
      await this._persist(deltas)
    }

    this.head = pending[0]
    this.clock = clock
    this._deltas = deltas
    this._emitUpdateEvents(deltas)

    return updated
  }

  async _persistDelta (id, core, delta) {
    const nodes = this.nodes.get(id)
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
    for (const [id, delta] of deltas) {
      const output = this.autobase._localOutputsByKey.get(id)
      promises.push(this._persistDelta(id, output, delta))
    }
    return Promise.all(promises)
  }
}

module.exports = LinearizedView

async function raceCausalUpdates (causalStream, causalTracker, outputsTracker) {
  const pending = []
}


function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}

function lengths (nodes) {
  return new Map([...nodes].map(([id, n]) => [id, n.length]))
}
