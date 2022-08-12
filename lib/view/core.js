const { EventEmitter } = require('events')
const debounceify = require('debounceify')

const { OutputNode } = require('../nodes')

const PROMISES = Symbol.for('hypercore.promises')
const MAX_GET_RETRIES = 32

class LinearizedCore {
  constructor (autobase, view, index, opts = {}) {
    this.autobase = autobase
    this.view = view
    this.index = index

    this.header = opts.header
    this.root = opts.root || this
    this.nodes = opts.nodes || []
    this.clock = null

    this.lastUpdate = opts.lastUpdate

    this.status = opts.status || { appended: 0, truncated: 0 }
    this.length = opts.length || 0

    this._writable = opts.writable !== false
    this._applying = null
    this._sessions = []

    this.update = debounceify(this._update.bind(this))
  }

  get isRoot () {
    return this.root === this
  }

  _rollback (clock) {
    if (this.nodes.length) {
      // If we have nodes, then the last applied clock is the clock (useful in the case of partial applies)
      // If apply was ever called, then this.nodes will be populated
      this.clock = this.nodes[this.nodes.length - 1].clock
    }
  }

  _emitStatus (status) {
    for (const session of this._sessions) {
      if (this.status.truncated) session.emit('truncate', status.length)
      if (this.status.appended) session.emit('append')
    }
  }

  async _commit () {
    this.length = this.status.length
    this.nodes = this.status.nodes

    const localOutput = this.autobase.localOutputs[this.index]
    if (this._writable && localOutput) {
      if (this.status.truncated) {
        await localOutput.truncate(localOutput.length - this.status.truncated)
      }
      if (localOutput.length === 0 && this.nodes.length) {
        this.nodes[0].header = this.header
      }
      await localOutput.append(this.nodes)
    }
  }

  async _update () {
    // First check in the view if there's work to be done. If not, short-circuit
    if (!(await this.view._shouldUpdate(this.clock))) return

    // Next check if any snapshot sessions need to be migrated to a root clone before the update
    if (this.isRoot) {
      const snapshots = this._sessions.filter(s => s._snapshotted)
      for (const snapshot of snapshots) {
        migrateSession(this, this.clone(), snapshot)
      }
    }

    // Next perform the update
    await this.view._rebuild()
  }

  async updateSession (session) {
    if (this.autobase._loadingInputsCount > 0) {
      await this.autobase._waitForInputs()
    }
    if (!session._snapshotted) return this.update()
    // If the session is a snapshot and this LinearizedCore is not the root, migrate the session to the root and update the root.
    await this.root.update()

    if (session._core === this.root) return

    const oldLength = session._core.length
    migrateSession(session._core, this.root, session)

    if (this.root.length < oldLength) {
      session.emit('truncate', this.root.length)
    }
    if (this.root.length > oldLength) {
      session.emit('append')
    }

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
      await this.view._rebuild(this.lastUpdate.clock)
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
      status: this.status,
      length: this.length
    })
  }
}

class LinearizedCoreSession extends EventEmitter {
  constructor (core, opts = {}) {
    super()
    this[PROMISES] = true
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
      return
    }
    return this._core.closeSession(this)
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
