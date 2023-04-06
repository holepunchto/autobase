const ReadyResource = require('ready-resource')
const c = require('compact-encoding')

module.exports = class LinearizedCore extends ReadyResource {
  constructor (base, core, name, valueEncoding) {
    super()

    this.name = name
    this.base = base
    this.core = core

    this.indexedLength = 0
    this.isAutobase = true
    this.tip = []

    this.sessions = []
    this.snapshots = []

    // managed by base
    this.appending = 0
    this.truncating = 0
    this.indexing = 0

    this.valueEncoding = valueEncoding || c.from('binary')

    this.ready().catch(noop)
  }

  openSession (opts = {}) {
    const valueEncoding = opts.valueEncoding ? c.from(opts.valueEncoding) : this.valueEncoding
    return new LinearizedCoreSession(this, valueEncoding)
  }

  closeSession (s) {
    popAndSwap(this.sessions, this.sessions.indexOf(s))
    if (s.snapshotted) {
      popAndSwap(this.snapshots, this.snapshots.indexOf(s))
    }

    if (this.core.replicator) this.core.replicator.clearRequests(s.activeRequests, null)
  }

  getLength () {
    return this.indexedLength + this.tip.length
  }

  async _open () {
    const index = await this.base.system.getIndex(this.name)

    this.indexedLength = index ? index.length : 0
    const length = this.getLength()

    for (const s of this.sessions) {
      if (s.snapshotted) {
        s.snapshotLength = length
        s.snapshotIndexedLength = this.indexedLength
      }
    }
  }

  async append (buffers) {
    if (this.opened === false) await this.ready()

    for (let i = 0; i < buffers.length; i++) {
      this.tip.push(buffers[i])
    }

    this.base._onuserappend(this, buffers.length)

    const length = this.indexedLength + this.tip.length

    for (const s of this.sessions) {
      if (s.snapshotted) continue
      s.emit('append', length)
    }
  }

  // triggered by base
  _onindex (added) {
    // a bit over aggressive, but makes it easy atm. we need some in memory copy-on-write list abstraction instead
    for (const snapshot of this.snapshots) {
      if (snapshot.tip === null) {
        snapshot.tip = this.tip.slice(0)
      }
    }

    this.indexedLength += added
    this.tip = this.tip.slice(added)

    for (const s of this.sessions) {
      if (s.snapshotted) continue
      s.emit('indexed', this.indexedLength)
    }
  }

  // triggered by base
  _onundo (removed) {
    const oldLength = this.getLength()
    const newLength = oldLength - removed

    for (const snapshot of this.snapshots) {
      if (snapshot.snapshotLength > newLength && snapshot.tip === null) {
        snapshot.tip = this.tip.slice(0)
      }
    }

    while (removed-- > 0) this.tip.pop()

    for (const s of this.sessions) {
      if (s.snapshotted) continue
      s.emit('truncate', oldLength, 0)
    }
  }
}

class LinearizedCoreSession extends ReadyResource {
  constructor (core, valueEncoding) {
    super()

    this.core = core
    this.valueEncoding = valueEncoding
    this.activeRequests = []
    this.snapshotLength = 0
    this.snapshotIndexedLength = 0
    this.snapshotted = false
    this.fork = 0
    this.tip = null
    this.writable = true
    this.isAutobase = true

    this.core.sessions.push(this)
  }

  snapshot (opts) {
    const core = this._session(opts)

    if (this.snapshotted) {
      core.snapshotLength = this.snapshotLength
      core.snapshotIndexedLength = this.snapshotIndexedLength
      core.tip = this.tip
    } else {
      core.snapshotLength = this.core.getLength()
      core.snapshotIndexedLength = this.core.indexedLength
      core.tip = this.core.tip.slice()
    }

    core.snapshotted = true
    this.core.snapshots.push(core)
    return core
  }

  session (opts) {
    return this.snapshotted ? this.snapshot(opts) : this._session(opts)
  }

  _session (opts) {
    if (this.closing) { // Hypercore compat
      const err = new Error('SESSION_CLOSED: Cannot make sessions on a closing core')
      err.code = 'SESSION_CLOSED'
      throw err
    }
    return this.core.openSession({ valueEncoding: this.valueEncoding, ...opts })
  }

  _open () {
    return this.core.ready()
  }

  _close () {
    this.core.closeSession(this)
  }

  get name () {
    return this.core.name
  }

  get indexedLength () {
    return this.snapshotted ? this.snapshotIndexedLength : this.core.indexedLength
  }

  get length () {
    return this.snapshotted ? this.snapshotLength : this.core.getLength()
  }

  get byteLength () {
    return 0 // TODO: not hard, just can come later
  }

  async seek () {
    // TODO: again, not hard, just can come later
    throw new Error('Seek not yet implemented')
  }

  async update () {
    await this.core.base.update()

    if (this.snapshotted) {
      this.tip = null
      this.snapshotLength = this.core.getLength()
      this.snapshotIndexedLength = this.core.indexedLength
    }
  }

  async _getWithTip (seq, tip, activeRequests) {
    if (seq >= this.length || seq < 0) throw new Error('Out of bounds get')

    if (seq < this.indexedLength) return this.core.core.get(seq, { activeRequests })
    return tip[seq - this.indexedLength]
  }

  async get (seq, opts) {
    if (this.opened === false) await this.ready()

    const tip = this.tip || this.core.tip
    const block = await this._getWithTip(seq, tip, this.activeRequests)

    const valueEncoding = opts && opts.valueEncoding ? c.from(opts.valueEncoding) : this.valueEncoding
    return c.decode(valueEncoding, block)
  }

  async truncate (len) {
    throw new Error('Cannot truncate a linearized core')
  }

  async append (buffers) {
    if (this.snapshotted) throw new Error('Cannot append to a snapshot')

    if (!Array.isArray(buffers)) buffers = [buffers]

    const appending = []
    for (let i = 0; i < buffers.length; i++) {
      appending.push(c.encode(this.valueEncoding, buffers[i]))
    }
    await this.core.append(appending)

    return { length: this.length }
  }
}

function noop () {}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}
