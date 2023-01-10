const ReadyResource = require('ready-resource')
const c = require('compact-encoding')

module.exports = class LinearizedCore extends ReadyResource {
  constructor (base, core, name) {
    super()

    this.name = name
    this.base = base
    this.core = core

    this.indexedLength = 0
    this.tip = []

    this.sessions = []
    this.snapshots = []

    // managed by base
    this.appending = 0
    this.truncating = 0
    this.indexing = 0

    this.ready().catch(noop)
  }

  openSession (opts = {}) {
    const valueEncoding = opts.valueEncoding ? c.from(opts.valueEncoding) : c.from('binary')
    return new LinearizedCoreSession(this, valueEncoding)
  }

  closeSession (s) {
    popAndSwap(this.sessions, this.sessions.indexOf(s))
    if (s.isSnapshot) {
      popAndSwap(this.snapshots, this.snapshots.indexOf(s))
    }

    this.core.replicator.clearRequests(s.activeRequests, null)
  }

  getLength () {
    return this.indexedLength + this.tip.length
  }

  async _open () {
    const index = await this.base.system.getIndex(this.name)

    this.indexedLength = index ? index.length : 0
    const length = this.getLength()

    for (const s of this.sessions) {
      if (s.isSnapshot) s.snapshotLength = length
    }
  }

  async getWithTip (seq, tip, activeRequests) {
    if (this.opened === false) await this.ready()

    const length = this.getLength()

    if (seq > length || seq < 0) throw new Error('Out of bounds get')

    return seq < this.indexedLength ? this.core.get(seq, { activeRequests }) : tip[seq - this.indexedLength]
  }

  async append (buffers) {
    if (this.opened === false) await this.ready()

    for (let i = 0; i < buffers.length; i++) {
      this.tip.push(buffers[i])
    }

    this.base._onuserappend(this, buffers.length)

    const length = this.indexedLength + this.tip.length

    for (const s of this.sessions) {
      if (s.isSnapshot) continue
      s.emit('append', length)
    }
  }

  // triggered by base
  _onindex (added) {
    this.indexedLength += added
    this.tip = this.tip.slice(added)
  }

  // triggered by base
  _onundo (removed) {
    const oldLength = this.length
    const newLength = oldLength - removed

    for (const snapshot of this.snapshots) {
      if (snapshot.snapshotLength > newLength) {
        snapshot.tip = this.tip.slice(0)
      }
    }

    while (removed-- > 0) this.tip.pop()

    for (const s of this.sessions) {
      if (s.isSnapshot) continue
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
    this.isSnapshot = false
    this.fork = 0
    this.tip = null
    this.writable = true

    this.core.sessions.push(this)
  }

  snapshot (opts) {
    const core = this.session(opts)
    core.snapshotLength = this.core.getLength()
    core.isSnapshot = true
    this.core.snapshots.push(core)
    return core
  }

  session (opts) {
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
    return this.core.indexedLength
  }

  get length () {
    return this.isSnapshot ? this.snapshotLength : this.core.getLength()
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

    if (this.isSnapshot) {
      this.tip = null
      this.snapshotLength = this.core.getLength()
    }
  }

  async get (seq, opts) {
    if (this.opened === false) await this.ready()

    const tip = this.tip || this.core.tip
    const block = await this.core.getWithTip(seq, tip, this.activeRequests)

    const valueEncoding = opts && opts.valueEncoding ? c.from(opts.valueEncoding) : this.valueEncoding
    return c.decode(valueEncoding, block)
  }

  async truncate (len) {
    throw new Error('Cannot truncate a linearized core')
  }

  async append (buffers) {
    if (this.isSnapshot) throw new Error('Cannot append to a snapshot')

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
