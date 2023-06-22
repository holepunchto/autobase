const { EventEmitter } = require('events')
const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const c = require('compact-encoding')
const assert = require('nanoassert')
const b4a = require('b4a')
const {
  SESSION_CLOSED,
  BLOCK_NOT_AVAILABLE
} = require('hypercore/errors')

const BULK_SHIFT = 32

class Snapshot {
  constructor (source, update) {
    this.index = source._pendingSnapshots.push(this) - 1
    this.fork = 0
    this.length = 0
    this.byteLength = 0
    this.nodes = null
    this.source = source
    if (update) this.update()
  }

  clone () {
    const snap = new Snapshot(this.source, false)
    snap.fork = this.fork
    snap.length = this.length
    snap.byteLength = this.byteLength
    snap.nodes = this.nodes
    return snap
  }

  getIndexedLength () {
    const sharedLength = this.length - (this.nodes === null ? 0 : this.nodes.length)
    return Math.min(this.source.indexedLength, sharedLength)
  }

  getIndexedByteLength () {
    const sharedByteLength = this.nodes === null || this.nodes.length === 0
      ? this.byteLength
      : this.nodes[0].byteOffset

    return Math.min(this.source.indexedByteLength, sharedByteLength)
  }

  update () {
    if (this.nodes !== null) { // reattach
      this.index = this.source._pendingSnapshots.push(this) - 1
      this.nodes = null
    }
    this.fork = this.source.fork
    this.length = this.source.length
    this.byteLength = this.source.byteLength
  }

  clear () {
    if (this.nodes === null) this.detach(null)
    this.source = null
  }

  detach (nodes) {
    this.nodes = nodes
    const top = this.source._pendingSnapshots.pop()
    if (top === this) return
    this.source._pendingSnapshots[top.index = this.index] = top
  }
}

class AutocoreSession extends EventEmitter {
  constructor (source, snapshot, valueEncoding) {
    super()

    this.isAutobase = true

    this.closing = false
    this.closed = source.closed
    this.opened = false

    this.writable = true // TODO: make this configurable

    this.activeRequests = []
    this.valueEncoding = valueEncoding || null

    this._source = source
    this._index = source.sessions.push(this) - 1
    this._snapshot = snapshot

    this.ready().catch(safetyCatch)
  }

  get id () {
    return this.source.core.id
  }

  get key () {
    return this.source.core.key
  }

  get snapshotted () {
    return this._snapshot !== null
  }

  get discoveryKey () {
    return this.source.core.discoveryKey
  }

  get fork () {
    return this._snapshot === null ? this._source.fork : this._snapshot.fork
  }

  get byteLength () {
    return this._snapshot === null ? this._source.byteLength : this._snapshot.byteLength
  }

  get length () {
    return this._snapshot === null ? this._source.length : this._snapshot.length
  }

  get indexedByteLength () {
    return this._snapshot === null ? this._source.indexedByteLength : this._snapshot.getIndexedByteLength()
  }

  get indexedLength () {
    return this._snapshot === null ? this._source.indexedLength : this._snapshot.getIndexedLength()
  }

  async ready () {
    if (this.opened) return
    await this._source.ready()
    if (this.opened) return
    this.opened = true
    this.emit('ready')
  }

  async getUserData (name) {
    if (this.opened === false) await this.ready()
    return await this._source.core.getUserData(name)
  }

  async setUserData (name, value, opts) {
    if (this.opened === false) await this.ready()
    return await this._source.core.setUserData(name, value, opts)
  }

  snapshot ({ valueEncoding = this.valueEncoding } = {}) {
    if (this.closing) throw SESSION_CLOSED()

    return this._snapshot === null
      ? this._source.createSnapshot(valueEncoding)
      : this._source._createSession(this._snapshot.clone(), valueEncoding)
  }

  session ({ valueEncoding = this.valueEncoding, snapshot = this._snapshot !== false } = {}) {
    if (this.closing) throw SESSION_CLOSED()

    return snapshot
      ? this.snapshot({ valueEncoding })
      : this._source.createSession(valueEncoding)
  }

  async update (opts) {
    await this._source.update(opts)
    if (this._snapshot !== null) this._snapshot.update()
    return true
  }

  async seek (byteOffset, opts) {
    if (this.opened === false) await this.ready()

    if (byteOffset < this.indexedByteLength) {
      return this._source.core.seek(byteOffset, { activeRequest: this.activeRequests, ...opts })
    }

    const nodes = this._getNodes()
    const offset = nodes === this._source.nodes ? this._source._shifted : 0

    let start = offset
    let end = nodes.length

    while (end - start > 1) {
      const mid = (start + end) >>> 1

      if (nodes[mid].byteOffset <= byteOffset) start = mid
      else end = mid
    }

    if (start >= end) return null

    const node = nodes[start]
    if (node.byteOffset + node.block.byteLength <= byteOffset) return null

    return [start - offset + this.indexedLength, byteOffset - node.byteOffset]
  }

  async get (index, opts) {
    if (this.opened === false) await this.ready()

    opts = { activeRequest: this.activeRequests, valueEncoding: this.valueEncoding, ...opts }

    // check if we indexed this already
    if (index < this.indexedLength) return this._source.core.get(index, opts)

    // TODO: this should wait for the block to arrive if not a snap, per hypercore semantics...
    if (index >= this.length) {
      throw BLOCK_NOT_AVAILABLE()
    }

    const nodes = this._getNodes()

    if (nodes === this._source.nodes) {
      index -= this.indexedLength
      index += this._source._shifted
    } else {
      // nodes might have been indexed since we copied in the nodes, so calculate the indexedLength at that time
      index -= (this._snapshot.length - nodes.length)
    }

    const blk = index >= 0 && index < nodes.length ? nodes[index].block : null
    const enc = (opts.valueEncoding && c.from(opts.valueEncoding)) || this.valueEncoding

    return enc ? c.decode(enc, blk) : blk
  }

  async truncate (newLength) {
    if (this.opened === false) await this.ready()

    throw new Error('Truncating an Autobased index explicitly is not currently supported')
  }

  async append (block) {
    if (this.opened === false) await this.ready()

    const blocks = Array.isArray(block) ? block : [block]
    const buffers = new Array(blocks.length)

    for (let i = 0; i < blocks.length; i++) {
      const blk = blocks[i]

      if (b4a.isBuffer(blk)) buffers[i] = blk
      else if (this.valueEncoding) buffers[i] = c.encode(this.valueEncoding, blk)
      else buffers[i] = b4a.from(blk)
    }

    return this._source._append(buffers)
  }

  async close () {
    this.closing = true
    if (this.opened === false) await this.ready()

    if (this.closed) return
    this.closed = true

    if (this._snapshot !== null) this._snapshot.clear()

    const top = this._source.sessions.pop()
    if (top !== this) this._source.sessions[top._index = this._index] = top

    // TODO: clear active requests also!

    this.emit('close', false)
  }

  _getNodes () {
    return (this._snapshot === null || this._snapshot.nodes === null) ? this._source.nodes : this._snapshot.nodes
  }
}

module.exports = class Autocore extends ReadyResource {
  constructor (base, core, name) {
    super()

    this.indexedLength = core.length
    this.indexedByteLength = core.byteLength

    this.length = core.length
    this.byteLength = core.byteLength

    this.fork = 0

    this.base = base
    this.name = name
    this.core = core

    // managed by base
    this.appending = 0
    this.truncating = 0
    this.indexing = 0

    this.sessions = []
    this.nodes = []

    this._shifted = 0
    this._pendingSnapshots = []

    this.ready().catch(safetyCatch)
  }

  async _open () {
    await this.base._openingCores
    await this.base._ensureUserData(this.core, this.name)

    const index = await this.base.system.getIndex(this.name)
    const expectedLength = index ? index.length : 0

    await this.core.ready()

    // if we crashed between applying an append and the index being written
    // undo, and let the indexer re-append it.
    if (this.core.length !== expectedLength) {
      await this.core.truncate(expectedLength, this.core.fork)
    }

    this.indexedLength = this.core.length
    this.indexedByteLength = this.core.byteLength

    this.length = this.core.length
    this.byteLength = this.core.byteLength

    for (const snap of this._pendingSnapshots) snap.update()
  }

  createSession (valueEncoding) {
    return this._createSession(null, valueEncoding)
  }

  createSnapshot (valueEncoding) {
    return this._createSession(new Snapshot(this, this.opened), valueEncoding)
  }

  _createSession (snapshot, valueEncoding) {
    return new AutocoreSession(this, snapshot, valueEncoding ? c.from(valueEncoding) : null)
  }

  truncate (newLength) {
    this._truncateAndDetach(newLength)
    this.fork++

    for (const session of this.sessions) {
      if (session.snapshotted === false) session.emit('truncate', newLength, this.fork)
    }
  }

  update (opts) {
    return this.base.update(opts)
  }

  indexBatch (start, end) {
    const blocks = new Array(end - start)
    for (let i = 0; i < blocks.length; i++) {
      blocks[i] = this.nodes[this._shifted + start + i].block
    }
    return blocks
  }

  // called by autobase
  _onindex (added) {
    const head = this.nodes[this._shifted + added - 1]

    this.indexedLength += added
    this.indexedByteLength = head.byteOffset + head.block.byteLength

    this._shifted += added
    if (this._shifted >= BULK_SHIFT || this._shifted === this.nodes.length) this._gc()
  }

  // called by autobase
  _onundo (removed) {
    const newLength = this.length - removed
    this.truncate(newLength)
  }

  _append (batch) {
    for (let i = 0; i < batch.length; i++) {
      const block = batch[i]
      this.nodes.push({ byteOffset: this.byteLength, block })
      this.byteLength += block.byteLength
    }

    this.length += batch.length
    this.base._onuserappend(this, batch.length)

    for (const session of this.sessions) {
      if (session.snapshotted === false) session.emit('append')
    }
  }

  _gc () {
    if (this._shifted === 0) return
    this.nodes = this.nodes.slice(this._shifted)
    this._shifted = 0
  }

  _truncateAndDetach (sharedLength) {
    assert(this.indexedLength <= sharedLength && sharedLength <= this.length, 'Invalid truncation')

    let maxSnap = 0
    for (const snap of this._pendingSnapshots) {
      if (snap.length > sharedLength && maxSnap < snap.length) {
        maxSnap = snap.length
      }
    }

    if (maxSnap <= sharedLength) {
      while (this.length > sharedLength) {
        const { block } = this.nodes.pop()
        this.length--
        this.byteLength -= block.byteLength
      }
      return
    }

    this._gc()

    for (let i = this._pendingSnapshots.length - 1; i >= 0; i--) {
      const snap = this._pendingSnapshots[i]
      if (snap.length > sharedLength) snap.detach(this.nodes)
    }

    const firstRemoved = this.nodes[sharedLength - this.indexedLength]

    this.nodes = this.nodes.slice(0, sharedLength)
    this.length = sharedLength
    this.byteLength = firstRemoved.byteOffset
  }
}
