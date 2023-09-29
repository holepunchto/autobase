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
    this.tipLength = 0
    this.tip = null
    this.source = source
    if (update) this.update()
  }

  clone () {
    const snap = new Snapshot(this.source, false)
    snap.fork = this.fork
    snap.length = this.length
    snap.byteLength = this.byteLength

    // detached clones should be detached
    if (this.index === -1) {
      snap.detach(this.tip, this.tipLength)
    }

    return snap
  }

  getIndexedLength () {
    const sharedLength = this.length - this.tipLength
    return Math.min(this.source.indexedLength, sharedLength)
  }

  getSignedLength () {
    const sharedLength = this.length - this.tipLength
    return Math.min(this.source.signedLength, sharedLength)
  }

  getIndexedByteLength () {
    const sharedByteLength = this.tipLength === 0
      ? this.byteLength
      : this.tip[0].byteOffset

    return Math.min(this.source.indexedByteLength, sharedByteLength)
  }

  update () {
    if (this.index === -1) { // reattach
      this.index = this.source._pendingSnapshots.push(this) - 1
      this.tip = null
      this.tipLength = 0
    }
    this.fork = this.source.fork
    this.length = this.source.length
    this.byteLength = this.source.byteLength
  }

  clear () {
    if (this.index !== -1) this.detach(null, 0)
    this.source = null
  }

  detach (tip, length) {
    this.tip = tip
    this.tipLength = length
    if (this.index === -1) return
    const top = this.source._pendingSnapshots.pop()
    if (top !== this) this.source._pendingSnapshots[top.index = this.index] = top
    this.index = -1
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
    return this._source.id
  }

  get key () {
    return this._source.key
  }

  get snapshotted () {
    return this._snapshot !== null
  }

  get discoveryKey () {
    return this._source.discoveryKey
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

  get signedLength () {
    return this._snapshot === null ? this._source.signedLength : this._snapshot.getSignedLength()
  }

  getBackingCore () {
    return this._source.core ? this._source.core : null
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
    if (this.closing === true) throw SESSION_CLOSED()

    return await this._source.getUserData(name)
  }

  async setUserData (name, value, opts) {
    if (this.opened === false) await this.ready()
    if (this.closing === true) throw SESSION_CLOSED()

    return await this._source.setUserData(name, value, opts)
  }

  snapshot ({ valueEncoding = this.valueEncoding } = {}) {
    if (this.closing === true) throw SESSION_CLOSED()

    return this._snapshot === null
      ? this._source.createSnapshot(valueEncoding)
      : this._source._createSession(this._snapshot.clone(), valueEncoding)
  }

  session ({ valueEncoding = this.valueEncoding, snapshot = this._snapshot !== false } = {}) {
    if (this.closing === true) throw SESSION_CLOSED()

    return snapshot
      ? this.snapshot({ valueEncoding })
      : this._source.createSession(valueEncoding)
  }

  async update (opts) {
    if (this.opened === false) await this.ready()
    if (this.closing === true) throw SESSION_CLOSED()

    await this._source.update(opts)
    if (this.closing === true) throw SESSION_CLOSED()

    if (this._snapshot !== null) this._snapshot.update()
    return true
  }

  async seek (byteOffset, opts) {
    if (this.opened === false) await this.ready()
    if (this.closing === true) throw SESSION_CLOSED()

    if (byteOffset < this.indexedByteLength) {
      return this._source.seek(byteOffset, { activeRequest: this.activeRequests, ...opts })
    }

    const nodes = this._getNodes()
    const offset = nodes === this._source.nodes ? this._source._shifted : 0

    let start = offset
    let end = nodes === this._source.nodes ? nodes.length : this._snapshot.tipLength

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
    if (this.closing === true) throw SESSION_CLOSED()

    opts = { activeRequest: this.activeRequests, valueEncoding: this.valueEncoding, ...opts }

    // check if we indexed this already
    if (index < this.indexedLength) {
      return this._source.get(index, opts)
    }

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
      index -= this._snapshot.length
      index += this._snapshot.tipLength
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
    if (this.closing === true) throw SESSION_CLOSED()

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
    return (this._snapshot === null || this._snapshot.tip === null) ? this._source.nodes : this._snapshot.tip
  }
}

module.exports = class Autocore extends ReadyResource {
  constructor (base, core, name, indexers, opts = {}) {
    super()

    // set in _open
    this.indexedLength = 0
    this.indexedByteLength = 0
    this.length = 0
    this.byteLength = 0
    this.signedLength = 0 // TODO: use core.sessionLength instead of this

    this.fork = 0

    this.base = base
    this.name = name
    this.originalCore = core
    this.core = null

    this.queued = -1
    this._indexers = indexers ? indexers.length : 0

    this.opts = opts

    // managed by base
    this.appending = 0
    this.truncating = 0
    this.indexing = 0

    // managed by system
    this.likelyIndex = -1
    this.checkpointer = 0

    this.sessions = []
    this.nodes = []

    this.signer = null
    this._migrate = null

    this._shifted = 0
    this._pendingSnapshots = []
    this._lastCheckpoint = null

    this.ready().catch(safetyCatch)
  }

  _isSystem () {
    return this.base.system.core._source === this
  }

  get id () {
    return this.originalCore.id
  }

  get key () {
    return this.originalCore.key
  }

  get discoveryKey () {
    return this.originalCore.discoveryKey
  }

  get latestKey () {
    return this.base._viewStore.deriveKey(this.name) || this.key
  }

  get pendingIndexedLength () {
    return this.indexedLength + this.indexing
  }

  get flushableLength () {
    const signableLength = this.signer.getSignableLength()

    // current core can only flush up to first pending migrate
    return this.queued !== -1 && this.queued < signableLength
      ? this.queued
      : signableLength
  }

  get indexers () {
    return this.base.linearizer.indexers.slice(0, this._indexers)
  }

  async _ensureCore (key, length) {
    let swap = null

    if (!b4a.equals(this.originalCore.key, key)) {
      const encryptionKey = this.base._viewStore.getBlockKey(this.name)
      swap = this.base.store.get({ key, encryptionKey, isBlockKey: true, cache: !!this.originalCore.cache })
      await swap.ready()
    }

    const prevOriginalCore = this.originalCore
    this.originalCore = swap || this.originalCore

    const core = this.originalCore.batch({ autoClose: false, session: true, checkout: length })
    await core.ready()

    // reset state
    if (this.core) await this._updateBatch(core)
    else this.core = core

    if (swap) await prevOriginalCore.close()
  }

  async _updateBatch (core) {
    const prevCore = this.core
    const length = this.length

    // detach all snaps
    for (let i = this._pendingSnapshots.length - 1; i >= 0; i--) {
      const snap = this._pendingSnapshots[i]
      const end = snap.length - this.indexedLength - this._shifted
      snap.detach(this.nodes, end)
    }

    this.core = core
    this.nodes = []
    this._shifted = 0

    this.indexing = 0
    this.appending = 0
    this.truncating = 0

    this._updateCoreState()
    this.queued = this.indexedLength

    if (this.length > length) this._emitAppend()

    await prevCore.close()
  }

  _updateCoreState () {
    this.length = this.core.length
    this.byteLength = this.core.byteLength
    this.signedLength = this.core.indexedLength
    this.indexedLength = this.core.length
    this.indexedByteLength = this.core.byteLength
  }

  async _open () {
    await Promise.resolve() // wait a tick so this doesn't run sync in the constructor...
    await this.base._presystem

    await this.originalCore.ready()

    if (this.base.encryptionKey && !this.originalCore.encryption) {
      await this.originalCore.setEncryptionKey(this.base._viewStore.getBlockKey(this.name), { isBlockKey: true })
    }

    const sys = this.base.system.opened ? this.base.system : this.base._initialSystem

    if (sys) {
      await sys.ready()

      for (let { key, length } of sys.views) {
        if (this.isKeyOwner(key)) {
          // autoroll back if we didn't migrate (we migrate all or nothing!)
          // TODO: this if needs to be more involved when indexers are not append-only, but ok for now
          if (!this.base.system.opened && sys.core.manifest.multipleSigners && sys.indexers.length !== sys.core.manifest.multipleSigners.signers.length) {
            key = this.base._viewStore.deriveKey(this.name, this.base.linearizer.indexers.slice(0, sys.core.manifest.multipleSigners.signers.length))
          }

          await this._ensureCore(key, length)
          break
        }
      }
    }

    if (!this.core) {
      this.core = this.originalCore.batch({ autoClose: false, session: true, checkout: 0 })
      await this.core.ready()
    }

    await this._ensureUserData(this.originalCore)

    this._updateCoreState()

    for (const snap of this._pendingSnapshots) snap.update()
  }

  async _ensureUserData (core) {
    await core.setUserData('referrer', this.base.key)
    await core.setUserData('autobase/view', b4a.from(this.name))
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

  async seek (bytes, opts) {
    const core = this.core

    while (true) {
      try {
        return await core.seek(bytes, opts)
      } catch (err) {
        if (err.code === 'REQUEST_CANCELLED' || this.base.closing || core === this.core) throw err
        // retry
      }
    }
  }

  async get (index, opts) {
    const core = this.core

    while (true) {
      try {
        return await core.get(index, opts)
      } catch (err) {
        if (err.code === 'REQUEST_CANCELLED' || this.base.closing || core === this.core) throw err
        // retry
      }
    }
  }

  async setUserData (name, val, opts) {
    const core = this.core

    while (true) {
      try {
        return await core.setUserData(name, val, opts)
      } catch (err) {
        if (this.base.closing || core === this.core) throw err
        // retry
      }
    }
  }

  async getUserData (name) {
    const core = this.core

    while (true) {
      try {
        return await core.getUserData(name)
      } catch (err) {
        if (this.base.closing || core === this.core) throw err
        // retry
      }
    }
  }

  truncate (newLength) {
    this._truncateAndDetach(newLength)
    this.fork++

    for (const session of this.sessions) {
      if (session.snapshotted === false) session.emit('truncate', newLength, this.fork)
    }
  }

  checkpoint () {
    return {
      checkpointer: this.indexing === 0 ? this.checkpointer : 0,
      checkpoint: (this.checkpointer && this.indexing === 0) ? null : this._checkpoint()
    }
  }

  _checkpoint () {
    if (!this._lastCheckpoint || this._lastCheckpoint.length < this.core.length + this.indexing) {
      this._updateCheckpoint()
    }

    return this._lastCheckpoint
  }

  _updateCheckpoint () {
    const blocks = this.indexBatch(0, this.indexing)
    const batch = this.core.createTreeBatch(null, blocks)
    const namespace = this.base.localWriter.deriveNamespace(this.name)
    const signable = batch.signable(namespace)
    const signature = this.base.local.core.crypto.sign(signable, this.base.local.keyPair.secretKey)

    // todo: signer should sign
    this._lastCheckpoint = {
      signature,
      length: batch.length
    }
  }

  isKeyOwner (key) {
    return b4a.equals(key, this.originalCore.key) || this.base._viewStore.getByKey(key) === this
  }

  update (opts) {
    if (this._isSystem()) return
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

    this.checkpointer = 0
  }

  // called by autobase
  _onundo (removed) {
    if (!removed) return
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
    this.base._onviewappend(this, batch.length)
    this._emitAppend()
  }

  _emitAppend () {
    for (const session of this.sessions) {
      if (session.snapshotted === false) session.emit('append')
    }
  }

  _shouldFlush () {
    if (this.indexing > 0) return true
    if (this.queued >= 0) return true
    if (this.core.session.opened && this.core.indexedLength === this.core.length) return false
    return this.signer.getSignableLength() > this.signedLength
  }

  async flush () {
    if (!this._shouldFlush()) return true

    const migrated = await this._tryMigrateCore()

    if (this.indexing) {
      const batch = this.indexBatch(0, this.indexing)
      await this.core.append(batch)
    }

    if (!migrated) return false

    return this._flush()
  }

  async _flush (length = this.flushableLength) {
    if (length <= this.signedLength) return !!(length && length === this.signedLength)

    const signature = await this.signer.sign(this.indexers, length)
    const flushed = await this.core.flush({ length, signature })
    this.signedLength = length

    return flushed
  }

  async _tryMigrateCore () {
    if (!this.opened) await this.ready()

    if (this.queued === -1) return true

    const length = this.queued
    const signableLength = this.signer.getSignableLength()

    // only migrate when we overlap
    if (this.indexers.length > 0 && signableLength < length) return false

    // check we can sign the clone
    const indexers = this.base.linearizer.indexers.slice(0)

    if (this.signer.getSignableLength(indexers) < length) return false

    for (const idx of indexers) {
      if (!idx.core.manifest) return false // only triggered for bootstrap
    }

    // we can gc the queue now
    this.queued = -1

    if (sameIndexers(indexers, this.indexers)) return true

    const core = await this.base._viewStore.getCore(this, indexers, this.opts)
    await core.ready()

    // clone state from previous core
    await this._migrateCurrentCore(core, indexers, length)

    // todo: should core always be flushed to length?
    const checkout = this.core.indexedLength < length ? this.core.indexedLength : length
    const batch = core.batch({ autoClose: false, session: true, checkout })
    await batch.ready()

    // handle remaining state
    if (this.core) {
      if (batch.length < this.core.length) await batch.append(this.core._appends)
    } else {
      await this._ensureUserData(core)
    }

    this._indexers = indexers.length

    const old = this.core
    const oldOriginal = this.originalCore

    this.originalCore = core
    this.core = batch
    this.signedLength = length

    await old.close()
    await oldOriginal.close()

    // force clear anyone that is applying as we dont have guarantees about the consistency atm
    this.base.queueFastForward()

    return true
  }

  async _migrateCurrentCore (next, indexers, length) {
    // flush old core for posterity
    if (this.indexers.length) await this._flush(length)

    // can't sign zero length core, set userData as it won't be copied
    if (length === 0) return this._ensureUserData(next)

    const signature = await this.signer.sign(indexers, length)

    // copy state over
    await next.core.copyFrom(this.core.session.core, signature, {
      length: this.core.indexedLength
    })
  }

  async catchup ({ key, length }) {
    if (!this.opened) await this.ready()
    await this._ensureCore(key, length)
  }

  _gc () {
    if (this._shifted === 0) return
    this.nodes = this.nodes.slice(this._shifted)
    this._shifted = 0
  }

  _truncateAndDetach (sharedLength) {
    assert(this.indexedLength <= sharedLength && sharedLength <= this.length, 'Invalid truncation')

    // if same len, nothing to do...
    if (sharedLength === this.length) return

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
      const end = snap.length - this.indexedLength - this._shifted
      if (snap.length > sharedLength) snap.detach(this.nodes, end)
    }

    const firstRemovedIndex = sharedLength - this.indexedLength
    const firstRemoved = this.nodes[firstRemovedIndex]

    this.nodes = this.nodes.slice(0, firstRemovedIndex)
    this.length = sharedLength
    this.byteLength = firstRemoved.byteOffset
  }
}

function sameIndexers (a, b) {
  if (!a) return !b
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (!b4a.equals(a[i].core.key, b[i].core.key)) return false
  }
  return true
}
