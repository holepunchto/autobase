const { EventEmitter } = require('events')
const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const c = require('compact-encoding')
const assert = require('nanoassert')
const b4a = require('b4a')
const WakeupExtension = require('./extension')
const Signer = require('./signer')
const { ViewRecord } = require('./messages')

const {
  SESSION_CLOSED,
  REQUEST_CANCELLED,
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
    return Math.min(this.source.core.indexedLength, sharedLength)
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
  constructor (source, snapshot, indexed, valueEncoding) {
    super()

    this.isAutobase = true

    this.closing = false
    this.closed = source.closed
    this.opened = false

    this.indexed = !snapshot && indexed === true

    this.writable = true // TODO: make this configurable

    this.activeRequests = []
    this.valueEncoding = valueEncoding || null
    this.globalCache = source.base.globalCache

    this._source = source
    this._index = source.sessions.push(this) - 1
    this._snapshot = snapshot

    this.ready().catch(safetyCatch)
  }

  get base () {
    return this._source.base
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
    return this._snapshot === null ? this.indexed ? this.indexedByteLength : this._source.byteLength : this._snapshot.byteLength
  }

  get length () {
    return this._snapshot === null ? this.indexed ? this.indexedLength : this._source.length : this._snapshot.length
  }

  get indexedByteLength () {
    return this._snapshot === null ? this._source.indexedByteLength : this._snapshot.getIndexedByteLength()
  }

  get indexedLength () {
    return this._snapshot === null ? this._source.indexedLength : this._snapshot.getIndexedLength()
  }

  get signedLength () {
    return this._snapshot === null ? this._source.core.indexedLength : this._snapshot.getSignedLength()
  }

  get manifest () {
    return this._source.core ? this._source.core.manifest : null
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

  session ({ valueEncoding = this.valueEncoding, snapshot = this._snapshot !== false, indexed = this.indexed } = {}) {
    if (this.closing === true) throw SESSION_CLOSED()

    return snapshot
      ? this.snapshot({ valueEncoding })
      : this._source.createSession(valueEncoding, indexed)
  }

  async update (opts) {
    if (this.opened === false) await this.ready()
    if (this.closing === true) throw SESSION_CLOSED()

    if (opts && opts.wait) {
      await this._source.update(opts)
      if (this.closing === true) throw SESSION_CLOSED()
    }

    if (this._snapshot !== null) this._snapshot.update()
    return true
  }

  async seek (byteOffset, opts) {
    if (this.opened === false) await this.ready()
    if (this.closing === true) throw SESSION_CLOSED()

    if (byteOffset < this.indexedByteLength) {
      return this._source.seek(byteOffset, { activeRequests: this.activeRequests, ...opts })
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

    const onwait = autocoreOnWait.bind(this, opts && opts.onwait)

    opts = { activeRequests: this.activeRequests, valueEncoding: this.valueEncoding, ...opts, onwait }

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

    const core = this.getBackingCore().session
    if (core.replicator) core.replicator.clearRequests(this.activeRequests)

    this.emit('close', false)
  }

  _getNodes () {
    return (this._snapshot === null || this._snapshot.tip === null) ? this._source.nodes : this._snapshot.tip
  }
}

module.exports = class Autocore extends ReadyResource {
  constructor (base, core, name, opts = {}) {
    super()

    // set in _open
    this.indexedLength = 0
    this.indexedByteLength = 0
    this.length = 0
    this.byteLength = 0

    this.fork = 0

    this.base = base
    this.name = name
    this.originalCore = core
    this.core = null
    this.wakeupExtension = null
    this.migrated = null

    this.opts = opts

    // managed by base
    this.appending = 0
    this.truncating = 0
    this.indexing = 0

    // managed by system
    this.systemIndex = -1
    this.checkpointer = 0

    this.sessions = []
    this.nodes = []

    this.signer = new Signer(base, this)

    this._shifted = 0
    this._pendingSnapshots = []
    this._lastCheckpoint = null

    this._queueFastForward = this.base.queueFastForward.bind(this.base)

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

  _registerSystemCore () {
    this._registerWakeupExtension()
    this._registerFastForwardListener()
  }

  _registerWakeupExtension () {
    if (this.wakeupExtension && this.wakeupExtension.core === this.originalCore) return // no need to reset
    this.wakeupExtension = new WakeupExtension(this.base, this.originalCore)
  }

  _registerFastForwardListener () {
    this.originalCore.removeListener('append', this._queueFastForward)
    this.originalCore.on('append', this._queueFastForward)
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

    // set handlers if we replaced core
    if (this._isSystem()) this._registerSystemCore()

    const core = this.originalCore.batch({ autoClose: false, session: true, checkout: length })
    await core.ready()

    // reset state
    if (this.core) await this._updateBatch(core)
    else this.core = core

    if (swap) await prevOriginalCore.close()

    await this._ensureUserData(this.core, false)
  }

  async reset (length) {
    let core = this.originalCore.batch({ clear: true, autoClose: false, session: true })
    await core.ready()
    if (core.length > length) {
      await core.close()
      core = this.originalCore.batch({ checkout: length, autoClose: false, session: true })
      await core.ready()
    }

    await this._updateBatch(core)
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

    if (this.length > length) this._emitAppend()

    await prevCore.close()
  }

  _updateCoreState () {
    this.length = this.core.length
    this.byteLength = this.core.byteLength
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

      for (let i = 0; i < this.base._initialViews.length; i++) {
        const { name, key, length } = this.base._initialViews[i]
        if (name !== this.name) continue

        this.systemIndex = i - 1
        await this._ensureCore(key, length)
        break
      }
    }

    // register handlers if needed
    if (this._isSystem()) this._registerSystemCore()

    if (!this.core) {
      this.core = this.originalCore.batch({ autoClose: false, session: true, checkout: 0 })
      await this.core.ready()
    }

    await this._ensureUserData(this.originalCore, false)

    this._updateCoreState()

    for (const snap of this._pendingSnapshots) snap.update()

    if (this.signer !== null) this.signer.open()
  }

  _close () {
    return this.core.close()
  }

  async _ensureUserData (core, force) {
    await core.setUserData('referrer', this.base.key)

    const buf = await core.getUserData('autobase/view')
    const record = (buf !== null && buf[0] === 0) ? c.decode(ViewRecord, buf) : { name: null, migrated: null, audits: 1 }

    if (record.audits === 0) {
      // old core, audit it real quick
      const corrections = await core.core.audit()
      if (corrections.blocks || corrections.tree) debugWarn('view', core.id, 'auto corrected itself', corrections)
    }

    const prev = (!force && record.name) ? record : null
    if (prev && !this.migrated) this.migrated = prev.migrated

    await core.setUserData('autobase/view', c.encode(ViewRecord, { name: this.name, migrated: prev ? prev.migrated : this.migrated, audits: 1 }))
  }

  createSession (valueEncoding, indexed) {
    return this._createSession(null, valueEncoding, indexed)
  }

  createSnapshot (valueEncoding) {
    return this._createSession(new Snapshot(this, this.opened), valueEncoding)
  }

  _createSession (snapshot, valueEncoding, indexed) {
    return new AutocoreSession(this, snapshot, indexed, valueEncoding ? c.from(valueEncoding) : null)
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

  async checkpoint () {
    return {
      checkpointer: this.indexing === 0 ? this.checkpointer : 0,
      checkpoint: (this.checkpointer && this.indexing === 0) ? null : await this._checkpoint()
    }
  }

  async _checkpoint () {
    if (!this._lastCheckpoint || this._lastCheckpoint.length < this.pendingIndexedLength) {
      await this._updateCheckpoint()
    }

    return this._lastCheckpoint
  }

  async _updateCheckpoint (migrated) {
    const blocks = this.indexBatch(0, this.indexing)
    const batch = await this.core.restoreBatch(this.pendingIndexedLength, { blocks, clone: false })

    const signable = batch.signable(this.core.key)
    const signature = this.base.local.core.crypto.sign(signable, this.base.local.keyPair.secretKey)

    // todo: signer should sign
    this._lastCheckpoint = {
      signature,
      length: batch.length
    }
  }

  update (opts) {
    if (this._isSystem()) return
    return this.base.update(opts)
  }

  indexBatch (start, end) {
    if (start >= end) return []
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

    this._emitIndexedAppend()
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
      if (session.snapshotted === false && session.indexed === false) session.emit('append')
    }
  }

  _emitIndexedAppend () {
    for (const session of this.sessions) {
      if (session.indexed) session.emit('append')
    }
  }

  isBootstrapped () {
    return !!(this.core && this.core.manifest && this.core.manifest.signers.length > 0)
  }

  async bootstrap () {
    if (this.isBootstrapped()) return true

    if (this.base.linearizer.indexers.length === 0) return false

    const [bootstrap] = this.base.linearizer.indexers
    if (!bootstrap.core.manifest) return false

    await this.migrateTo([bootstrap], 0)

    this.base.queueFastForward()
    return true
  }

  _shouldFlush () {
    if (this.indexing > 0) return true
    if (this.core.session.opened && this.core.indexedLength === this.core.length) return false
    return true
  }

  async flush () {
    if (!this.core.opened) await this.core.ready()

    if (!this.isBootstrapped() && !(await this.bootstrap())) {
      return false
    }

    if (!this._shouldFlush()) return true

    if (this.indexing) {
      const batch = this.indexBatch(0, this.indexing)
      await this.core.append(batch)
    }

    return this._flush()
  }

  async _flush () {
    let flushed = this.core.flushedLength === this.core.length

    const maxLength = this.core.length
    if (maxLength === 0) return flushed

    const maxFlushedLength = Math.min(maxLength, this.core.length)

    if (maxFlushedLength > this.core.indexedLength && this.core.flushedLength < this.core.length) {
      flushed = false
    }

    const indexers = this.base._viewStore._indexers
    const signableLength = await this.signer.getSignableLength(indexers)

    // current core can only flush up to first pending migrate
    const maxSignedlength = Math.min(maxLength, signableLength)

    if (maxSignedlength > this.core.indexedLength) {
      const signature = await this.signer.sign(indexers, maxSignedlength)
      if (!(await this.core.flush({ length: maxSignedlength, signature }))) return false
      flushed = true
    }

    return flushed
  }

  async getPrologue (length) {
    if (!length) return null

    const blocks = this.indexBatch(0, length - this.indexedLength)
    const batch = await this.core.restoreBatch(length, { blocks, clone: false })

    return { hash: batch.hash(), length }
  }

  async deriveKey (indexers, length) {
    const prologue = await this.getPrologue(length)
    return this.base.deriveKey(this.name, indexers, prologue)
  }

  async migrateTo (indexers, length) {
    if (!this.opened) await this.ready()

    const core = await this.base._viewStore.getCore(this, indexers, length, this.opts)
    await core.ready()

    // clone state from previous core
    const batch = await this._migrateCurrentCore(core, length, indexers)

    const old = this.core
    const oldOriginal = this.originalCore

    this.originalCore = core
    this.core = batch

    await old.close()
    await oldOriginal.close()

    if (this._isSystem()) this._registerSystemCore()

    for (const session of this.sessions) {
      if (!session.snapshotted) session.emit('migrate')
    }
  }

  async _migrateCurrentCore (next, length, indexers) {
    if (length > 0) {
      const sourceLength = this.core.indexedLength

      // copy state over
      const additional = []
      for (let i = sourceLength; i < length; i++) {
        additional.push(await this.core.get(i, { raw: true }))
      }

      if (this.core.session.core !== next.core) {
        await next.core.copyPrologue(this.core.session.core, { additional })
      }
    }

    // force update as we might have copied to old migrated pointer userdata
    this.migrated = this.core.key
    await this._ensureUserData(next, true)

    // todo: should core always be flushed to length?
    const batch = next.batch({ autoClose: false, session: true, checkout: length })
    await batch.ready()

    // handle remaining state
    if (batch.length < this.core.length) {
      const blocks = []
      while (batch.length + blocks.length < this.core.length) {
        blocks.push(await this.core.get(batch.length + blocks.length))
      }
      await batch.append(blocks)
    }

    return batch
  }

  async catchup ({ key, length, systemIndex }) {
    if (!this.opened) await this.ready()
    await this._ensureCore(key, length)
    if (systemIndex !== -1) this.systemIndex = systemIndex
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
    if (sharedLength === 0) this.base._viewStore._unindex(this)

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

function debugWarn (...msg) { // calls to this are ONLY allowed for soft assertions
  console.log('[autobase]', ...msg)
}

function autocoreOnWait (fn, index, core) {
  if (this.closing) throw REQUEST_CANCELLED()
  if (fn) return fn(index, this)
}
