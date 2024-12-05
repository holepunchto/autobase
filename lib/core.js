const { EventEmitter } = require('events')
const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const assert = require('nanoassert')
const b4a = require('b4a')
const WakeupExtension = require('./extension')
const Signer = require('./signer')
const { ViewRecord } = require('./messages')

const {
  SESSION_CLOSED,
  SESSION_NOT_WRITABLE,
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
    this._snapshot = null

    if (update) this.update()
  }

  async ready () {
    await this.source.ready()
    if (this._snapshot) await this._snapshot.ready()
  }

  clone () {
    const snap = new Snapshot(this.source, false)
    snap.fork = this.fork
    snap.length = this.length
    snap.byteLength = this.byteLength

    // detached clones should be detached
    if (this.index === -1) {
      const sharedLength = this.length - this.tipLength
      snap.detach(this._snapshot, sharedLength, !this._snapshot)
      if (this._snapshot) {
        snap._snapshot = this._snapshot.session()
        snap._snapshot.snapshot = this
      }
    }

    return snap
  }

  get (index, opts) {
    if (index >= this.length) throw BLOCK_NOT_AVAILABLE()
    return this._snapshot !== null ? this._snapshot.get(index, opts) : this.source.get(index, opts)
  }

  seek (byteOffset, opts) {
    if (byteOffset >= this.byteLength) return null
    return this._snapshot !== null ? this._snapshot.seek(byteOffset, opts) : this.source.seek(byteOffset, opts)
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

  async update () {
    if (this.index === -1) { // reattach
      this.index = this.source._pendingSnapshots.push(this) - 1
      this.tipLength = 0
      if (this._snapshot) await this._snapshot.close()
    }
    this.fork = this.source.fork
    this.length = this.source.length
    this.byteLength = this.source.byteLength
  }

  close () {
    this._gc()
    if (this._snapshot) return this._snapshot.close()
  }

  detach (core, length, session) {
    if (session) this._snapshot = core.snapshot()
    this.tipLength = this.length - length
    this._gc()
  }

  _gc () {
    if (this.index === -1) return
    const top = this.source._pendingSnapshots.pop()
    if (top !== this) this.source._pendingSnapshots[top.index = this.index] = top
    this.index = -1
  }
}

class AutocoreSession extends EventEmitter {
  constructor (source, core, snapshot, indexed, valueEncoding) {
    super()

    this.isAutobase = true

    this.closing = false
    this.closed = source.closed
    this.opened = false

    this.indexed = !snapshot && indexed === true

    this.writable = core !== null

    this.activeRequests = []
    this.valueEncoding = valueEncoding || null
    this.globalCache = source.base.globalCache

    this._core = core
    this._sourceLength = core ? core.length : -1

    this._source = source
    this._index = source.sessions.push(this) - 1
    this._snapshot = snapshot

    this.ready().catch(safetyCatch)
  }

  get base () {
    return this._source.base
  }

  get name () {
    return this._source.name
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
    return this._core !== null ? this._core.fork : this._snapshot === null ? this._source.fork : this._snapshot.fork
  }

  get byteLength () {
    return this._core !== null ? this._core.byteLength : this._snapshot === null ? this._source.core ? this._source.core.byteLength : 0 : this._snapshot.byteLength
  }

  get length () {
    return this._core !== null ? this._core.length : this._snapshot === null ? this.indexed ? this.indexedLength : this._source.length : this._snapshot.length
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

  get manifest () {
    return this._source.core ? this._source.core.manifest : null
  }

  getBackingCore () {
    return this._core ? this._core : this._source.core ? this._source.core : null
  }

  async ready () {
    if (this.opened) return
    await this._source.ready()
    if (this._core) await this._core.ready()
    if (this._snapshot) await this._snapshot.ready()
    if (this.opened) return
    this.opened = true
    this.emit('ready')
  }

  async getPrologue (length) {
    if (!length) return null

    const batch = await this.getBackingCore().restoreBatch(length)

    return { hash: batch.hash(), length }
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

    if (this._core) return this._core.snapshot({ valueEncoding })

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

    if (byteOffset >= this.byteLength) return null

    if (this._snapshot !== null) {
      return this._snapshot.seek(byteOffset)
    }

    return this._source.seek(byteOffset, { activeRequests: this.activeRequests, ...opts })
  }

  async get (index, opts) {
    if (this.opened === false) await this.ready()
    if (this.closing === true) throw SESSION_CLOSED()

    const onwait = autocoreOnWait.bind(this, opts && opts.onwait)

    opts = { activeRequests: this.activeRequests, valueEncoding: this.valueEncoding, ...opts, onwait }

    // TODO: this should wait for the block to arrive if not a snap, per hypercore semantics...
    if (index >= this.length) {
      throw BLOCK_NOT_AVAILABLE()
    }

    if (this._core) return this._core.get(index, opts)

    return this._snapshot !== null ? this._snapshot.get(index, opts) : this._source.get(index, opts)
  }

  async truncate (newLength) {
    if (this.opened === false) await this.ready()

    throw new Error('Truncating an Autobased index explicitly is not currently supported')
  }

  async append (block) {
    if (this.opened === false) await this.ready()
    if (!this.writable) throw SESSION_NOT_WRITABLE()
    if (this.closing === true) throw SESSION_CLOSED()

    const blocks = Array.isArray(block) ? block : [block]
    const buffers = new Array(blocks.length)

    for (let i = 0; i < blocks.length; i++) {
      const blk = blocks[i]

      if (b4a.isBuffer(blk)) buffers[i] = blk
      else if (this.valueEncoding) buffers[i] = c.encode(this.valueEncoding, blk)
      else buffers[i] = b4a.from(blk)
    }

    this.base._onviewappend(this._source, blocks.length)

    await this._core.append(buffers)
  }

  async close () {
    this.closing = true

    if (this._core !== null) this._core.close()
    if (this.opened === false) await this.ready()

    if (this.closed) return
    this.closed = true

    if (this._snapshot !== null) this._snapshot.close()

    const top = this._source.sessions.pop()
    if (top !== this) this._source.sessions[top._index = this._index] = top

    const core = this.getBackingCore()
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

    this._batchCounter = 0

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

  get length () {
    return this.core ? this.core.length : 0
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

  get signedLength () {
    return this.core.flushedLength
  }

  _registerSystemCore () {
    this.base.wakeupExtension = this._registerWakeupExtension()
    this._registerFastForwardListener()
  }

  _registerWakeupExtension () {
    if (this.wakeupExtension && this.wakeupExtension.core === this.originalCore) return // no need to reset
    return new WakeupExtension(this.base, this.originalCore)
  }

  _registerFastForwardListener () {
    this.originalCore.removeListener('append', this._queueFastForward)
    this.originalCore.on('append', this._queueFastForward)
  }

  async _ensureCore (key, length, overwrite = true) {
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

    // never truncate unless we are overwriteing
    const checkout = overwrite ? length : undefined

    const core = this.originalCore.session({ name: 'batch', checkout, overwrite })
    await core.ready()

    // reset state
    if (this.core) await this._updateBatch(core, length)
    else this.core = core

    this._updateCoreState(this.core.flushedLength)

    if (swap) await prevOriginalCore.close()

    await this._ensureUserData(this.originalCore, false)
    await this._ensureUserData(this.core, false)
  }

  async reset (length) {
    let core = this.originalCore.session({ name: 'batch', checkout: length })
    await core.ready()
    if (core.length > length) {
      await core.close()
      core = this.originalCore.session({ name: 'batch', checkout: length })
      await core.ready()
    }

    await this._updateBatch(core, length)
  }

  async _updateBatch (core, length) {
    const prevCore = this.core

    // detach all snaps
    for (let i = this._pendingSnapshots.length - 1; i >= 0; i--) {
      const snap = this._pendingSnapshots[i]
      snap.detach(this.core, this.indexedLength, true)
    }

    const currentLength = this.core.length

    this.core = core
    this.nodes = []
    this._shifted = 0

    this.indexing = 0
    this.appending = 0
    this.truncating = 0

    this._updateCoreState(length)

    if (length > currentLength) this._emitAppend()

    await prevCore.close()
  }

  _updateCoreState (length) {
    this.indexedLength = length
    // this.indexedByteLength = this.core.byteLength
  }

  async _open () {
    await Promise.resolve() // wait a tick so this doesn't run sync in the constructor...
    await this.base._presystem

    const sys = this.base._initialSystem

    await this.originalCore.ready()

    if (this.base.encryptionKey && !this.originalCore.encryption) {
      await this.originalCore.setEncryptionKey(this.base._viewStore.getBlockKey(this.name), { isBlockKey: true })
    }

    for (let i = 0; i < this.base._initialViews.length; i++) {
      if (this.base._initialViews[i] !== this.name) continue
      this.systemIndex = i
      break
    }

    if (this.systemIndex === -1 && !this._isSystem() && sys) {
      for (let i = 0; i < sys.views.length; i++) {
        if (!b4a.equals(this.key, sys.views[i].key)) continue
        this.systemIndex = i
        break
      }
    }

    if (this.systemIndex !== -1) {
      const { key, length } = sys.views[this.systemIndex]
      await this._ensureCore(key, length, false)
    }

    // register handlers if needed
    if (this._isSystem()) {
      if (sys) await this._ensureCore(sys.core.key, sys.core.length, false)
      this._registerSystemCore()
    }

    if (!this.core) {
      this.core = this.originalCore.session({ name: 'batch' })
      await this.core.ready()
      this._updateCoreState(this.originalCore.length)
      await this._ensureUserData(this.core, false)
    }

    await this._ensureUserData(this.originalCore, false)

    const openSnapshots = []
    for (const snap of this._pendingSnapshots) openSnapshots.push(snap.update())
    await Promise.all(openSnapshots)

    if (this.signer !== null) this.signer.open()
  }

  _close () {
    return Promise.all([
      this.core.close(),
      this.originalCore.close()
    ])
  }

  async _ensureUserData (core, force) {
    await core.setUserData('referrer', this.base.key)

    const buf = await core.getUserData('autobase/view')
    const record = (buf !== null && buf[0] === 0) ? c.decode(ViewRecord, buf) : { name: null, migrated: null, audits: 0 }

    // if (record.audits === 0) {
    //   // old core, audit it real quick
    //   const corrections = await core.core.audit()
    //   if (corrections.blocks || corrections.tree) debugWarn('view', core.id, 'auto corrected itself', corrections)
    // }

    const prev = (!force && record.name) ? record : null
    if (prev && !this.migrated) this.migrated = prev.migrated

    await core.setUserData('autobase/view', c.encode(ViewRecord, { name: this.name, migrated: prev ? prev.migrated : this.migrated, audits: 1 }))
  }

  createSession (valueEncoding, indexed) {
    return this._createSession(null, valueEncoding, { indexed, writable: false })
  }

  createSnapshot (valueEncoding) {
    return this._createSession(new Snapshot(this, this.opened), valueEncoding, { indexed: false, writable: false })
  }

  createWritable (valueEncoding, checkout) {
    // BIG HACK: need to have proper way to express pending system index
    if (checkout === 0) this.base._viewStore._unindex(this)

    return this._createSession(null, valueEncoding, { indexed: false, writable: true, checkout })
  }

  _createSession (snapshot, valueEncoding, { indexed, writable, checkout } = {}) {
    if (writable) {
      const core = this.core.session({
        name: 'writable',
        draft: true,
        checkout,
        overwrite: true,
        parent: this.core
      })

      return new AutocoreSession(this, core, null, false, valueEncoding ? c.from(valueEncoding) : null)
    }

    return new AutocoreSession(this, null, snapshot, indexed, valueEncoding ? c.from(valueEncoding) : null)
  }

  async flushWriteBatch (batch, treeLength) {
    const reorg = treeLength < this.core.length

    if (reorg) this._detachSnapshots(treeLength)
    if (reorg && batch.length === 0) this.base._viewStore._unindex(this)

    await this.core.state.overwrite(batch._core.state, { treeLength })

    if (reorg) this._emitTruncate(treeLength)
    this._emitAppend()
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

  async truncate (newLength) {
    this.fork++

    if (newLength === 0) this.base._viewStore._unindex(this)
    await this._detachSnapshots(newLength, newLength)
    this._emitTruncate(newLength, this.fork)
    return this.core.truncate(newLength, this.core.fork)
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
    const batch = await this.core.restoreBatch(this.pendingIndexedLength, { clone: false })

    const signable = batch.signable(this.core.key)
    const signature = crypto.sign(signable, this.base.local.keyPair.secretKey)

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

  // called by autobase
  _onindex (added) {
    // const head = this.nodes[this._shifted + added - 1]

    this.indexedLength += added

    // this.indexedByteLength = head.byteOffset + head.block.byteLength

    this._shifted += added
    if (this._shifted >= BULK_SHIFT || this._shifted === this.nodes.length) this._gc()

    this.checkpointer = 0

    this._emitIndexedAppend()
  }

  // called by autobase
  async _onundo (removed) {
    if (!removed) return
    const newLength = this.length - removed
    await this.truncate(newLength)
  }

  async _append (batch) {
    await this.core.append(batch)
    this.base._onviewappend(this, batch.length)
    this._emitAppend()
  }

  _emitAppend () {
    for (const session of this.sessions) {
      if (session.snapshotted === false && session.indexed === false) session.emit('append')
    }
  }

  _emitTruncate (length, fork) {
    for (const session of this.sessions) {
      if (session.snapshotted === false && session.indexed === false) session.emit('truncate', length, fork)
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
    if (this.core.opened && this.pendingIndexedLength === this.core.flushedLength) return false
    return true
  }

  async flush () {
    if (!this.core.opened) await this.core.ready()

    if (!this.isBootstrapped() && !(await this.bootstrap())) {
      return false
    }

    if (!this._shouldFlush()) return true

    return this._flush()
  }

  async _flush () {
    if (this.core.flushedLength >= this.pendingIndexedLength) return true

    const indexers = this.base._viewStore._indexers
    const signableLength = await this.signer.getSignableLength(indexers)

    // current core can only flush up to first pending migrate
    if (signableLength <= this.core.flushedLength) return false

    const signature = await this.signer.sign(indexers, signableLength)
    return this.originalCore.core.commit(this.core.state, { length: signableLength, signature })
  }

  async getPrologue (length) {
    if (!length) return null

    const batch = await this.core.restoreBatch(length)

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
    if (length > 0 && this.core.session.core !== next.core) {
      await next.core.copyPrologue(this.core.state)
    }

    // force update as we might have copied to old migrated pointer userdata
    this.migrated = this.core.key

    // todo: should core always be flushed to length?
    const batch = next.session({ name: 'batch', checkout: length })
    await batch.ready()

    await this._ensureUserData(next, true)
    await this._ensureUserData(batch, true)

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
    await this._ensureCore(key, length, true)
    if (systemIndex !== -1) this.systemIndex = systemIndex
  }

  _gc () {
    if (this._shifted === 0) return
    this.nodes = this.nodes.slice(this._shifted)
    this._shifted = 0
  }

  _detachSnapshots (sharedLength) {
    assert(this.indexedLength <= sharedLength && sharedLength <= this.length, 'Invalid truncation')

    // if same len, nothing to do...
    if (sharedLength === this.length) return

    let maxSnap = 0
    for (const snap of this._pendingSnapshots) {
      if (snap.length > sharedLength && maxSnap < snap.length) {
        maxSnap = snap.length
      }
    }

    for (let i = this._pendingSnapshots.length - 1; i >= 0; i--) {
      const snap = this._pendingSnapshots[i]
      if (snap.length > sharedLength) snap.detach(this.core, sharedLength, true)
    }
  }
}

// function debugWarn (...msg) { // calls to this are ONLY allowed for soft assertions
//   console.log('[autobase]', ...msg)
// }

function autocoreOnWait (fn, index, core) {
  if (this.closing) throw REQUEST_CANCELLED()
  if (fn) return fn(index, this)
}
