const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const c = require('compact-encoding')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const assert = require('nanoassert')
const b4a = require('b4a')
const WakeupExtension = require('./extension')
const Signer = require('./signer')
const { ViewRecord } = require('./messages')

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
    return this.name === '_system'
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

    const core = this.originalCore.session({ name: 'batch' })
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

  async _preload (opts) {
    await this.ready()

    const parent = opts.indexed ? this.originalCore : this.core
    return { ...opts, parent }
  }

  createSession (valueEncoding, indexed) {
    if (this.opened === false) return new Hypercore({ preload: this._preload({ valueEncoding, indexed }) })

    if (indexed) return this.originalCore.session({ valueEncoding })
    return this.core.session({ valueEncoding })
  }

  createSnapshot (valueEncoding) {
    return this.core.snapshot({ valueEncoding })
  }

  createWritable (valueEncoding, checkout) {
    // BIG HACK: need to have proper way to express pending system index
    if (checkout === 0) this.base._viewStore._unindex(this)

    return this.core.session({
      name: 'writable',
      valueEncoding,
      draft: true,
      checkout,
      overwrite: true,
      parent: this.core
    })
  }

  async flushWriteBatch (batch, treeLength) {
    const reorg = treeLength < this.core.length

    if (reorg) this._detachSnapshots(treeLength)
    if (reorg && batch.length === 0) this.base._viewStore._unindex(this)

    await this.core.state.overwrite(batch.state, { treeLength })

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

  async checkpoint (length) {
    const same = this._lastCheckpoint && length <= this._lastCheckpoint.length
    return {
      checkpointer: same ? this.checkpointer : 0,
      checkpoint: (this.checkpointer && same) ? null : await this._checkpoint(length)
    }
  }

  async _checkpoint (length) {
    if (!this._lastCheckpoint || this._lastCheckpoint.length < length) {
      await this._updateCheckpoint(length)
    }

    return this._lastCheckpoint
  }

  async _updateCheckpoint (length) {
    const batch = await this.core.restoreBatch(length, { clone: false })

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
  _onindex (length) {
    this.indexedLength = length
    this.checkpointer = 0
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

  _shouldFlush (length) {
    if (this.core.opened && length === this.core.flushedLength) return false
    return true
  }

  async flush (length) {
    if (!this.core.opened) await this.core.ready()

    if (!this.isBootstrapped() && !(await this.bootstrap())) {
      return false
    }

    if (!this._shouldFlush(length)) return true

    return this._flush(length)
  }

  async _flush (length) {
    if (this.core.flushedLength >= length) return true

    const indexers = this.base._viewStore._indexers
    const signableLength = await this.signer.getSignableLength(indexers, length)

    // current core can only flush up to first pending migrate
    if (signableLength <= this.core.flushedLength) return false

    const signature = await this.signer.sign(indexers, signableLength, length)
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
    await this._migrateCurrentCore(core, length, indexers)

    const closing = this.originalCore.close()
    this.originalCore = core

    await closing

    if (this._isSystem()) this._registerSystemCore()

    // for (const session of this.sessions) {
    //   if (!session.snapshotted) session.emit('migrate')
    // }
  }

  async _migrateCurrentCore (next, length) {
    if (length > 0 && this.core.core !== next.core) {
      await next.core.copyPrologue(this.core.state)
    }

    // force update as we might have copied to old migrated pointer userdata
    this.migrated = this.core.key

    await this.core.truncate(length)
    await this.core.state.moveTo(next.core)

    await this._ensureUserData(next, true)
    await this._ensureUserData(this.core, true)

    return this.core
  }

  async catchup ({ key, length, systemIndex }) {
    if (!this.opened) await this.ready()

    let swap = null

    if (!b4a.equals(this.originalCore.key, key)) {
      const encryptionKey = this.base._viewStore.getBlockKey(this.name)
      swap = this.base.store.get({ key, encryptionKey, isBlockKey: true, cache: !!this.originalCore.cache })
      await swap.ready()
    }

    const prevOriginalCore = this.originalCore
    this.originalCore = swap || prevOriginalCore

    const prevLength = this.core.length
    const treeLength = this.core.flushedLength

    await this.core.state.overwrite(this.originalCore.state, { length, treeLength })

    if (swap) {
      // set handlers if we replaced core
      if (this._isSystem()) this._registerSystemCore()

      await this.core.state.moveTo(swap.core)
      await prevOriginalCore.close()
    }

    this._updateCoreState(this.core.flushedLength)

    await this._ensureUserData(this.originalCore, false)

    if (!swap && treeLength < prevLength) this._emitTruncate(treeLength)
    if (prevLength < length) this._emitAppend()

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

// function autocoreOnWait (fn, index, core) {
//   if (this.closing) throw REQUEST_CANCELLED()
//   if (fn) return fn(index, this)
// }
