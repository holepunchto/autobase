const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const c = require('compact-encoding')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const Signer = require('./signer')
const { ViewRecord } = require('./messages')

module.exports = class Autocore extends ReadyResource {
  constructor (base, core, name, opts = {}) {
    super()

    // set in _open
    this.indexedLength = 0
    this.indexedByteLength = 0

    this.base = base
    this.name = name
    this.originalCore = core
    this.core = null
    this.wakeupExtension = null
    this.migrated = null
    this.atom = null

    this.opts = opts

    // managed by system
    this.systemIndex = -1
    this.checkpointer = 0

    this.signer = new Signer(base, this)

    this._lingering = []
    this._gc = new Set()

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

  get signedLength () {
    return this.core.signedLength
  }

  _registerSystemCore () {
    this._registerFastForwardListener()
  }

  _registerFastForwardListener () {
    this.originalCore.removeListener('append', this._queueFastForward)
    this.originalCore.on('append', this._queueFastForward)
  }

  _createCoreSession (length) {
    if (!this.originalCore) throw new Error('No core is present')

    return this.originalCore.session({
      name: 'batch',
      writable: false,
      checkout: length === -1 ? undefined : length
    })
  }

  async _ensureCore (key, length) {
    let swap = null

    if (!b4a.equals(this.originalCore.key, key)) {
      const encryption = this.base._viewStore.getBlockEncryption(this.name)
      swap = this.base.store.get({ key, encryption, cache: !!this.originalCore.cache })
      await swap.ready()
    }

    const prevOriginalCore = this.originalCore
    this.originalCore = swap || this.originalCore

    // set handlers if we replaced core
    if (this._isSystem()) this._registerSystemCore()

    const core = this._createCoreSession(-1)
    await core.ready()

    // reset state
    if (this.core) await this._updateBatch(core, length)
    else this.core = core

    this._updateCoreState(this.core.signedLength)

    if (swap) {
      this._lingering.push(prevOriginalCore)
    }

    await this._ensureUserData(this.originalCore, false, null)
    await this._ensureUserData(this.core, false, null)
  }

  async reset (length) {
    let core = this._createCoreSession(length)
    await core.ready()
    if (core.length > length) {
      await core.close()
      core = this._createCoreSession(length)
      await core.ready()
    }

    await this._updateBatch(core, length)
  }

  async _updateBatch (core, length) {
    const prevCore = this.core

    this.core = core
    this._updateCoreState(length)

    this._lingering.push(prevCore)
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
      const encryption = this.base._viewStore.getBlockEncryption(this.name)
      await this.originalCore.setEncryptionKey(encryption.key, { isBlockKey: true })
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
      this.core = this._createCoreSession(-1)
      await this.core.ready()
      this._updateCoreState(this.originalCore.length)
      await this._ensureUserData(this.core, false, null)
    }

    await this._ensureUserData(this.originalCore, false, null)

    if (this.signer !== null) this.signer.open()
  }

  _close () {
    const promises = [
      ...this._lingering.map(c => c.close()),
      this.core.close(),
      this.originalCore.close()
    ]

    for (const c of this._gc) promises.push(c.close())

    return Promise.all(promises)
  }

  async _ensureUserData (core, force, atom) {
    await core.setUserData('referrer', this.base.key)

    const buf = await core.getUserData('autobase/view')
    const record = (buf !== null && buf[0] === 0) ? c.decode(ViewRecord, buf) : { name: null, migrated: null, audits: 1 }

    // if (record.audits === 0) {
    //   // old core, audit it real quick
    //   const corrections = await core.core.audit()
    //   if (corrections.blocks || corrections.tree) debugWarn('view', core.id, 'auto corrected itself', corrections)
    // }

    const prev = (!force && record.name) ? record : null
    if (prev && !this.migrated) this.migrated = prev.migrated

    return core.setUserData('autobase/view', c.encode(ViewRecord, { name: this.name, migrated: prev ? prev.migrated : this.migrated, audits: 1 }), { atom })
  }

  async _preload () {
    await this.ready()
    return { parent: this.core }
  }

  createSession (valueEncoding) {
    if (this.opened === false) {
      return new Hypercore({ valueEncoding, writable: false, preload: this._preload() })
    }

    return this.core.session({ valueEncoding, writable: false })
  }

  createSnapshot (valueEncoding) {
    return this.core.snapshot({ valueEncoding })
  }

  createWritable (valueEncoding, atom) {
    return this.core.session({
      valueEncoding,
      atom,
      writable: true,
      parent: this.core
    })
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
    if (newLength === 0) this.base._viewStore._unindex(this)
    return this.core.truncate(newLength)
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
    const batch = await this.core.restoreBatch(length)
    // always against the main fork
    batch.fork = this.core.core.state.fork

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
    if (this.core.opened && length === this.core.signedLength) return false
    return true
  }

  async flush (length, atom) {
    if (!this.core.opened) await this.core.ready()

    if (!this.isBootstrapped() && !(await this.bootstrap())) return false

    if (!this._shouldFlush(length)) return true

    return this._flush(length, atom)
  }

  async _flush (length, atom) {
    if (this.core.signedLength >= length) return true

    const indexers = this.base._viewStore._indexers
    const signableLength = await this.signer.getSignableLength(indexers, length)

    // current core can only flush up to first pending migrate
    if (signableLength <= this.core.signedLength) return false

    const signature = await this.signer.sign(indexers, signableLength, length)

    const atomic = this.originalCore.session({ atom })
    this._gc.add(atomic)

    atom.onflush(() => {
      this._gc.delete(atomic)
      return atomic.close()
    })

    return atomic.commit(this.core, { length: signableLength, signature, atom })
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

  async migrateTo (indexers, length, atom) {
    if (!this.opened) await this.ready()

    const core = await this.base._viewStore.getCore(this, indexers, length, this.opts)
    await core.ready()

    // clone state from previous core
    if (length > 0 && this.core.core !== core.core) {
      await core.core.copyPrologue(this.core.state)
    }

    // force update as we might have copied to old migrated pointer userdata
    this.migrated = this.core.key

    const src = this.core.session({ atom })
    const dst = core.session({ atom })

    await src.ready()
    await dst.ready()

    if (atom) {
      this._gc.add(src)
      this._gc.add(dst)

      atom.onflush(() => src.close())
      atom.onflush(() => dst.close())
    }

    await src.state.moveTo(core.core, length)

    await this._ensureUserData(src, true)
    await this._ensureUserData(dst, true)

    if (!atom) {
      await src.close()
      await dst.close()
    }

    this._lingering.push(this.originalCore)
    this.originalCore = core

    if (this._isSystem()) this._registerSystemCore()
  }

  async catchup ({ key, length, treeLength, systemIndex }, atom) {
    if (!this.opened) await this.ready()

    let swap = null

    if (!b4a.equals(this.originalCore.key, key)) {
      const encryption = this.base._viewStore.getBlockEncryption(this.name)
      swap = this.base.store.get({ key, encryption, cache: !!this.originalCore.cache })
      await swap.ready()
    }

    const prevOriginalCore = this.originalCore
    this.originalCore = swap || prevOriginalCore

    const core = this.core.session({ atom })
    await core.ready()

    await core.state.overwrite(this.originalCore.state, { length, treeLength, shallow: true })

    if (swap) await core.state.moveTo(swap.core, length, atom)

    atom.onflush(() => {
      core.close()

      if (swap) {
        // set handlers if we replaced core
        if (this._isSystem()) this._registerSystemCore()
        this._lingering.push(prevOriginalCore)
      }

      this._updateCoreState(this.core.signedLength)

      if (systemIndex !== -1) this.systemIndex = systemIndex
    })
  }
}
