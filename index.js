const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const hypercoreId = require('hypercore-id-encoding')
const assert = require('nanoassert')
const SignalPromise = require('signal-promise')
const CoreCoupler = require('core-coupler')
const mutexify = require('mutexify/promise')
const ProtomuxWakeup = require('protomux-wakeup')
const rrp = require('resolve-reject-promise')
const crypto = require('hypercore-crypto')
const Hypercore = require('hypercore')

const LocalState = require('./lib/local-state.js')
const Linearizer = require('./lib/linearizer.js')
const SystemView = require('./lib/system.js')
const { EncryptionView } = require('./lib/encryption.js')
const UpdateChanges = require('./lib/updates.js')
const messages = require('./lib/messages.js')
const Timer = require('./lib/timer.js')
const Writer = require('./lib/writer.js')
const decodeValue = require('./lib/decode-value.js')
const ActiveWriters = require('./lib/active-writers.js')
const AutoWakeup = require('./lib/wakeup.js')

const FastForward = require('./lib/fast-forward.js')
const AutoStore = require('./lib/store.js')
const ApplyState = require('./lib/apply-state.js')
const { PublicApplyCalls } = require('./lib/apply-calls.js')
const boot = require('./lib/boot.js')
const { DEFAULT_AUTOBASE_VERSION, MAX_AUTOBASE_VERSION } = require('./lib/caps.js')

const inspect = Symbol.for('nodejs.util.inspect.custom')
const INTERRUPT = new Error('Apply interrupted')
const BINARY_ENCODING = c.from('binary')

const RECOVERIES = 3
const FF_RECOVERY = 1

// default is to automatically ack
const DEFAULT_ACK_INTERVAL = 10_000
const DEFAULT_ACK_THRESHOLD = 4

const REMOTE_ADD_BATCH = 64
const MIN_FF_WAIT = 300_000 // wait at least 5min before attempting to ff again after failure

class WakeupHandler {
  constructor (base, discoveryKey) {
    this.active = true
    this.discoveryKey = discoveryKey
    this.base = base
  }

  onpeeractive (peer, session) {
    this.base._bumpWakeupPeer(peer)
  }

  onlookup (req, peer, session) {
    const wakeup = this.base._getWakeup()
    if (wakeup.length === 0) return
    session.announce(peer, wakeup)
  }

  onannounce (wakeup, peer, session) {
    if (this.base.isFastForwarding()) {
      this.base._needsWakeupRequest = true
      return
    }
    this.base.hintWakeup(wakeup)
  }
}

module.exports = class Autobase extends ReadyResource {
  constructor (store, bootstrap, handlers = {}) {
    if (Array.isArray(bootstrap)) bootstrap = bootstrap[0] // TODO: just a quick compat, lets remove soon

    if (bootstrap && typeof bootstrap !== 'string' && !b4a.isBuffer(bootstrap)) {
      handlers = bootstrap
      bootstrap = null
    }

    super()

    const key = bootstrap ? toKey(bootstrap) : null

    this.id = null
    this.key = key
    this.discoveryKey = null

    this.keyPair = null
    this.valueEncoding = c.from(handlers.valueEncoding || 'binary')
    this.store = store
    this.globalCache = store.globalCache || null
    this.migrated = false

    this.encrypted = handlers.encrypted || !!handlers.encryptionKey
    this.encrypt = !!handlers.encrypt
    this.encryptionKey = handlers.encryptionKey || null
    this.encryption = null

    this.local = null
    this.localWriter = null
    this.isIndexer = false

    this.activeWriters = new ActiveWriters()
    this.linearizer = null
    this.updating = false

    this.nukeTip = !!handlers.nukeTip

    this.wakeupOwner = !handlers.wakeup
    this.wakeupCapability = null
    this.wakeupProtocol = handlers.wakeup || new ProtomuxWakeup()
    this.wakeupSession = null

    this._primaryBootstrap = null

    this.fastForwardEnabled = handlers.fastForward !== false
    this.fastForwarding = null
    this.fastForwardTo = null
    this.fastForwardFailedAt = 0

    this._bootstrapWriters = [] // might contain dups, but thats ok
    this._bootstrapWritersChanged = false

    this._checkWriters = []
    this._optimistic = -1
    this._appended = 0
    this._appending = null
    this._wakeup = new AutoWakeup(this)
    this._wakeupHints = new Map()
    this._wakeupPeerBound = this._wakeupPeer.bind(this)
    this._coupler = null

    this._updateLocalCore = null
    this._lock = mutexify()

    this._needsWakeupRequest = false
    this._needsWakeup = true
    this._needsWakeupHeads = true

    this._updates = []
    this._handlers = handlers || {}
    this._warn = emitWarning.bind(this)

    this._draining = false
    this._writable = null
    this._advancing = null
    this._interrupting = false
    this._caughtup = false

    this.paused = false

    this._bump = debounceify(() => {
      this._advancing = this._advance()
      return this._advancing
    })

    this._onremotewriterchangeBound = this._onremotewriterchange.bind(this)
    this._onlocalwriterchangeBound = this._onlocalwriterchange.bind(this)

    this.maxSupportedVersion = DEFAULT_AUTOBASE_VERSION // working version

    this._preopen = null

    this._hasOpen = !!this._handlers.open
    this._hasApply = !!this._handlers.apply
    this._hasOptimisticApply = !!this._handlers.optimistic
    this._hasUpdate = !!this._handlers.update
    this._hasClose = !!this._handlers.close

    this._viewStore = new AutoStore(this)
    this._applyState = null

    this.view = null
    this.core = null
    this.version = -1
    this.interrupted = null
    this.recoveries = RECOVERIES

    const {
      ackInterval = DEFAULT_ACK_INTERVAL,
      ackThreshold = DEFAULT_ACK_THRESHOLD
    } = handlers

    this._ackInterval = ackInterval
    this._ackThreshold = ackThreshold
    this._ackTickThreshold = ackThreshold
    this._ackTick = 0

    this._ackTimer = null
    this._acking = false

    this._waiting = new SignalPromise()
    this._bootRecovery = false

    this.view = this._hasOpen ? this._handlers.open(this._viewStore, new PublicApplyCalls(this)) : null
    this.core = this._viewStore.get({ name: '_system' })

    if (this.fastForwardEnabled && isObject(handlers.fastForward)) {
      this._runFastForward(new FastForward(this, handlers.fastForward.key, { verified: false })).catch(noop)
    }

    this.ready().catch(safetyCatch)
  }

  [inspect] (depth, opts) {
    let indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    return indent + 'Autobase { ... }'
  }

  // just compat, use .key
  get bootstrap () {
    return this.key
  }

  // TODO: compat, will be removed
  get bootstraps () {
    return [this.bootstrap]
  }

  get writable () {
    return this.localWriter !== null && !this.localWriter.isRemoved
  }

  get ackable () {
    return this.localWriter !== null && this.localWriter.isActiveIndexer
  }

  get signedLength () {
    return this.core.signedLength
  }

  get indexedLength () {
    return this._applyState ? this._applyState.indexedLength : 0
  }

  get length () {
    return this.core.length
  }

  hash () {
    return this.core.treeHash()
  }

  // deprecated, use .core.key
  getSystemKey () {
    return this.core.key
  }

  get system () {
    return this._applyState && this._applyState.system
  }

  // deprecated
  async getIndexedInfo () {
    if (this.opened === false) await this.ready()
    return this._applyState && this._applyState.system.getIndexedInfo(this._applyState.indexedLength)
  }

  _isActiveIndexer () {
    return this.localWriter ? this.localWriter.isActiveIndexer : false
  }

  replicate (isInitiator, opts) {
    const stream = this.store.replicate(isInitiator, opts)
    this.wakeupProtocol.addStream(stream)
    return stream
  }

  heads () {
    if (!this._applyState || !this._applyState.opened) return []
    const nodes = new Array(this._applyState.system.heads.length)
    for (let i = 0; i < this._applyState.system.heads.length; i++) nodes[i] = this._applyState.system.heads[i]
    return nodes.sort(compareNodes)
  }

  hintWakeup (hints) {
    if (!Array.isArray(hints)) hints = [hints]
    for (const { key, length } of hints) {
      const hex = b4a.toString(key, 'hex')
      const prev = this._wakeupHints.get(hex)
      if (!prev || length === -1 || prev < length) this._wakeupHints.set(hex, length)
    }
    this._queueBump()
  }

  _queueBump () {
    this._bump().catch(safetyCatch)
  }

  async _runPreOpen () {
    if (this._handlers.wait) await this._handlers.wait()

    await this.store.ready()

    this.keyPair = (await this._handlers.keyPair) || null

    const result = await boot(this.store, this.key, {
      encryptionKey: this.encryptionKey,
      encrypt: this.encrypt,
      keyPair: this.keyPair
    })

    const pointer = await result.local.getUserData('autobase/boot')

    if (pointer) {
      const { recoveries } = c.decode(messages.BootRecord, pointer)
      this.recoveries = recoveries
    }

    this._primaryBootstrap = result.bootstrap
    this.local = result.local

    this.key = result.bootstrap.key
    this.discoveryKey = result.bootstrap.discoveryKey
    this.id = result.bootstrap.id

    this.encryptionKey = result.encryptionKey

    if (this.encrypted) {
      assert(this.encryptionKey !== null, 'Encryption key is expected')
    }

    if (this.encryptionKey) {
      this.encryption = new EncryptionView(this, null)

      this.local.setEncryption(this.getWriterEncryption())
      this._primaryBootstrap.setEncryption(this.getWriterEncryption())
    }

    if (this.nukeTip) await this._nukeTip()

    this.local.on('append', this._onlocalwriterchangeBound)

    this.wakeupCapability = (await this._handlers.wakeupCapability) || { key: this.key, discoveryKey: this.discoveryKey }
    this.setWakeup(this.wakeupCapability.key, this.wakeupCapability.discoveryKey)
  }

  async _nukeTipBatch (key, length) {
    const core = this.store.get({ key, active: false })
    await core.ready()

    const batch = core.session({ name: 'batch' })
    await batch.ready()
    if (batch.length > length) await batch.truncate(length)
    await batch.close()

    await core.close()
  }

  // TODO: not atomic atm, so more of a (very useful) debug helper
  async _nukeTip () {
    const pointer = await this.local.getUserData('autobase/boot')
    if (!pointer) return

    const boot = c.decode(messages.BootRecord, pointer)

    await LocalState.clear(this.local)

    await this._nukeTipBatch(boot.key, boot.systemLength)

    const core = this.store.get({ key: boot.key, active: false, encryption: null })
    const encCore = await EncryptionView.setSystemEncryption(this, core)

    const batch = core.session({ name: 'batch' })
    await batch.ready()

    const info = await SystemView.getIndexedInfo(batch, boot.systemLength)
    await batch.close()

    for (const view of info.views) { // ensure any views ref'ed by system are consistent as well
      await this._nukeTipBatch(view.key, view.length)
    }

    if (boot.heads) this.hintWakeup(boot.heads)
    if (this.local.length) this.hintWakeup([{ key: this.local.key, length: this.local.length }])

    await core.close()
    if (encCore) await encCore.close()
  }

  _bumpWakeupPeer (peer) {
    if (this._coupler) this._coupler.update(peer.stream)
  }

  async _rotateLocalWriter (newLocal) {
    assert(!this.writable, 'Cannot rotate a newLocal writer if a current one is open')

    const oldLocal = this.local

    if (this._applyState) await this._applyState.close()

    this.local = newLocal
    this.local.on('append', this._onlocalwriterchangeBound)

    this.localWriter = null
    this._updateLocalCore = null

    const store = this._viewStore.atomize()

    const local = store.getLocal()
    await local.ready()

    await LocalState.moveTo(oldLocal, local)

    await local.setUserData('referrer', this.key)
    if (this.encryptionKey) await local.setUserData('autobase/encryption', this.encryptionKey)

    await store.flush()
    await store.close()

    await this._primaryBootstrap.setUserData('autobase/local', local.key)

    await this._clearWriters()
    await oldLocal.close()

    // done, soft reboot

    await this._viewStore.updateLocal()

    await this._clearWriters()

    this._applyState = new ApplyState(this)
    await this._applyState.ready()
    await this._makeLinearizerFromViewState()

    this._caughtup = false

    this._rebooted()
  }

  async setLocal (key, { keyPair } = {}) {
    if (!this.opened) await this.ready()

    const manifest = keyPair ? { version: this.store.manifestVersion, signers: [{ publicKey: keyPair.publicKey }] } : null
    if (!key) key = Hypercore.key(manifest)
    // If the keys are the same, no need to rotate
    if (b4a.equals(key, this.local.key)) return

    const encryption = this.encryptionKey ? this.getWriterEncryption() : null

    const local = this.store.get({ key, manifest, active: false, exclusive: true, encryption, valueEncoding: messages.OplogMessage })
    await local.ready()

    this._updateLocalCore = local

    await this._bump()
  }

  setWakeup (cap, discoveryKey) {
    if (this.wakeupSession) this.wakeupSession.destroy()
    if (!discoveryKey && b4a.equals(cap, this.key)) discoveryKey = this.discoveryKey
    this.wakeupSession = this.wakeupProtocol.session(cap, new WakeupHandler(this, discoveryKey || null))

    // incase this session has active peers already, bump the coupler
    for (const peer of this.wakeupSession.peers) {
      if (peer.active) this._bumpWakeupPeer(peer)
    }
  }

  async _getMigrationPointer (key, length) {
    const core = this.store.get({ key, active: false, encryption: null })
    const encCore = await EncryptionView.setSystemEncryption(this, core)

    const min = (core.manifest && core.manifest.prologue) ? core.manifest.prologue.length : 0

    for (let i = length - 1; i >= min; i--) {
      if (!(await core.has(i))) continue

      const sys = new SystemView(core, { checkout: i + 1 })
      await sys.ready()

      let good = true

      for (const v of sys.views) {
        const vc = this.store.get({ key: v.key, active: false })
        await vc.ready()
        if (vc.length < v.length) good = false
        await vc.close()
      }

      await sys.close()
      if (!good) continue

      return i + 1
    }

    await core.close()
    if (encCore) await encCore.close()

    return min
  }

  // migrating from 6 -> latest
  async _migrate6 (key, length) {
    const core = this.store.get({ key, active: false })
    await core.ready()
    const batch = core.session({ name: 'batch', overwrite: true, checkout: length })
    await batch.ready()
    await batch.close()
    await core.close()
  }

  // called by view-store for bootstrapping
  async _getSystemInfo () {
    const boot = await this._getBootRecord()
    if (!boot.key) return null

    const migrated = !!boot.heads
    const indexedLength = boot.systemLength

    if (migrated) { // ensure system batch is consistent on initial migration
      await this._migrate6(boot.key, indexedLength)
    }

    const core = this.store.get({ key: boot.key, encryption: null, active: false })
    await core.ready()

    const batch = core.session({ name: 'batch' })
    const encCore = await EncryptionView.setSystemEncryption(this, batch)

    const info = await SystemView.getIndexedInfo(batch, indexedLength)

    if (encCore) await encCore.close()
    await batch.close()
    await core.close()

    if (info.version > MAX_AUTOBASE_VERSION) {
      throw new Error('Autobase upgrade required.')
    }

    // just compat
    if (migrated) {
      this.migrated = true

      for (const view of info.views) { // ensure any views ref'ed by system are consistent as well
        await this._migrate6(view.key, view.length)
      }

      if (boot.heads) this.hintWakeup(boot.heads)
      if (this.local.length) this.hintWakeup([{ key: this.local.key, length: this.local.length }])
    }

    return {
      key: boot.key,
      indexers: info.indexers,
      views: info.views,
      entropy: info.entropy
    }
  }

  // called by the apply state for bootstrapping
  async _getBootRecord () {
    await this._preopen

    const pointer = await this.local.getUserData('autobase/boot')

    const boot = pointer
      ? c.decode(messages.BootRecord, pointer)
      : { key: null, systemLength: 0, indexersUpdated: false, fastForwarding: false, recoveries: RECOVERIES, heads: null }

    if (boot.heads) {
      const len = await this._getMigrationPointer(boot.key, boot.systemLength)
      if (len !== boot.systemLength) this._warn(new Error('Invalid pointer in migration, correcting (' + len + ' vs ' + boot.systemLength + ')'))
      boot.systemLength = len
    }

    return boot
  }

  _interrupt (reason) {
    assert(!!this._applyState.applying, 'Interrupt is only allowed in apply')
    this._interrupting = true
    if (reason) this.interrupted = reason
    throw INTERRUPT
  }

  async flush () {
    if (this.opened === false) await this.ready()
    await this._advancing
  }

  recouple () {
    if (this._coupler) this._coupler.destroy()
    const core = this._viewStore.getSystemCore()
    this._coupler = new CoreCoupler(core, this._wakeupPeerBound)
  }

  _updateBootstrapWriters () {
    const writers = this.linearizer.getBootstrapWriters()

    // first clear all, but without applying it for churn reasons
    for (const writer of this._bootstrapWriters) {
      writer.isBootstrap = false
      writer.isCoupled = false
    }

    // all passed are bootstraps
    for (const writer of writers) {
      writer.isCoupled = true
      writer.setBootstrap(true)
    }

    // reset activity on old ones, all should be in sync now
    for (const writer of this._bootstrapWriters) {
      if (writer.isBootstrap === false) writer.setBootstrap(false)
    }

    this._bootstrapWriters = writers
    this._bootstrapWritersChanged = false
  }

  async _openLinearizer () {
    if (this._applyState.system.bootstrapping) {
      await this._makeLinearizer(null)
      this._bootstrapLinearizer()
      return
    }

    await this._makeLinearizerFromViewState()
  }

  async _catchupApplyState () {
    if (await this._applyState.shouldMigrate()) {
      await this._migrate()
    } else {
      await this._applyState.catchup(this.linearizer)
    }

    this._caughtup = true
  }

  async _open () {
    this._preopen = this._runPreOpen()
    await this._preopen

    if (this.closing) return

    this._applyState = new ApplyState(this)

    try {
      await this._applyState.ready()
    } catch (err) {
      if (this.closing) return

      try {
        await this._applyState.close()
      } catch {}

      try {
        await this.core.ready()
      } catch {}

      this._applyState = null
      if (this.closing) return

      this._warn(new Error('Failed to boot due to: ' + err.message))

      if (this.recoveries < RECOVERIES) {
        this._bootRecovery = true
        this._queueBump()
        return
      }

      throw err
    }

    try {
      await this._openLinearizer()
      await this.core.ready()
      await this._wakeup.ready()
    } catch (err) {
      if (this.closing) return
      throw err
    }

    if (this.core.length - this._applyState.indexedLength > this._ackTickThreshold) {
      this._ackTick = this._ackTickThreshold
    }
    if (this.localWriter && this._ackInterval) {
      this._startAckTimer()
    }

    this._updateBootstrapWriters()

    this.recouple()
    this._queueFastForward()

    // queue a full bump that handles wakeup etc (not legal to wait for that here)
    this._queueBump()
  }

  async _close () {
    this._interrupting = true
    await Promise.resolve() // defer one tick

    if (this.wakeupSession) this.wakeupSession.destroy()
    if (this.wakeupOwner) this.wakeupProtocol.destroy()

    if (this.fastForwarding) await this.fastForwarding.close()

    if (this._coupler) this._coupler.destroy()
    this._coupler = null
    this._waiting.notify(null)

    await this.activeWriters.clear()

    const closing = this._advancing ? this._advancing.catch(safetyCatch) : null

    await this._clearWriters()
    if (this._primaryBootstrap) await this._primaryBootstrap.close()
    await this.local.close()

    if (this._ackTimer) {
      this._ackTimer.stop()
      await this._ackTimer.flush()
    }

    await this._wakeup.close()

    if (this._hasClose) await this._handlers.close(this.view)
    if (this._applyState) await this._applyState.close()

    await this._viewStore.close()
    await this.core.close()
    await this.store.close()

    if (this._writable) this._writable.resolve(false)

    await closing
  }

  _onError (err) {
    if (this.closing) return

    if (err === INTERRUPT) {
      this.emit('interrupt', this.interrupted)
      this.emit('update')
      return
    }

    this.close().catch(safetyCatch)

    // if no one is listening we should crash! we cannot rely on the EE here
    // as this is wrapped in a promise so instead of nextTick throw it
    if (ReadyResource.listenerCount(this, 'error') === 0) {
      crashSoon(err)
      return
    }

    this.emit('error', err)
  }

  async _closeWriter (w) {
    this.activeWriters.delete(w)
    await w.close()
  }

  async _gcWriters () {
    // just return early, why not
    if (this._checkWriters.length === 0) return

    while (this._checkWriters.length > 0) {
      const w = this._checkWriters.pop()

      // doesnt hurt
      w.updateActivity()

      if (!w.flushed()) continue

      const unqueued = this._wakeup.unqueue(w.core.key, w.core.length)

      if (!unqueued || w.isActiveIndexer) continue
      if (this.localWriter === w) continue

      await this._closeWriter(w)
    }

    await this._wakeup.flush()
  }

  _startAckTimer () {
    if (this._ackTimer) return
    this._ackTimer = new Timer(this._backgroundAck.bind(this), this._ackInterval)
    this._bumpAckTimer()
  }

  _bumpAckTimer () {
    if (!this._ackTimer) return
    this._ackTimer.bump()
  }

  async update () {
    if (this.opened === false) await this.ready()

    try {
      await this._bump()
      if (this._acking) await this._bump() // if acking just rebump incase it was triggered from above...
    } catch (err) {
      if (this._interrupting) return
      throw err
    }
  }

  // runs in bg, not allowed to throw
  // TODO: refactor so this only moves the writer affected to a updated set
  async _onremotewriterchange () {
    this._bumpAckTimer()

    try {
      await this._bump()
    } catch {}
  }

  _onlocalwriterchange () {
    if (!this.localWriter || this.localWriter.isRemoved) this._queueBump()
  }

  _onwakeup () {
    this._needsWakeup = true
    this._queueBump()
  }

  isFastForwarding () {
    if (this.fastForwardTo !== null) return true
    return this.fastForwardEnabled && this.fastForwarding !== null
  }

  _backgroundAck () {
    return this.ack(true)
  }

  async ack (bg = false) {
    if (this.opened === false) await this.ready()
    if (this.localWriter === null || this._acking || this._interrupting || this._appending !== null) return

    if (this._applyState === null) {
      try {
        await this._bump()
      } catch {}
      if (this._applyState === null || this._interrupting) return
    }

    const applyState = this._applyState
    if (applyState.opened === false) await applyState.ready()

    const isPendingIndexer = applyState.isLocalPendingIndexer()

    // if no one is waiting for our index manifest, wait for FF before pushing an ack
    if ((!isPendingIndexer && this.isFastForwarding()) || this._interrupting) return

    const isIndexer = applyState.isLocalIndexer() || isPendingIndexer
    if (!isIndexer) return

    this._acking = true

    try {
      await this._bump()
    } catch (err) {
      if (!this._interrupting) throw err
    }

    if (this._interrupting || !this.localWriter || this.localWriter.closed) {
      this._acking = false
      return
    }

    // avoid lumping acks together due to the bump wait here
    if (this._ackTimer && bg) await this._ackTimer.asapStandalone()
    if (this._interrupting) {
      this._acking = false
      return
    }

    const alwaysWrite = isPendingIndexer || this._applyState.shouldWrite()

    if (alwaysWrite || this.linearizer.shouldAck(this.localWriter, false)) {
      try {
        if (this.localWriter && !this.localWriter.closed) await this.append(null)
      } catch (err) {
        if (!this._interrupting) throw err
      }
    }

    if (!this._interrupting) {
      this._updateAckThreshold()
      this._bumpAckTimer()
    }

    this._acking = false
  }

  views () {
    if (this._applyState) return this._applyState.store.listViews()
    return this._viewStore.listViews()
  }

  async append (value, opts) {
    if (this.opened === false) await this.ready()
    if (this._advancing !== null) await this._advancing

    if (this._interrupting) throw new Error('Autobase is closing')
    if (value && this.valueEncoding !== BINARY_ENCODING) value = normalize(this.valueEncoding, value)

    const optimistic = !!opts && !!opts.optimistic && !!value

    // we wanna allow acks so interdexers can flush
    if (!optimistic && (this.localWriter === null || (this.localWriter.isRemoved && value !== null))) {
      throw new Error('Not writable')
    }

    if (this._appending === null) this._appending = []

    if (Array.isArray(value)) {
      for (const v of value) this._append(v)
    } else {
      this._append(value)
    }

    if (optimistic) this._optimistic = this._appending.length - 1
    const target = this._appended + this._appending.length

    // await in case append is in current tick
    if (this._advancing) await this._advancing

    // bump until we've flushed the nodes
    while (this._appended < target && !this._interrupting) {
      await this._bump()
      // safety
      if (this.localWriter && this.localWriter.idle()) return
    }
  }

  _append (value) {
    // if prev value is an ack that hasnt been flushed, skip it
    if (this._appending.length > 0) {
      if (value === null) return
      if (this._appending[this._appending.length - 1] === null) {
        this._appending.pop()
      }
    }
    this._appending.push(value)
  }

  static decodeValue (value, opts) {
    return decodeValue(value, opts)
  }

  static encodeValue (value, opts = {}) {
    const block = c.encode(messages.OplogMessage, {
      version: opts.version || DEFAULT_AUTOBASE_VERSION,
      maxSupportedVersion: opts.maxVersion || DEFAULT_AUTOBASE_VERSION,
      digest: null,
      checkpoint: null,
      optimistic: !!opts.optimistic,
      node: {
        heads: opts.heads || [],
        batch: 1,
        value
      }
    })

    if (!opts.encrypted) return block

    if (!opts.optimistic) {
      throw new Error('Encoding an encrypted value is not supported')
    }

    const padding = b4a.alloc(16) // min hash length is 16
    crypto.hash(block, padding)
    padding[0] = 0

    return b4a.concat([padding.subarray(0, 8), block])
  }

  static async getLocalKey (store, opts = {}) {
    const core = opts.keyPair ? store.get({ ...opts, active: false }) : store.get({ ...opts, name: 'local', active: false })
    await core.ready()
    const key = core.key
    await core.close()
    return key
  }

  static getLocalCore (store, handlers, encryptionKey) {
    const encryption = !encryptionKey ? null : { key: encryptionKey }
    const opts = { ...handlers, compat: false, active: false, exclusive: true, valueEncoding: messages.OplogMessage, encryption }
    return opts.keyPair ? store.get(opts) : store.get({ ...opts, name: 'local' })
  }

  static async getUserData (core) {
    const view = await core.getUserData('autobase/view')

    return {
      referrer: await core.getUserData('referrer'),
      view: view ? b4a.toString(view) : null
    }
  }

  static async isAutobase (core, opts = {}) {
    const block = await core.get(0, opts)
    if (!block) throw new Error('Core is empty.')
    if (!b4a.isBuffer(block)) return isAutobaseMessage(block)

    try {
      const m = c.decode(messages.OplogMessage, block)
      return isAutobaseMessage(m)
    } catch {
      return false
    }
  }

  // no guarantees where the user data is stored, just that its associated with the base
  async setUserData (key, val) {
    await this._preopen
    const core = this._primaryBootstrap === null ? this.local : this._primaryBootstrap

    await core.setUserData(key, val)
  }

  async getUserData (key) {
    await this._preopen
    const core = this._primaryBootstrap === null ? this.local : this._primaryBootstrap

    return await core.getUserData(key)
  }

  _needsLocalWriter () {
    return this.localWriter === null || this.localWriter.closed
  }

  // no guarantees about writer.isActiveIndexer property here
  async _getWriterByKey (key, len, seen, allowGC, isAdded, system) {
    assert(this._draining === true || (this.opening && !this.opened) || this._optimistic > -1)

    const release = await this._lock()

    if (this._interrupting) {
      release()
      throw new Error('Autobase is closing')
    }

    try {
      let w = this.activeWriters.get(key)

      const alreadyActive = !!w
      const sys = system || this._applyState.system
      const writerInfo = await sys.get(key)

      if (len === -1) {
        if (!allowGC && writerInfo === null) {
          if (w) w.isRemoved = !isAdded
          return null
        }

        len = writerInfo === null ? 0 : writerInfo.length
      }

      const isActive = writerInfo !== null && (isAdded || !writerInfo.isRemoved)
      const isRemoved = !isActive

      if (w) {
        w.isRemoved = isRemoved
      } else {
        w = this._makeWriter(key, len, isActive, isRemoved)
        if (!w) return null
      }

      if (isRemoved && sys.bootstrapping && b4a.equals(w.core.key, this.key)) {
        w.isRemoved = false
      }

      if (this._isLocalCore(w.core) && this._needsLocalWriter()) {
        this._setLocalWriter(w)
      }

      w.seen(seen)

      if (alreadyActive) return w

      await w.ready()

      if (this._isLocalCore(w.core) && this._needsLocalWriter()) {
        this._setLocalWriter(w)
      }

      if (allowGC && w.flushed()) {
        this._wakeup.unqueue(key, len)
        if (w !== this.localWriter) {
          await w.close()
          return w
        }
      }

      this.activeWriters.add(w)
      this._checkWriters.push(w)

      assert(w.opened)
      assert(!w.closed)

      w.updateActivity()
      return w
    } finally {
      release()
    }
  }

  _updateAll () {
    const p = []
    for (const w of this.activeWriters) p.push(w.update(null).catch(this._warn))
    return Promise.all(p)
  }

  getWriterEncryption () {
    if (!this.encryptionKey) return null
    return this.encryption.getWriterEncryption()
  }

  _makeWriterCore (key) {
    if (this.closing) throw new Error('Autobase is closing')
    if (this._interrupting) throw INTERRUPT()

    const local = b4a.equals(key, this.local.key)
    const encryption = this.getWriterEncryption()

    const core = local
      ? this.local.session({ valueEncoding: messages.OplogMessage, encryption, active: false })
      : this.store.get({ key, compat: false, writable: false, valueEncoding: messages.OplogMessage, encryption, active: false })

    return core
  }

  _makeWriter (key, length, isActive, isRemoved) {
    const core = this._makeWriterCore(key)
    const w = new Writer(this, core, length, isRemoved)

    if (this._isLocalCore(core)) {
      if (isActive) this._setLocalWriter(w) // only set active writer
      return w
    }

    core.on('append', this._onremotewriterchangeBound)
    core.on('download', this._onremotewriterchangeBound)
    core.on('manifest', this._onremotewriterchangeBound)

    return w
  }

  _updateLinearizer (indexers, heads) {
    // only current active indexers are reset to true below
    for (const w of this.activeWriters) w.isActiveIndexer = false
    if (this.localWriter) this.localWriter.isActiveIndexer = false

    for (const writer of indexers) writer.isActiveIndexer = true

    if (this._isActiveIndexer() && !this.isIndexer) {
      this._setLocalIndexer()
    } else if (!this._isActiveIndexer() && this.isIndexer) {
      this._clearLocalIndexer()
    }

    this.linearizer = new Linearizer(indexers, { heads, writers: this.activeWriters })

    this._updateAckThreshold()
  }

  async _updateLocalWriter (sys) {
    if (this.localWriter !== null && !this.localWriter.closed) return
    await this._getWriterByKey(this.local.key, -1, 0, true, false, sys)
  }

  async _bootstrapLinearizer () {
    const bootstrap = this._makeWriter(this.key, 0, true, false)

    this.activeWriters.add(bootstrap)
    this._checkWriters.push(bootstrap)
    await bootstrap.ready()
    this._ensureWakeup(bootstrap)

    this._updateLinearizer([bootstrap], [])
  }

  async _makeLinearizer (sys) {
    if (sys === null) {
      return this._bootstrapLinearizer()
    }

    if (this.opened || await sys.hasLocal(this.local.key)) {
      await this._updateLocalWriter(sys)
    }

    const indexers = []

    for (const head of sys.indexers) {
      const writer = await this._getWriterByKey(head.key, head.length, 0, false, false, sys)
      indexers.push(writer)
    }

    if (!this._isActiveIndexer()) {
      for (const key of sys.pendingIndexers) {
        if (b4a.equals(key, this.local.key)) {
          this._setLocalIndexer()
          break
        }
      }
    }

    this._updateLinearizer(indexers, sys.heads)

    for (const { key, length } of sys.heads) {
      await this._getWriterByKey(key, length, 0, false, false, sys)
    }
  }

  async _clearWriters () {
    await this.activeWriters.clear()
    if (this.localWriter !== null) await this.localWriter.close()
    this._checkWriters = []
  }

  async _makeLinearizerFromViewState () {
    const sys = await this._applyState.getIndexedSystem()
    await this._makeLinearizer(sys)
    await sys.close()
  }

  async _recoverMaybe () {
    if (!this._applyState) {
      if (!this._bootRecovery) return
      await this._runFastForward(new FastForward(this, this.core.key, { force: true }))
      this._queueBump()
      return
    }

    this._bootRecovery = false

    const ff = await this._applyState.recoverAt()
    if (!ff || this.fastForwardTo) return

    this.fastForwardTo = ff
    this._queueBump()
  }

  async _applyFastForward () {
    if (!this.fastForwardTo.force && this.fastForwardTo.length < this.core.length + FastForward.MINIMUM) {
      this.fastForwardTo = null
      this._updateActivity()
      // still not worth it
      return
    }

    const changes = this._hasUpdate ? new UpdateChanges(this) : null
    if (changes) changes.track(this._applyState)

    // close existing state
    if (this._applyState) await this._applyState.close()

    const {
      key,
      length,
      views,
      indexers,
      manifestVersion,
      entropy
    } = this.fastForwardTo

    const from = this.core.signedLength
    const store = this._viewStore.atomize()

    const ffed = new Set()
    const migrated = !b4a.equals(key, this.core.key)

    const systemRef = await this._viewStore.findViewByKey(key, indexers, manifestVersion, entropy)
    ffed.add(systemRef)

    if (migrated) {
      await this._applyFastForwardMigration(systemRef, { key, length })
    } else {
      await systemRef.catchup(store.atom, length)
    }

    for (const v of views) {
      const ref = await this._viewStore.findViewByKey(v.key, indexers, manifestVersion, entropy)
      if (!ref) continue // unknown, view ignored
      ffed.add(ref)

      if (migrated) {
        await this._applyFastForwardMigration(ref, v)
      } else {
        await ref.catchup(store.atom, v.length)
      }
    }

    // migrate zero length cores
    if (migrated) {
      const manifests = await this._viewStore.getIndexerManifests(indexers)

      for (const [name, ref] of this._viewStore.byName) {
        if (ffed.has(ref)) continue
        await this._migrateView(manifests, null, name, 0, manifestVersion, entropy, [], null)
      }
    }

    const value = c.encode(messages.BootRecord, {
      key,
      systemLength: length,
      indexersUpdated: false,
      fastForwarding: true,
      recoveries: RECOVERIES
    })

    const local = store.getLocal()
    await local.ready()
    await local.setUserData('autobase/boot', value)

    await LocalState.clear(local)

    if (changes) changes.finalise()

    await store.flush()
    await store.close()

    const to = this.core.signedLength

    for (const ref of ffed) await ref.release()

    this.recoveries = RECOVERIES
    this.fastForwardTo = null
    this._queueFastForward()

    await this._clearWriters()

    this._applyState = new ApplyState(this)
    await this._applyState.ready()

    if (changes) await this._handlers.update(this._applyState.view, changes)

    if (await this._applyState.shouldMigrate()) {
      await this._migrate()
    } else {
      await this._makeLinearizerFromViewState()
      await this._applyState.catchup(this.linearizer)
    }

    if (!this.localWriter || this.localWriter.closed) {
      await this._updateLocalWriter(this._applyState.system)
    }

    this._caughtup = true

    this._rebooted()
    this.emit('fast-forward', to, from)
  }

  // TODO: not atomic in regards to the ff, fix that
  async _applyFastForwardMigration (ref, v) {
    const next = this.store.get(v.key)
    await next.ready()

    const prologue = next.manifest && next.manifest.prologue

    if (prologue && prologue.length > 0 && ref.core.length >= prologue.length) {
      try {
        await next.core.copyPrologue(ref.core.state)
      } catch {
        // we might be missing some nodes for this, just ignore, only an optimisation
      }
    }

    const batch = next.session({ name: 'batch', overwrite: true, checkout: v.length })
    await batch.ready()

    // remake the batch, reset from our prologue in case it replicated inbetween
    // TODO: we should really have an HC function for this

    await ref.batch.state.moveTo(batch, batch.length)
    await batch.close()

    ref.migrated(this, next)
  }

  async _migrateView (indexerManifests, source, name, indexedLength, manifestVersion, entropy, linked, manifestData) {
    const ref = this._viewStore.byName.get(name)

    const prologue = indexedLength === 0
      ? null
      : { length: indexedLength, hash: await source.treeHash(indexedLength) }

    if (manifestData === null && ref.core.manifest.userData !== null) {
      manifestData = ref.core.manifest.userData
    }

    const next = this._viewStore.getViewCore(indexerManifests, name, prologue, manifestVersion, entropy, linked, manifestData)
    await next.ready()

    if (indexedLength > 0) {
      await next.core.copyPrologue(source.state)
    }

    // remake the batch, reset from our prologue in case it replicated inbetween
    // TODO: we should really have an HC function for this
    const batch = next.session({ name: 'batch', overwrite: true, checkout: indexedLength })
    await batch.ready()

    if (source !== null) {
      while (batch.length < source.length) {
        await batch.append(await source.get(batch.length))
      }
    }

    await ref.batch.state.moveTo(batch, batch.length)
    await batch.close()

    ref.migrated(this, next)

    return ref
  }

  async _migrate () {
    const length = this._applyState.indexedLength
    const system = this._applyState.system

    const info = await system.getIndexedInfo(length)
    const indexerManifests = await this._viewStore.getIndexerManifests(info.indexers)

    const { views, systemView, encryptionView } = this._applyState

    const manifestVersion = this._applyState.system.core.manifest.version

    for (const view of views) {
      const v = this._applyState.getViewFromSystem(view, info)
      const indexedLength = v ? v.length : 0

      const ref = this._viewStore.getViewByName(view.name)
      const source = ref.batch || ref.core
      await source.ready()

      await this._migrateView(indexerManifests, source, view.name, indexedLength, manifestVersion, info.entropy, null, null)
    }

    const source = encryptionView.ref.batch || encryptionView.ref.core
    await source.ready()

    const enc = await this._migrateView(indexerManifests, source, '_encryption', info.encryptionLength, manifestVersion, info.entropy, null, null)

    const linked = [enc.core.key]

    const sysCore = systemView.ref.batch
    const ref = await this._migrateView(indexerManifests, sysCore, '_system', length, manifestVersion, info.entropy, linked, null, null)

    return this._reboot(ref.core.key)
  }

  async _reboot (key) {
    await this._clearWriters()
    await this._makeLinearizerFromViewState()

    await this._applyState.finalize(key)

    this._applyState = new ApplyState(this)

    await this._applyState.ready()
    await this._applyState.catchup(this.linearizer)

    // end soft shutdown

    this._queueFastForward()

    this._rebooted()
  }

  _rebooted () {
    this.recouple()
    this._updateActivity()

    this.emit('reboot')

    // ensure we re-evalute our state
    this._bootstrapWritersChanged = true
    this._needsWakeup = true

    this.updating = true
    this._queueBump()
  }

  _setLocalWriter (w) {
    this.localWriter = w
    if (this._ackInterval) this._startAckTimer()
  }

  _unsetLocalWriter () {
    if (!this.localWriter) return

    this._closeWriter(this.localWriter)
    if (this.localWriter.isActiveIndexer) this._clearLocalIndexer()

    this.localWriter = null
  }

  _setLocalIndexer () {
    assert(this.localWriter !== null)

    this.isIndexer = true
    this.emit('is-indexer')
  }

  _clearLocalIndexer () {
    assert(this.localWriter !== null)

    if (this._ackTimer) this._ackTimer.stop()

    this.isIndexer = false
    this._ackTimer = null

    this.emit('is-non-indexer')
  }

  _isLocalCore (core) {
    return core.writable && core.id === this.local.id
  }

  _addLocalHeads () {
    // not writable atm, will prop be writable in a subsequent bump tho
    if (!this.localWriter || this.localWriter.closed) return null
    // safety, localwriter is still processing, should prop be an assertion
    if (!this.localWriter.idle()) return null

    const length = this._optimistic === -1
      ? this._appending.length
      : this._optimistic || 1

    const nodes = new Array(length)
    for (let i = 0; i < length; i++) {
      const heads = this.linearizer.getHeads()
      const deps = new Set(this.linearizer.heads)
      const batch = this._appending.length - i
      const value = this._appending[i]

      const node = this.localWriter.append(value, heads, batch, deps, this.maxSupportedVersion, this._optimistic === 0)

      this.linearizer.addHead(node)
      nodes[i] = node
    }

    this._appended += length
    this._appending = length === this._appending.length ? null : this._appending.slice(length)

    if (this._optimistic > -1 && this._optimistic < length) this._optimistic = -1

    return nodes
  }

  async _addRemoteHeads () {
    let added = 0

    while (added < REMOTE_ADD_BATCH) {
      await this._updateAll()

      let advanced = 0

      for (const w of this.activeWriters) {
        let node = w.advance()
        if (node === null) continue

        advanced += node.batch

        while (true) {
          this.linearizer.addHead(node)
          if (node.batch === 1) break
          node = w.advance()
        }
      }

      if (advanced === 0) break
      added += advanced
    }

    return added
  }

  async _drain () {
    const writable = this.writable

    while (!this._interrupting && !this.paused) {
      if (this.fastForwardTo !== null) {
        await this._applyFastForward()
        continue // revaluate conditions...
      }

      // we defer this to post ready so its not blocking reading the views
      if (this._caughtup === false) {
        await this._catchupApplyState()
        continue
      }

      const remoteAdded = await this._addRemoteHeads()
      const localNodes = this._appending !== null ? this._addLocalHeads() : null

      if (this._interrupting) return

      if (remoteAdded > 0 || localNodes !== null) {
        this.updating = true
      }

      const u = this.linearizer.update()
      const update = u ? await this._applyState.update(u, localNodes) : { reboot: false, migrated: false }

      if (!update.reboot) {
        if (this._applyState.shouldFlush()) {
          await this._applyState.flush()
          this.updating = true
        }

        if (this._checkWriters.length > 0) {
          await this._gcWriters()
          continue // rerun the update loop as a writer might have been added
        }
        if (remoteAdded >= REMOTE_ADD_BATCH) continue
        break
      }

      await this._gcWriters()

      if (update.migrated) await this._migrate()
      else await this._reboot(this._applyState.system.core.key)
    }

    // emit state changes post drain
    if (writable !== this.writable) {
      if (this.writable && this._writable) this._writable.resolve(true)
      this.emit(writable ? 'unwritable' : 'writable')
    }
  }

  _wakeupPeer (stream) {
    if (!this.wakeupSession) return
    const wakeup = this._getWakeup()
    if (wakeup.length === 0) return
    this.wakeupSession.announceByStream(stream, wakeup)
  }

  _getWakeup () {
    const writers = []

    for (const w of this.activeWriters) {
      if (w.isActiveIndexer || w.flushed()) continue
      writers.push({ key: w.core.key, length: w.length })
    }

    return writers
  }

  async _wakeupWriter (key, length) {
    this._ensureWakeup(await this._getWriterByKey(key, -1, length, true, false, null))
  }

  // ensure wakeup on an existing writer (the writer calls this in addition to above)
  _ensureWakeup (w) {
    if (w === null || w.isBootstrap === true) return
    w.setBootstrap(true) // even if turn false at end of drain, hypercore makes them linger a bit so no churn
    this._bootstrapWriters.push(w)
    this._bootstrapWritersChanged = true
  }

  async _drainWakeup () {
    const promises = []

    // warmup all the below gets
    if (this._needsWakeup) {
      for (const { key, length } of this._wakeup) {
        const w = this.activeWriters.get(key)
        if (w) {
          if (w.length < length) w.seen(length)
          continue
        }
        promises.push(this._applyState.system.get(key))
      }

      if (this._needsWakeupHeads) {
        for (const { key } of await this._applyState.system.heads) {
          if (this.activeWriters.has(key)) continue
          promises.push(this._applyState.system.get(key))
        }
      }
    }
    for (const [hex, length] of this._wakeupHints) {
      const key = b4a.from(hex, 'hex')
      if (length !== -1) {
        const w = this.activeWriters.get(key)
        if (w) {
          if (w.length < length) w.seen(length)
          continue
        }
      }
      promises.push(this._applyState.system.get(key))
    }

    await Promise.allSettled(promises)

    if (this._needsWakeup === true) {
      this._needsWakeup = false

      for (const { key, length } of this._wakeup) {
        if (this.activeWriters.has(key)) continue
        await this._wakeupWriter(key, length)
      }

      if (this._needsWakeupHeads === true) {
        this._needsWakeupHeads = false

        for (const { key } of await this._applyState.system.heads) {
          if (this.activeWriters.has(key)) continue
          await this._wakeupWriter(key, 0)
        }
      }
    }

    for (const [hex, length] of this._wakeupHints) {
      const key = b4a.from(hex, 'hex')
      if (this.activeWriters.has(key)) continue
      if (length !== -1) {
        const info = await this._applyState.system.get(key)
        if (info && length <= info.length) continue // stale hint
      }
      await this._wakeupWriter(key, length === -1 ? 0 : length)
    }

    this._wakeupHints.clear()
  }

  pause () {
    this.paused = true
  }

  resume () {
    this.paused = false
    this._queueBump()
  }

  waitForWritable () {
    if (this.writable) return Promise.resolve(true)
    if (!this._writable) this._writable = rrp()
    return this._writable.promise
  }

  async _advance () {
    if (this.opened === false) await this.ready()
    if (this.paused || this._interrupting) return

    this._draining = true

    if (this.recoveries < FF_RECOVERY || this._bootRecovery) {
      await this._recoverMaybe()
    }

    if (this._updateLocalCore !== null) {
      await this._rotateLocalWriter(this._updateLocalCore)
    }

    const local = this.local.length

    try {
      await this._drain()

      // must run post drain so the linearizer is caught up
      if (this._caughtup && (this._needsWakeup === true || this._wakeupHints.size > 0)) await this._drainWakeup()

      // check if we should update local writer
      if (!this.localWriter || this.localWriter.closed) {
        await this._updateLocalWriter(this._applyState.system)
      }

      this._draining = false
    } catch (err) {
      this._onError(err)
      return
    }

    if (this._interrupting) return

    if (this.localWriter && !this.localWriter.closed) {
      if (this._applyState.isLocalPendingIndexer()) this.ack().catch(noop)
      else if (this._triggerAckAsap()) this._ackTimer.asap()
    }

    // keep bootstraps in sync with linearizer
    if (this.updating === true || this._bootstrapWritersChanged === true) {
      this._updateBootstrapWriters()
    }

    if (this.updating === true) {
      this.updating = false

      if (local !== this.local.length) this._resetAckTick()
      else this._ackTick++

      if (!this._interrupting) this.emit('update')
      this._waiting.notify(null)
    }

    if (!this._interrupting) await this._gcWriters()
  }

  _triggerAckAsap () {
    if (!this._ackTimer) return false

    // flush if threshold is reached and we are not already acking
    if (this._ackTickThreshold && !this._acking && this._ackTick >= this._ackTickThreshold) {
      if (this._ackTimer) {
        for (const w of this.linearizer.indexers) {
          if (w.core.length > w.length) return false // wait for the normal ack cycle in this case
        }
      }

      return true
    }

    return false
  }

  _queueFastForward () {
    if (!this.core.opened) return
    // should have a better way to get this
    const latestSignedLength = this.core.core.state.length

    if (!this.fastForwardEnabled || this.fastForwarding !== null || this._interrupting) return
    if (latestSignedLength - this.core.length < FastForward.MINIMUM) return
    if (this.fastForwardTo !== null) return
    if ((Date.now() - this.fastForwardFailedAt) < MIN_FF_WAIT) return

    this._runFastForward(new FastForward(this, this.core.key)).catch(noop)
  }

  _queueStaticFastForward (key) {
    if (!this.fastForwardEnabled || this.fastForwarding !== null || this._interrupting) return
    if (this.fastForwardTo !== null) return
    if ((Date.now() - this.fastForwardFailedAt) < MIN_FF_WAIT) return

    this._runFastForward(new FastForward(this, key, { verified: false })).catch(noop)
  }

  _updateActivity () {
    this.activeWriters.updateActivity()
    if (this._applyState) {
      if (this.isFastForwarding()) this._applyState.pause()
      else this._applyState.resume()
    }

    // in case we ignored wakeups case we were fast forwarding, request them now if not
    if (this._needsWakeupRequest && !this.isFastForwarding() && this.wakeupSession) {
      this._needsWakeupRequest = false

      this.wakeupSession.broadcastLookup({})
      // tmp, will be removed
      const sys = this._viewStore.getSystemView()
      if (sys.compatExtension) sys.compatExtension.broadcast()
    }
  }

  async _runFastForward (ff) {
    this.fastForwarding = ff

    this._updateActivity()

    const result = await ff.upgrade()
    await ff.close()

    if (this.fastForwarding === ff) this.fastForwarding = null

    if (!result) {
      if (ff.failed) this.fastForwardFailedAt = Date.now()
      else this._queueFastForward()
      this._updateActivity()
      return
    }

    this.fastForwardFailedAt = 0
    this.fastForwardTo = result

    this._bumpAckTimer()
    this._queueBump()
  }

  // triggered from apply
  async _addWriter (key, sys) { // just compat for old version
    assert(!!this._applyState.applying, 'System changes are only allowed in apply')

    const writer = (await this._getWriterByKey(key, -1, 0, false, true, sys)) || this._makeWriter(key, 0, true, false)
    await writer.ready()

    if (!this.activeWriters.has(key)) {
      this.activeWriters.add(writer)
      this._checkWriters.push(writer)
      this._ensureWakeup(writer)
    }

    // fetch any nodes needed for dependents
    this._queueBump()
  }

  // triggered from apply
  _removeWriter (key) { // just compat for old version
    const w = this.activeWriters.get(key)
    if (w) w.isRemoved = true

    this._queueBump()
  }

  removeable (key) {
    return this._applyState ? this._applyState.removeable(key) : false
  }

  _updateAckThreshold () {
    if (this._ackThreshold === 0) return
    if (this._ackTimer) this._ackTimer.bau()
    this._ackTick = 0
    this._ackTickThreshold = random2over1(this.linearizer.indexers.length * this._ackThreshold)
  }

  _resetAckTick () {
    this._ackTick = 0
    if (this._ackTimer) this._ackTimer.bau()
  }

  _shiftWriter (w) {
    w.shift()
    if (w.flushed()) this._checkWriters.push(w)
  }
}

function toKey (k) {
  return b4a.isBuffer(k) ? k : hypercoreId.decode(k)
}

function isAutobaseMessage (msg) {
  return msg.checkpoint ? msg.checkpoint.length > 0 : msg.checkpoint === null
}

function compareNodes (a, b) {
  return b4a.compare(a.key, b.key)
}

function random2over1 (n) {
  return Math.floor(n + Math.random() * n)
}

function noop () {}

function crashSoon (err) {
  queueMicrotask(() => { throw err })
  throw err
}

function isObject (obj) {
  return typeof obj === 'object' && obj !== null
}

function emitWarning (err) {
  safetyCatch(err)
  this.emit('warning', err)
}

function normalize (valueEncoding, value) {
  const state = { buffer: null, start: 0, end: 0 }
  valueEncoding.preencode(state, value)
  state.buffer = b4a.allocUnsafe(state.end)
  valueEncoding.encode(state, value)
  state.start = 0
  return valueEncoding.decode(state)
}
