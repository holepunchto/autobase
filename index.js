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

const Linearizer = require('./lib/linearizer')
const AutoStore = require('./lib/view-store')
const SystemView = require('./lib/system')
const messages = require('./lib/messages')
const Timer = require('./lib/timer')
const Writer = require('./lib/writer')
const ActiveWriters = require('./lib/active-writers')
const CorePool = require('./lib/core-pool')
const AutoWakeup = require('./lib/wakeup')

const WakeupExtension = require('./lib/extension')
const InternalView = require('./lib/view.js')
const boot = require('./lib/boot.js')

const inspect = Symbol.for('nodejs.util.inspect.custom')
const INTERRUPT = new Error('Apply interrupted')

const AUTOBASE_VERSION = 1

// default is to automatically ack
const DEFAULT_ACK_INTERVAL = 10_000
const DEFAULT_ACK_THRESHOLD = 4

const FF_THRESHOLD = 16
const DEFAULT_FF_TIMEOUT = 10_000

const REMOTE_ADD_BATCH = 64

module.exports = class Autobase extends ReadyResource {
  constructor (store, bootstrap, handlers = {}) {
    if (Array.isArray(bootstrap)) bootstrap = bootstrap[0] // TODO: just a quick compat, lets remove soon

    if (bootstrap && typeof bootstrap !== 'string' && !b4a.isBuffer(bootstrap)) {
      handlers = bootstrap
      bootstrap = null
    }

    super()

    this.bootstrap = bootstrap ? toKey(bootstrap) : null
    this.keyPair = handlers.keyPair || null
    this.valueEncoding = c.from(handlers.valueEncoding || 'binary')
    this.store = store
    this.globalCache = store.globalCache || null

    this.encrypted = handlers.encrypted || !!handlers.encryptionKey
    this.encrypt = !!handlers.encrypt
    this.encryptionKey = handlers.encryptionKey || null
    this.encryption = null

    this._tryLoadingLocal = true
    this._primaryBootstrap = null

    this.local = null
    this.localWriter = null
    this.isIndexer = false

    this.activeWriters = new ActiveWriters()
    this.corePool = new CorePool()
    this.linearizer = null
    this.updating = false
    this.wakeupExtension = null

    this.fastForwardEnabled = handlers.fastForward !== false
    this.fastForwarding = 0
    this.fastForwardTo = null

    if (this.fastForwardEnabled && isObject(handlers.fastForward)) {
      this.fastForwardTo = handlers.fastForward
    }

    this._bootstrapWriters = [] // might contain dups, but thats ok
    this._bootstrapWritersChanged = false

    this._checkWriters = []
    this._appending = null
    this._wakeup = new AutoWakeup(this)
    this._wakeupHints = new Map()
    this._wakeupPeerBound = this._wakeupPeer.bind(this)
    this._coupler = null

    this._lock = mutexify()

    this._updatingCores = false
    this._localDigest = null
    this._needsWakeup = true
    this._needsWakeupHeads = true
    this._addCheckpoints = false
    this._firstCheckpoint = true
    this._hasPendingCheckpoint = false
    this._completeRemovalAt = null
    this._systemPointer = 0
    this._maybeStaticFastForward = false // writer bumps this

    this._updates = []
    this._handlers = handlers || {}
    this._warn = emitWarning.bind(this)

    this._draining = false
    this._advancing = null
    this._advanced = null
    this._interrupting = false

    this.paused = false

    this._bump = debounceify(() => {
      this._advancing = this._advance()
      return this._advancing
    })

    this._onremotewriterchangeBound = this._onremotewriterchange.bind(this)

    this.maxSupportedVersion = AUTOBASE_VERSION // working version

    this._preopen = null

    this._hasApply = !!this._handlers.apply
    this._hasOpen = !!this._handlers.open
    this._hasClose = !!this._handlers.close

    this.onindex = handlers.onindex || noop

    this._viewStore = new AutoStore(this)

    this.view = null
    this.applyView = null
    this.version = -1
    this.interrupted = null

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

    // const sysCore = this._viewStore.get({ name: '_system', exclusive: true })

    // this.system = new SystemView(sysCore, {
    //   checkout: 0
    // })

    this.view = this._hasOpen ? this._handlers.open(this._viewStore, this) : null

    this.ready().catch(safetyCatch)
  }

  [inspect] (depth, opts) {
    let indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    return indent + 'Autobase { ... }'
  }

  // TODO: compat, will be removed
  get bootstraps () {
    return [this.bootstrap]
  }

  get writable () {
    return this.localWriter !== null && !this.localWriter.isRemoved
  }

  get ackable () {
    return this.localWriter !== null // prop should add .isIndexer but keeping it simple for now
  }

  get key () {
    return this._primaryBootstrap === null ? this.local.key : this._primaryBootstrap.key
  }

  get discoveryKey () {
    return this._primaryBootstrap === null ? this.local.discoveryKey : this._primaryBootstrap.discoveryKey
  }

  get signedLength () {
    return this.applyView ? this.applyView.system.core.signedLength : 0
  }

  get indexedLength () {
    return this.applyView ? this.applyView.indexedLength : 0
  }

  get length () {
    return this.applyView ? this.applyView.system.core.length : 0
  }

  hash () {
    return this.applyView ? this.applyView.system.core.treeHash() : null
  }

  getIndexedInfo () {
    return this.system.getIndexedInfo(this.applyView.indexedLength)
  }

  _isActiveIndexer () {
    return this.localWriter ? this.localWriter.isActiveIndexer : false
  }

  replicate (init, opts) {
    return this.store.replicate(init, opts)
  }

  heads () {
    const nodes = new Array(this.system.heads.length)
    for (let i = 0; i < this.system.heads.length; i++) nodes[i] = this.system.heads[i]
    return nodes.sort(compareNodes)
  }

  // any pending indexers
  hasPendingIndexers () {
    if (this.system.pendingIndexers.length > 0) return true
    return this.hasUnflushedIndexers()
  }

  // confirmed indexers that aren't in linearizer yet
  hasUnflushedIndexers () {
    if (this.linearizer.indexers.length !== this.system.indexers.length) return true

    for (let i = 0; i < this.system.indexers.length; i++) {
      const w = this.linearizer.indexers[i]
      if (!b4a.equals(w.core.key, this.system.indexers[i].key)) return true
    }

    return false
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

    const result = await boot(this.store, this.bootstrap, {
      encryptionKey: this.encryptionKey,
      encrypt: this.encrypt,
      keyPair: this.keyPair
    })

    this.bootstrap = result.key
    this.local = result.local

    this._primaryBootstrap = result.bootstrap

    this.wakeupExtension = new WakeupExtension(this, this._primaryBootstrap, true)
    this.encryptionKey = result.encryptionKey
    if (this.encryptionKey) this.encryption = { key: this.encryptionKey }

    if (this.encrypted) {
      assert(this.encryptionKey !== null, 'Encryption key is expected')
    }
  }

  // called by view-store for bootstrapping
  async _getSystemInfo () {
    const boot = await this._getBootRecord()
    if (!boot) return null

    const encryption = this.encryptionKey
      ? { key: AutoStore.getBlockKey(this.bootstrap, this.encryptionKey, '_system'), block: true }
      : null

    const core = this.store.get({ key: boot.key, encryption, active: false })
    await core.ready()
    const batch = core.session({ name: 'batch' })
    const info = await SystemView.getIndexedInfo(batch, boot.indexedLength)
    await batch.close()
    await core.close()

    if (info.version > AUTOBASE_VERSION) {
      throw new Error('Autobase upgrade required.')
    }

    // just compat
    if (boot.heads) this.hintWakeup(boot.heads)

    return {
      key: boot.key,
      indexers: info.indexers,
      views: info.views
    }
  }

  // called by the apply state for bootstrapping
  async _getBootRecord () {
    await this._preopen

    const pointer = await this.local.getUserData('autobase/boot')
    return pointer && c.decode(messages.BootRecord, pointer)
  }

  interrupt (reason) {
    assert(this.applyView.applying, 'Interrupt is only allowed in apply')
    this._interrupting = true
    if (reason) this.interrupted = reason
    throw INTERRUPT
  }

  async flush () {
    if (this.opened === false) await this.ready()
    await this._advancing
  }

  getSystemKey () {
    return this.system.core.key
  }

  recouple () {
    if (this._coupler) this._coupler.destroy()
    const core = this._viewStore.getSystemCore()
    this._coupler = new CoreCoupler(core, this._wakeupPeerBound)
  }

  _updateBootstrapWriters () {
    const writers = this.linearizer.getBootstrapWriters()

    // first clear all, but without applying it for churn reasons
    for (const writer of this._bootstrapWriters) writer.isBootstrap = false

    // all passed are bootstraps
    for (const writer of writers) writer.setBootstrap(true)

    // reset activity on old ones, all should be in sync now
    for (const writer of this._bootstrapWriters) {
      if (writer.isBootstrap === false) writer.setBootstrap(false)
    }

    this._bootstrapWriters = writers
    this._bootstrapWritersChanged = false
  }

  async _open () {
    this._preopen = this._runPreOpen()
    await this._preopen

    this.applyView = new InternalView(this)
    await this.applyView.ready()

    if (this.applyView.system.bootstrapping) {
      await this._makeLinearizer(null)
      this._bootstrapLinearizer()
    } else {
      const sys = await this.applyView.getIndexedSystem()
      await this._makeLinearizer(sys)
      await sys.close()
    }

    await this.applyView.catchup(this.linearizer)

    await this._wakeup.ready()

    this.requestWakeup()

    // queue a full bump that handles wakeup etc (not legal to wait for that here)
    this._queueBump()
    this._advanced = this._advancing

    // this.queueFastForward()
    this._updateBootstrapWriters()

    // if (this.fastForwardTo !== null) {
    //   const { key, timeout } = this.fastForwardTo
    //   this.fastForwardTo = null // will get reset once ready
    //   this.initialFastForward(key, timeout || DEFAULT_FF_TIMEOUT * 2)
    // }

    // if (this.localWriter && this._ackInterval) this._startAckTimer()
  }

  async _closeLocalCores () {
    const closing = []
    if (this.system) closing.push(this.system.close())
    if (this._primaryBootstrap) closing.push(this._primaryBootstrap.close())
    if (this.localWriter) closing.push(this._unsetLocalWriter())
    closing.push(this._closeAllActiveWriters())
    if (this.localWriter) await this.localWriter.close()
    await Promise.all(closing)
    await this.local.close()
  }

  async _close () {
    this._interrupting = true
    await Promise.resolve() // defer one tick

    if (this._coupler) this._coupler.destroy()
    this._coupler = null
    this._waiting.notify(null)

    await this.activeWriters.clear()

    const closing = this._advancing.catch(safetyCatch)
    await this._closeLocalCores()

    if (this._ackTimer) {
      this._ackTimer.stop()
      await this._ackTimer.flush()
    }

    await this._wakeup.close()

    if (this._hasClose) await this._handlers.close(this.view)
    if (this.applyView) await this.applyView.close()

    await this._viewStore.close()
    await this.corePool.clear()
    await this.store.close()
    await closing
  }

  _onError (err) {
    if (this.closing) return

    if (err === INTERRUPT) {
      this.emit('interrupt', this.interrupted)
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

  async _closeWriter (w, now) {
    this.activeWriters.delete(w)
    if (!now) this.corePool.linger(w.core)
    await w.close()
  }

  async _gcWriters () {
    // just return early, why not
    if (this._checkWriters.length === 0) return

    while (this._checkWriters.length > 0) {
      const w = this._checkWriters.pop()

      if (!w.flushed()) continue

      const unqueued = this._wakeup.unqueue(w.core.key, w.core.length)
      // this._coupler.remove(w.core)

      if (!unqueued || w.isActiveIndexer) continue
      if (this.localWriter === w) continue

      await this._closeWriter(w, false)
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

  async _waitForIdle () {
    if (this.localWriter) {
      await this.localWriter.ready()
      while (!this.closing && this.localWriter.core.length > this.localWriter.length) {
        await this.localWriter.waitForSynced()
        await this._bump() // make sure its all flushed...
      }
    }
  }

  async update () {
    if (this.opened === false) await this.ready()

    try {
      await this._bump()
      if (this._acking) await this._bump() // if acking just rebump incase it was triggered from above...
      await this._waitForIdle()
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
    } catch (err) {
      if (!this._interrupting) throw err
    }
  }

  _onwakeup () {
    this._needsWakeup = true
    this._queueBump()
  }

  _isPending () {
    for (const key of this.system.pendingIndexers) {
      if (b4a.equals(key, this.local.key)) return true
    }
    return false
  }

  _isFastForwarding () {
    if (this.fastForwardTo !== null) return true
    return this.fastForwardEnabled && this.fastForwarding > 0
  }

  _backgroundAck () {
    return this.ack(true)
  }

  async ack (bg = false) {
    if (this.localWriter === null) return

    const isPendingIndexer = this._isPending()

    // if no one is waiting for our index manifest, wait for FF before pushing an ack
    if (!isPendingIndexer && this._isFastForwarding()) return

    const isIndexer = this.localWriter.isActiveIndexer || isPendingIndexer

    if (!isIndexer || this._acking || this._interrupting) return

    this._acking = true

    try {
      await this._bump()
    } catch (err) {
      if (!this._interrupting) throw err
    }

    if (this._interrupting) return

    // avoid lumping acks together due to the bump wait here
    if (this._ackTimer && bg) await this._ackTimer.asapStandalone()

    if (this._interrupting) return

    const unflushed = this._hasPendingCheckpoint || this.hasUnflushedIndexers()
    if (!this._interrupting && (isPendingIndexer || this.linearizer.shouldAck(this.localWriter, unflushed))) {
      try {
        if (this.localWriter) await this.append(null)
      } catch (err) {
        if (!this._interrupting) throw err
      }

      if (!this._interrupting) {
        this._updateAckThreshold()
        this._bumpAckTimer()
      }
    }

    this._acking = false
  }

  async append (value) {
    if (!this.opened) await this.ready()
    if (this._interrupting) throw new Error('Autobase is closing')

    // we wanna allow acks so interdexers can flush
    if (this.localWriter === null || (this.localWriter.isRemoved && value !== null)) {
      throw new Error('Not writable')
    }

    if (this._appending === null) this._appending = []

    if (Array.isArray(value)) {
      for (const v of value) this._append(v)
    } else {
      this._append(value)
    }

    // await in case append is in current tick
    if (this._advancing) await this._advancing

    // only bump if there are unflushed nodes
    if (this._appending !== null) return this._bump()
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

  static getLocalCore (store, handlers, encryptionKey) {
    const encryption = encryptionKey === null ? null : { key: encryptionKey }
    const opts = { ...handlers, compat: false, active: false, exclusive: true, valueEncoding: messages.OplogMessage, encryption }
    return opts.keyPair ? store.get(opts) : store.get({ ...opts, name: 'local' })
  }

  static async getUserData (core) {
    const view = await core.getUserData('autobase/view')

    return {
      referrer: await core.getUserData('referrer'),
      view: (!view || view[0] !== 0) ? null : c.decode(messages.ViewRecord, view)
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

  getNamespace (key, core) {
    const w = this.activeWriters.get(key)
    if (!w) return null

    const namespace = w.deriveNamespace(core.name)
    const publicKey = w.core.manifest.signers[0].publicKey

    return {
      namespace,
      publicKey
    }
  }

  _needsLocalWriter () {
    return this.localWriter === null || this.localWriter.closed
  }

  // no guarantees about writer.isActiveIndexer property here
  async _getWriterByKey (key, len, seen, allowGC, isAdded, system) {
    assert(this._draining === true || (this.opening && !this.opened))

    const release = await this._lock()

    if (this._interrupting) {
      release()
      throw new Error('Autobase is closing')
    }

    try {
      let w = this.activeWriters.get(key)
      if (w !== null) {
        if (isAdded && w.core.writable && this._needsLocalWriter()) this._setLocalWriter(w)
        if (w.isRemoved && isAdded) w.isRemoved = false

        if (system) {
          const info = await system.get(key)
          const length = info ? info.length : 0
          if (w.length !== length) w.reset(length)
          w.resume()
        }

        w.seen(seen)
        return w
      }

      const sys = system || this.system
      const writerInfo = await sys.get(key)

      if (len === -1) {
        if (!allowGC && writerInfo === null) return null
        len = writerInfo === null ? 0 : writerInfo.length
      }

      const isActive = writerInfo !== null && (isAdded || !writerInfo.isRemoved)

      // assumes that seen is passed 0 everywhere except in writer._ensureNodeDependencies
      const isRemoved = seen === 0
        ? writerInfo !== null && (!isAdded && writerInfo.isRemoved)
        : !isActive // a writer might have referenced a removed writer

      w = this._makeWriter(key, len, isActive, isRemoved)
      if (!w) return null

      w.seen(seen)
      await w.ready()

      if (allowGC && w.flushed()) {
        this._wakeup.unqueue(key, len)
        if (w !== this.localWriter) {
          this.corePool.linger(w.core)
          await w.close()
          return w
        }
      }

      this.activeWriters.add(w)
      this._checkWriters.push(w)
      if (isAdded && w.core.writable && this._needsLocalWriter()) this._setLocalWriter(w)

      // will only add non-indexer writers
      if (this._coupler) this._coupler.add(w.core)

      assert(w.opened)
      assert(!w.closed)

      this._resumeWriter(w)
      return w
    } finally {
      release()
    }
  }

  _updateAll () {
    const p = []
    for (const w of this.activeWriters) p.push(w.update(false).catch(this._warn))
    return Promise.all(p)
  }

  _makeWriterCore (key) {
    if (this.closing) throw new Error('Autobase is closing')
    if (this._interrupting) throw INTERRUPT()

    const pooled = this.corePool.get(key)
    if (pooled) {
      pooled.valueEncoding = messages.OplogMessage
      return pooled
    }

    const local = b4a.equals(key, this.local.key)

    const core = local
      ? this.local.session({ valueEncoding: messages.OplogMessage, encryption: this.encryption, active: false })
      : this.store.get({ key, compat: false, writable: false, valueEncoding: messages.OplogMessage, encryption: this.encryption, active: false })

    return core
  }

  _makeWriter (key, length, isActive, isRemoved) {
    const core = this._makeWriterCore(key)
    const w = new Writer(this, core, length, isRemoved)

    if (core.writable) {
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
    const wasActiveIndexer = this._isActiveIndexer()

    for (const w of this.activeWriters) w.isActiveIndexer = false
    if (this.localWriter) this.localWriter.isActiveIndexer = false

    for (const writer of indexers) writer.isActiveIndexer = true

    if (this._isActiveIndexer() && !wasActiveIndexer) {
      this._setLocalIndexer()
    } else if (!this._isActiveIndexer() && wasActiveIndexer) {
      this._unsetLocalIndexer()
      this._clearLocalIndexer()
    }

    this.linearizer = new Linearizer(indexers, { heads, writers: this.activeWriters })
    // this._addCheckpoints = !!(this.localWriter && (this.localWriter.isActiveIndexer || this._isPending()))
    this._updateAckThreshold()
  }

  _resumeWriter (w) {
    if (!this._isFastForwarding()) w.resume()
  }

  async _updateLocalWriter (sys) {
    if (this.localWriter !== null && !this.localWriter.closed) return
    await this._getWriterByKey(this.local.key, -1, 0, false, false, sys)
    if (this.localWriter !== null && this.localWriter.closed) this.localWriter = null
  }

  async _bootstrapLinearizer () {
    const bootstrap = this._makeWriter(this.bootstrap, 0, true, false)

    this.activeWriters.add(bootstrap)
    this._checkWriters.push(bootstrap)
    bootstrap.inflateBackground()
    await bootstrap.ready()
    this._resumeWriter(bootstrap)

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
      writer.inflateBackground()
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

  async _migrateView (indexerManifests, name, indexedLength) {
    const ref = this._viewStore.byName.get(name)

    const core = ref.batch || ref.core
    const prologue = indexedLength === 0
      ? null
      : { length: indexedLength, hash: (await core.restoreBatch(indexedLength)).hash() }

    const next = this._viewStore.getViewCore(indexerManifests, name, prologue)
    await next.ready()

    if (indexedLength > 0) {
      await next.core.copyPrologue(core.state)
    }

    // remake the batch, reset from our prologue in case it replicated inbetween
    // TODO: we should really have an HC function for this
    const batch = next.session({ name: 'batch', overwrite: true, checkout: indexedLength })
    await batch.ready()

    if (core.length > batch.length) {
      for (let i = batch.length; i < core.length; i++) {
        await batch.append(await core.get(i))
      }
    }

    await ref.batch.state.moveTo(batch, core.length)
    await batch.close()

    // TODO: close old core, for now we just close when the autobase is closed indirectly
    // atm its unsafe to do as moveTo has a bug due to a missing read lock in hc
    ref.core = next
    ref.registerExtension(this)

    return ref
  }

  async migrate () {
    const length = this.applyView.indexedLength
    const system = this.applyView.system

    const info = await system.getIndexedInfo(length)
    const indexerManifests = await this._viewStore.getIndexerManifests(info.indexers)

    for (let i = 0; i < this.applyView.views.length; i++) {
      const name = this.applyView.views[i].name
      const indexedLength = info.views[i].length

      await this._migrateView(indexerManifests, name, indexedLength)
    }

    const ref = await this._migrateView(indexerManifests, '_system', length)

    // start soft shutdown

    await this.activeWriters.clear()
    if (this.localWriter !== null) await this.localWriter.close()
    this._checkWriters = []

    const sys = await this.applyView.getIndexedSystem()
    await this._makeLinearizer(sys)
    await sys.close()

    await this.applyView.finalize(ref.core.key)

    // end soft shutdown

    this.applyView = new InternalView(this)

    await this.applyView.ready()
    await this.applyView.catchup(this.linearizer)

    this.recouple()

    // ensure we re-evalute our state
    this._bootstrapWritersChanged = true
    this.updated = true

    this._queueBump()
  }

  _setLocalWriter (w) {
    this.localWriter = w
    if (this._ackInterval) this._startAckTimer()
  }

  _unsetLocalWriter () {
    if (!this.localWriter) return

    this._closeWriter(this.localWriter, true)
    if (this.localWriter.isActiveIndexer) this._clearLocalIndexer()

    this.localWriter = null
  }

  _setLocalIndexer () {
    assert(this.localWriter !== null)
    if (this.isIndexer) return

    this.isIndexer = true
    this.emit('is-indexer')
  }

  _unsetLocalIndexer () {
    assert(this.localWriter !== null)
    if (!this.isIndexer) return

    this.isIndexer = false
    this.emit('is-non-indexer')
  }

  _clearLocalIndexer () {
    assert(this.localWriter !== null)

    // this.localWriter.isActiveIndexer = false

    if (this._ackTimer) this._ackTimer.stop()
    this._ackTimer = null
    this._addCheckpoints = false
  }

  _addLocalHeads () {
    if (!this.localWriter.idle()) return null

    const nodes = new Array(this._appending.length)
    for (let i = 0; i < this._appending.length; i++) {
      const heads = this.linearizer.getHeads()
      const deps = new Set(this.linearizer.heads)
      const batch = this._appending.length - i
      const value = this._appending[i]

      const node = this.localWriter.append(value, heads, batch, deps, this.maxSupportedVersion)

      this.linearizer.addHead(node)
      nodes[i] = node
    }

    this._appending = null

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
      if (this.opened && this.fastForwardTo !== null) {
        // await this._applyFastForward()
        this.requestWakeup()
      }

      const remoteAdded = this.opened ? await this._addRemoteHeads() : null
      const localNodes = this.opened && this._appending !== null ? this._addLocalHeads() : null

      if (this._maybeStaticFastForward === true && this.fastForwardEnabled === true) await this._checkStaticFastForward()
      if (this._interrupting) return

      if (remoteAdded > 0 || localNodes !== null) {
        this.updating = true
      }

      const u = this.linearizer.update()
      const indexerUpdate = u ? await this.applyView.update(u, localNodes) : false

      if (this.applyView.dirty) await this.applyView.flush()

      if (!indexerUpdate) {
        if (this._checkWriters.length > 0) {
          await this._gcWriters()
          continue // rerun the update loop as a writer might have been added
        }
        if (remoteAdded >= REMOTE_ADD_BATCH) continue
        break
      }

      await this._gcWriters()
      await this.migrate()
    }

    // emit state changes post drain
    if (writable !== this.writable) this.emit(writable ? 'unwritable' : 'writable')
  }

  _wakeupPeer (peer) {
    if (this.wakeupExtension) {
      this.wakeupExtension.sendWakeup(peer.remotePublicKey)
    }
  }

  broadcastWakeup () {
    if (this.wakeupExtension) {
      this.wakeupExtension.broadcastWakeup()
    }
  }

  requestWakeup () {
    if (this.wakeupExtension) {
      this.wakeupExtension.requestWakeup()
    }
  }

  async _wakeupWriter (key) {
    this._ensureWakeup(await this._getWriterByKey(key, -1, 0, true, false, null))
  }

  // ensure wakeup on an existing writer (the writer calls this in addition to above)
  _ensureWakeup (w) {
    if (w === null || w.isBootstrap === true) return
    w.setBootstrap(true) // even if turn false at end of drain, hypercore makes them linger a bit so no churn
    this._bootstrapWriters.push(w)
    this._bootstrapWritersChanged = true
  }

  async _drainWakeup () { // TODO: parallel load the writers here later
    if (this._needsWakeup === true) {
      this._needsWakeup = false

      for (const { key } of this._wakeup) {
        await this._wakeupWriter(key)
      }

      if (this._needsWakeupHeads === true) {
        this._needsWakeupHeads = false

        for (const { key } of await this.applyView.system.heads) {
          await this._wakeupWriter(key)
        }
      }
    }

    for (const [hex, length] of this._wakeupHints) {
      const key = b4a.from(hex, 'hex')
      if (length !== -1) {
        const info = await this.applyView.system.get(key)
        if (info && length < info.length) continue // stale hint
      }

      await this._wakeupWriter(key)
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

  get system () {
    return this.applyView.system
  }

  async _advance () {
    if (this.opened === false) await this.ready()
    if (this.paused) return

    try {
      this._draining = true
      // note: this might block due to network i/o
      if (this._needsWakeup === true || this._wakeupHints.size > 0) await this._drainWakeup()
      await this._drain()
      this._draining = false
    } catch (err) {
      this._onError(err)
      return
    }

    if (!this._interrupting && this.localWriter && this._ackIsNeeded()) {
      if (this._ackTimer) this._ackTimer.asap()
      else this.ack()
    }

    // keep bootstraps in sync with linearizer
    if (this.updating === true || this._bootstrapWritersChanged === true) {
      this._updateBootstrapWriters()
    }

    if (this.updating === true) {
      this.updating = false
      this.emit('update')
      this._waiting.notify(null)
    }

    if (!this.closing) await this._gcWriters()
  }

  _ackIsNeeded () {
    // return false
    // if (!this._addCheckpoints) return false // ack has no impact

    // flush any pending indexers
    if (this.system.pendingIndexers.length > 0) {
      for (const key of this.system.pendingIndexers) {
        if (b4a.equals(key, this.local.key) && !b4a.equals(key, this.bootstrap)) {
          return true
        }
      }
    }

    return false

    // flush any pending migrates
    // for (const view of this._viewStore.opened.values()) {
    //   if (view.queued === -1) continue

    //   const checkpoint = view.signer.bestCheckpoint(this.localWriter)
    //   const length = checkpoint ? checkpoint.length : 0

    //   if (length < view.queued && length < view.indexedLength) {
    //     this._hasPendingCheckpoint = true
    //     return true
    //   }
    // }

    // // flush if threshold is reached and we are not already acking
    // if (this._ackTickThreshold && !this._acking && this._ackTick >= this._ackTickThreshold) {
    //   if (this._ackTimer) { // the bool in this case is implicitly an "asap" signal
    //     for (const w of this.linearizer.indexers) {
    //       if (w.core.length > w.length) return false // wait for the normal ack cycle in this case
    //     }

    //     return this.linearizer.shouldAck(this.localWriter, this.hasUnflushedIndexers())
    //   }

    //   return true
    // }

    // return false
  }

  doneFastForwarding () {
    if (--this.fastForwarding === 0 && !this._isFastForwarding()) {
      for (const w of this.activeWriters) w.resume()
    }
  }

  async _checkStaticFastForward () {
    let tally = null

    for (let i = 0; i < this.linearizer.indexers.length; i++) {
      const w = this.linearizer.indexers[i]
      if (w.system !== null && !b4a.equals(w.system, this.system.core.key)) {
        if (tally === null) tally = new Map()
        const hex = b4a.toString(w.system, 'hex')
        tally.set(hex, (tally.get(hex) || 0) + 1)
      }
    }

    if (tally === null) {
      this._maybeStaticFastForward = false
      return
    }

    const maj = (this.linearizer.indexers.length >> 1) + 1

    let candidate = null
    for (const [hex, vote] of tally) {
      if (vote < maj) continue
      candidate = b4a.from(hex, 'hex')
      break
    }

    if (candidate && !this._isFastForwarding()) {
      await this.initialFastForward(candidate, DEFAULT_FF_TIMEOUT * 2)
    }
  }

  async initialFastForward (key, timeout) {
    this.fastForwarding++

    const encryption = this._viewStore.getBlockEncryption(this._viewStore.getSystemCore().name)

    const core = this.store.get({ key, encryption })
    await core.ready()

    // get length from network
    const length = await new Promise((resolve, reject) => {
      if (core.length) return resolve(core.length)

      const timer = setTimeout(() => {
        core.off('append', resolveLength)
        resolve(0)
      }, timeout)

      core.once('append', resolveLength)

      function resolveLength () {
        clearTimeout(timer)
        resolve(core.length)
      }
    })

    if (!length || length < this.system.core.indexedLength) {
      await core.close()
      this.doneFastForwarding()
      this.queueFastForward()
      return
    }

    let target = null

    try {
      target = await this._preFastForward(core, length, timeout)
    } finally {
      await core.close()
    }

    // initial fast-forward failed
    if (target === null) {
      this.doneFastForwarding()
      return
    }

    this.fastForwardTo = target
    this.doneFastForwarding()

    this._bumpAckTimer()
    this._queueBump()
  }

  async queueFastForward () {
    // if already FFing, let the finish. TODO: auto kill the attempt after a while and move to latest?
    if (!this.fastForwardEnabled || this.fastForwarding > 0) return

    const core = this.system.core
    const originalCore = this._viewStore.getSystemCore().originalCore

    if (originalCore.length <= core.length + FF_THRESHOLD) return
    if (this.fastForwardTo !== null && originalCore.length <= this.fastForwardTo.length + FF_THRESHOLD) return
    if (!originalCore.length) return

    this.fastForwarding++
    const target = await this._preFastForward(originalCore, originalCore.length, DEFAULT_FF_TIMEOUT)

    // fast-forward failed
    if (target === null) {
      this.doneFastForwarding()
      return
    }

    // if it migrated underneath us, ignore for now
    if (core !== this.system.core) {
      this.doneFastForwarding()
      return
    }

    this.fastForwardTo = target
    this.doneFastForwarding()

    this._bumpAckTimer()
    this._queueBump()
  }

  // NOTE: runs in parallel with everything, can never fail
  async _preFastForward (core, length, timeout) {
    if (length === 0) return null

    const info = {
      key: core.key,
      length,
      localLength: 0
    }

    // pause writers
    for (const w of this.activeWriters) w.pause()

    let sess = null

    try {
      // sys runs open with wait false, so get head block first for low complexity
      if (!(await core.has(length - 1))) {
        await core.get(length - 1, { timeout })
      }

      sess = core.session()
      await sess.ready()

      let system = new SystemView(sess, {
        checkout: length,
        maxCacheSize: this.maxCacheSize
      })

      await system.ready()
      await this.system.ready()

      if (system.version > this.maxSupportedVersion) {
        const upgrade = {
          version: system.version,
          length
        }

        this.emit('upgrade-available', upgrade)
        return null
      }

      const systemShouldMigrate = b4a.equals(core.key, this.system.core.key) &&
        !system.sameIndexers(this.linearizer.indexers)

      const localLookup = this.localWriter ? system.get(this.local.key, { timeout }) : null
      if (localLookup) localLookup.catch(noop)

      const indexers = []
      const pendingViews = []

      for (const { key, length } of system.indexers) {
        if (length === 0) continue
        const core = this.store.get(key)
        await core.ready()
        indexers.push({ key, core, length })
      }

      // handle rest of views
      for (const v of system.views) {
        const core = this.store.get(v.key)

        // same as below, we technically just need to check that we have the hash, not the block
        if (v.length === 0 || await core.has(v.length - 1)) {
          await core.close()
        } else {
          pendingViews.push({ core, length: v.length })
        }
      }

      // handle system migration
      if (systemShouldMigrate) {
        const hash = system.core.core.state.hash()
        const name = this._viewStore.getSystemCore().name
        const prologue = { hash, length }

        info.key = this.deriveKey(name, indexers, prologue)

        await system.close()
        await sess.close()

        sess = this.store.get(info.key)
        await sess.get(length - 1, { timeout })

        system = new SystemView(sess, {
          checkout: length,
          maxCacheSize: this.maxCacheSize
        })

        await system.ready()
      }

      const promises = []

      for (const { key, core, length } of indexers) {
        if (core.length === 0 && length > 0) promises.push(core.get(length - 1, { timeout }))
        promises.push(system.get(key, { timeout }))
      }

      for (const { core, length } of pendingViews) {
        // we could just get the hash here, but likely user wants the block so yolo
        promises.push(core.get(length - 1, { timeout }))
      }

      await Promise.all(promises)

      if (localLookup) {
        const value = await localLookup
        if (value) info.localLength = value.isRemoved ? -1 : value.length
      }

      const closing = []

      for (const { core } of pendingViews) {
        closing.push(core.close())
      }

      closing.push(system.close())

      await Promise.allSettled(closing)
    } catch (err) {
      safetyCatch(err)
      return null
    } finally {
      if (sess) await sess.close()
    }

    return info
  }

  _clearFastForward (queue) {
    if (this.fastForwarding === 0) {
      for (const w of this.activeWriters) w.resume()
    }
    this.fastForwardTo = null
    if (queue) this.queueFastForward() // queue in case we lost an ff while applying this one
  }

  async _applyFastForward () {
    // remember these in case another fast forward gets queued
    const { key, length, localLength } = this.fastForwardTo

    const migrated = !b4a.equals(key, this.system.core.key)

    const name = this._viewStore.getSystemCore().name
    const encryption = this._viewStore.getBlockEncryption(name)

    const core = this.store.get({ key, encryption })
    await core.ready()

    const from = this.system.core.length

    // just extra sanity check that we are not going back in time, nor that we cleared the storage needed for ff
    if (from >= length || core.length < length) {
      this._clearFastForward(true)
      await core.close()
      return
    }

    const system = new SystemView(core, {
      checkout: length,
      maxCacheSize: this.maxCacheSize
    })

    await system.ready()

    const opened = [core]
    const indexers = [] // only used in migrate branch
    const prologues = [] // only used in migrate branch

    // preload async state
    if (migrated) {
      for (const { key } of system.indexers) {
        const core = this.store.get(key)
        await core.ready()
        indexers.push({ core })
        await core.close()
      }

      for (const { key } of system.views) {
        const core = this.store.get(key)
        await core.ready()
        prologues.push(core.manifest.prologue)
        await core.close()
      }
    }

    const views = new Map()

    const currentInfo = await this.system.getIndexedInfo(this._indexedLength)

    const sysView = this._viewStore.getSystemCore()
    const sysInfo = { key, length, treeLength: this._indexedLength, systemIndex: -1 }

    views.set(sysView, sysInfo)

    for (let i = 0; i < system.views.length; i++) {
      const v = system.views[i]

      // TODO: check behaviour if new view keys (+ double FF)
      let view = this._viewStore.getByKey(v.key)

      // search for corresponding view
      if (!view) {
        for (view of this._viewStore.opened.values()) {
          const key = this.deriveKey(view.name, indexers, i < prologues.length ? prologues[i] : null)
          if (b4a.equals(key, v.key)) break
          view = null
        }
      }

      if (!view) {
        await closeAll(opened)
        this._clearFastForward(false) // something wrong somewhere, likely a bug, just safety
        return
      }

      const core = this.store.get(v.key)
      await core.ready()

      opened.push(core)

      if (core.length < v.length) { // sanity check in case there was a migration etc
        await closeAll(opened)
        this._clearFastForward(true)
        return
      }

      const treeLength = i < currentInfo.views.length ? currentInfo.views[i].length : 0
      views.set(view, { key: v.key, length: v.length, treeLength, systemIndex: i })
    }

    await system.close()
    await this._closeAllActiveWriters(false)

    const atom = this.store.storage.createAtom()

    await this._advanceBootRecord(key, atom)

    for (const view of this._viewStore.opened.values()) {
      const info = views.get(view)
      if (info) await view.catchup(info, atom)
      else if (migrated) await view.migrateTo(indexers, 0, atom)
    }

    await atom.flush()

    this._updates = []

    await this._refreshSystemState(this.system)

    if (this.localWriter) {
      if (localLength < 0) this._unsetLocalWriter()
      else this.localWriter.reset(localLength)
    }

    this._indexedLength = length

    await this._makeLinearizer(this.system)

    this.version = this.system.version

    // manually set the digest
    if (migrated) {
      this._setDigest(key)
      this.recouple()
    }

    if (b4a.equals(this.fastForwardTo.key, key) && this.fastForwardTo.length === length) {
      this._clearFastForward(false)
    }

    this.updating = true
    this.emit('fast-forward', length, from)

    // requeue in case we can do another jump!
    this.queueFastForward()

    await closeAll(opened)
  }

  async _closeAllActiveWriters (keepPool) {
    for (const w of this.activeWriters) {
      if (this.localWriter === w) continue
      await this._closeWriter(w, true)
    }
    if (keepPool) await this.corePool.clear()
  }

  // triggered from apply
  async addWriter (key, { indexer = true, isIndexer = indexer } = {}) { // just compat for old version
    assert(this.applyView.applying, 'System changes are only allowed in apply')

    const sys = this.applyView.system
    await sys.add(key, { isIndexer })

    const writer = (await this._getWriterByKey(key, -1, 0, false, true, null)) || this._makeWriter(key, 0, true, false)
    await writer.ready()

    if (!this.activeWriters.has(key)) {
      this.activeWriters.add(writer)
      this._checkWriters.push(writer)
      this._resumeWriter(writer)
    }

    // If we are getting added as indexer, already start adding checkpoints while we get confirmed...
    if (writer === this.localWriter) {
      if (isIndexer) this._setLocalIndexer()
      else this._unsetLocalIndexer() // unset if demoted
    }

    // fetch any nodes needed for dependents
    this._queueBump()
  }

  removeable (key, sys = this.system) {
    if (sys.indexers.length !== 1) return true
    return !b4a.equals(sys.indexers[0].key, key)
  }

  // triggered from apply
  async removeWriter (key) { // just compat for old version
    assert(this.applyView.applying, 'System changes are only allowed in apply')

    if (!this.removeable(key, this.applyView.system)) {
      throw new Error('Not allowed to remove the last indexer')
    }

    await this.applyView.system.remove(key)

    if (b4a.equals(key, this.local.key)) {
      if (this.isIndexer) this._unsetLocalIndexer()
    }

    const w = this.activeWriters.get(key)
    if (w) w.isRemoved = true

    this._queueBump()
  }

  _updateAckThreshold () {
    if (this._ackThreshold === 0) return
    if (this._ackTimer) this._ackTimer.bau()
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

async function closeAll (list) {
  for (const core of list) await core.close()
}
