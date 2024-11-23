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
const AutoStore = require('./lib/store')
const SystemView = require('./lib/system')
const messages = require('./lib/messages')
const Timer = require('./lib/timer')
const Writer = require('./lib/writer')
const ActiveWriters = require('./lib/active-writers')
const CorePool = require('./lib/core-pool')
const AutoWakeup = require('./lib/wakeup')

const WakeupExtension = require('./lib/extension')

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
    this.encryptionKey = handlers.encryptionKey || null

    this._tryLoadingLocal = true
    this._primaryBootstrap = null
    if (this.bootstrap) {
      this._primaryBootstrap = this.store.get({ key: this.bootstrap, compat: false, active: false, encryptionKey: this.encryptionKey })
      this.wakeupExtension = new WakeupExtension(this, this._primaryBootstrap, true)
      this.store = this.store.namespace(this._primaryBootstrap, { detach: false })
    }

    this.local = null
    this.localWriter = null
    this.isIndexer = false

    this.activeWriters = new ActiveWriters()
    this.corePool = new CorePool()
    this.linearizer = null
    this.updating = false

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

    this._queueViewReset = false
    this._lock = mutexify()

    this._applying = null
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

    this.reindexing = false
    this.paused = false

    this._bump = debounceify(() => {
      this._advancing = this._advance()
      return this._advancing
    })

    this._onremotewriterchangeBound = this._onremotewriterchange.bind(this)

    this.maxSupportedVersion = AUTOBASE_VERSION // working version

    this._presystem = null
    this._prebump = null

    this._hasApply = !!this._handlers.apply
    this._hasOpen = !!this._handlers.open
    this._hasClose = !!this._handlers.close

    this.onindex = handlers.onindex || noop

    this._viewStore = new AutoStore(this)

    this.view = null
    this.system = null
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

    this._initialHeads = []
    this._initialSystem = null
    this._initialViews = null

    this._waiting = new SignalPromise()

    const sysCore = this._viewStore.get({ name: '_system', exclusive: true })

    this.system = new SystemView(sysCore, {
      checkout: 0
    })

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

  async _openPreSystem () {
    if (this._handlers.wait) await this._handlers.wait()
    await this.store.ready()

    const opts = {
      valueEncoding: this.valueEncoding,
      keyPair: this.keyPair,
      key: this._primaryBootstrap ? await this._primaryBootstrap.getUserData('autobase/local') : null
    }

    this.local = Autobase.getLocalCore(this.store, opts, this.encryptionKey)

    await this.local.ready()

    if (this.encryptionKey) {
      await this.local.setUserData('autobase/encryption', this.encryptionKey)
    } else {
      this.encryptionKey = await this.local.getUserData('autobase/encryption')
      if (this.encryptionKey) {
        await this.local.setEncryptionKey(this.encryptionKey)
        // not needed but, just for good meassure
        if (this._primaryBootstrap) this._primaryBootstrap.setEncryptionKey(this.encryptionKey)
      }
    }

    if (this.encrypted) {
      assert(this.encryptionKey !== null, 'Encryption key is expected')
    }

    // stateless open
    const ref = await this.local.getUserData('referrer')
    if (ref && !b4a.equals(ref, this.local.key) && !this._primaryBootstrap) {
      this._primaryBootstrap = this.store.get({ key: ref, compat: false, active: false, encryptionKey: this.encryptionKey })
      this.wakeupExtension = new WakeupExtension(this, this._primaryBootstrap, true)
      this.store = this.store.namespace(this._primaryBootstrap, { detach: false })
    }

    await this.local.setUserData('referrer', this.key)

    if (this._primaryBootstrap) {
      await this._primaryBootstrap.ready()
      this._primaryBootstrap.setUserData('autobase/local', this.local.key)
      if (this.encryptionKey) await this._primaryBootstrap.setUserData('autobase/encryption', this.encryptionKey)
    } else {
      this.local.setUserData('autobase/local', this.local.key)
      this.wakeupExtension = new WakeupExtension(this, this.local, true)
    }

    const { bootstrap, system, heads } = await this._loadSystemInfo()

    this.version = system
      ? system.version
      : this.bootstrap && !b4a.equals(this.bootstrap, this.local.key)
        ? -1
        : this.maxSupportedVersion

    this.bootstrap = bootstrap

    this._initialSystem = system
    this._initialHeads = heads

    await this._makeLinearizer(system)
  }

  async _loadSystemInfo () {
    const pointer = await this.local.getUserData('autobase/boot')
    const bootstrap = this.bootstrap || (await this.local.getUserData('referrer')) || this.local.key
    if (!pointer) return { bootstrap, system: null, heads: [] }

    const { indexed, views, heads } = c.decode(messages.BootRecord, pointer)
    const { key, length } = indexed

    this._systemPointer = length

    if (!length) return { bootstrap, system: null, heads: [] }

    const encryptionKey = AutoStore.getBlockKey(bootstrap, this.encryptionKey, '_system')
    const actualCore = this.store.get({ key, exclusive: false, compat: false, encryptionKey, isBlockKey: true })

    await actualCore.ready()

    const core = actualCore.batch({ checkout: length, session: false })

    // safety check the batch is not corrupt
    if (length === 0 || !(await core.has(length - 1))) {
      await this.local.setUserData('autobase/boot', null)
      this._systemPointer = 0
      return { bootstrap, system: null, heads: [] }
    }

    const system = new SystemView(core, {
      checkout: length
    })

    await system.ready()

    if (system.version > this.maxSupportedVersion) {
      throw new Error('Autobase upgrade required')
    }

    this._initialViews = [{ name: '_system', key, length }]

    for (let i = 0; i < system.views.length; i++) {
      this._initialViews.push({ name: views[i], ...system.views[i] })
    }

    return {
      bootstrap,
      system,
      heads
    }
  }

  interrupt (reason) {
    assert(this._applying !== null, 'Interrupt is only allowed in apply')
    this._interrupting = true
    if (reason) this.interrupted = reason
    throw INTERRUPT
  }

  async flush () {
    if (this.opened === false) await this.ready()
    await this._advancing
  }

  getSystemKey () {
    const core = this.system.core.getBackingCore()
    return core ? core.key : null
  }

  recouple () {
    if (this._coupler) this._coupler.destroy()
    const core = this.system.core.getBackingCore()
    this._coupler = new CoreCoupler(core.session, this._wakeupPeerBound)
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

  async _openPreBump () {
    this._presystem = this._openPreSystem()

    try {
      await this._presystem
      await this._viewStore.flush()
    } catch (err) {
      safetyCatch(err)
      if (err.code === 'ELOCKED') throw err
      await this.local.setUserData('autobase/last-error', b4a.from(err.stack + ''))
      await this.local.setUserData('autobase/boot', null)
      this.store.close().catch(safetyCatch)
      throw err
    }

    // see if we can load from indexer checkpoint
    await this.system.ready()

    if (this._initialSystem) {
      await this._initialSystem.close()
      this._initialSystem = null
      this._initialViews = null
    }

    // check if this is a v0 base
    const record = await this.local.getUserData('autobase/system')
    if (record !== null && (await this.local.getUserData('autobase/reindexed')) === null) {
      this.reindexing = true
      this.emit('reindexing')
      this._onreindexing(record).catch(safetyCatch)
    }

    // load previous digest if available
    if (this.localWriter && !this.system.bootstrapping) {
      await this._restoreLocalState()
    }

    this.recouple()

    if (this.fastForwardTo !== null) {
      const { key, timeout } = this.fastForwardTo
      this.fastForwardTo = null // will get reset once ready
      this.initialFastForward(key, timeout || DEFAULT_FF_TIMEOUT * 2)
    }

    if (this.localWriter && this._ackInterval) this._startAckTimer()
  }

  async _onreindexing (record) {
    const { key, length } = messages.Checkout.decode({ buffer: record, start: 0, end: record.byteLength })
    const encryptionKey = this._viewStore.getBlockKey(this._viewStore.getSystemCore().name)
    const core = this.store.get({ key, encryptionKey, isBlockKey: true }).batch({ checkout: length, session: false })

    const base = this
    const system = new SystemView(core, {
      checkout: length
    })

    await system.ready()

    const indexerCores = []
    for (const { key } of system.indexers) {
      const core = this.store.get({ key, compat: false, valueEncoding: messages.OplogMessage, encryptionKey: this.encryptionKey })
      indexerCores.push(core)
    }

    await system.close()

    for (const core of indexerCores) tail(core).catch(safetyCatch)

    async function onsyskey (key) {
      for (const core of indexerCores) await core.close()
      if (key === null || !base.reindexing || base._isFastForwarding()) return
      base.initialFastForward(key, DEFAULT_FF_TIMEOUT * 2)
    }

    async function tail (core) {
      await core.ready()

      while (base.reindexing && !base._isFastForwarding()) {
        const seq = core.length - 1
        const blk = seq >= 0 ? await core.get(seq) : null
        if (blk && blk.version >= 1) {
          const sysKey = await getSystemKey(core, seq, blk)
          if (sysKey) return onsyskey(sysKey)
        }

        await core.get(core.length) // force get next blk
      }

      return onsyskey(null)
    }

    async function getSystemKey (core, seq, blk) {
      if (!blk.digest) return null
      if (blk.digest.key) return blk.digest.key
      const p = await core.get(seq - blk.digest.pointer)
      return p.digest && p.digest.key
    }
  }

  async _restoreLocalState () {
    const version = await this.localWriter.getVersion()
    if (version > this.maxSupportedVersion) {
      this.store.close().catch(safetyCatch)
      throw new Error('Autobase version cannot be downgraded')
    }

    await this._updateDigest()
  }

  async _open () {
    this._prebump = this._openPreBump()
    await this._prebump

    await this._catchup(this._initialHeads)

    await this._wakeup.ready()

    this.system.requestWakeup()

    // queue a full bump that handles wakeup etc (not legal to wait for that here)
    this._queueBump()
    this._advanced = this._advancing

    if (this.reindexing) this._setReindexed()

    this.queueFastForward()
    this._updateBootstrapWriters()
  }

  async _catchup (nodes) {
    if (!nodes.length) return

    const visited = new Set()
    const writers = new Map()

    while (nodes.length) {
      const { key, length } = nodes.pop()

      const hex = b4a.toString(key, 'hex')
      const ref = hex + ':' + length

      if (visited.has(ref)) continue
      visited.add(ref)

      let w = writers.get(hex)
      if (!w) {
        const writer = await this._getWriterByKey(key, -1, 0, true, false, null)

        w = { writer, end: writer.length }

        writers.set(hex, w)
      }

      if (w.writer.length >= length) continue

      if (length > w.end) w.end = length

      // we should have all nodes locally
      const block = await w.writer.core.get(length - 1, { wait: false })

      assert(block !== null, 'Catchup failed: local block not available')

      for (const dep of block.node.heads) {
        nodes.push(dep)
      }
    }

    while (writers.size) {
      for (const [hex, info] of writers) {
        const { writer, end } = info

        if (writer === null || writer.length === end) {
          writers.delete(hex)
          continue
        }

        if (writer.available <= writer.length) {
          // force in case they are not indexed yet
          await writer.update(true)
        }

        const node = writer.advance()
        if (!node) continue

        this.linearizer.addHead(node)
      }
    }

    await this._drain() // runs for one tick
  }

  _reindexersIdle () {
    for (const idx of this.linearizer.indexers) {
      if (idx.core.length !== idx.length) return false
    }
    return !this.localWriter || this.localWriter.core.length === this.localWriter.length
  }

  async _setReindexed () {
    try {
      while (true) {
        await this._bump()

        let p = this.progress()
        if (p.processed === p.total && !(this.linearizer.indexers.length === 1 && this.linearizer.indexers[0].core.length === 0)) break

        await this._waiting.wait(2000)
        await this._advancing

        p = this.progress()
        if (p.processed === p.total) break

        if (this._reindexersIdle()) break
      }
      if (this._interrupting) return
      await this.local.setUserData('autobase/reindexed', b4a.from([0]))
      this.reindexing = false
      this.emit('reindexed')
    } catch (err) {
      safetyCatch(err)
    }
  }

  async _close () {
    this._interrupting = true
    await Promise.resolve() // defer one tick

    if (this._coupler) this._coupler.destroy()
    this._coupler = null
    this._waiting.notify(null)

    const closing = this._advancing.catch(safetyCatch)

    if (this._ackTimer) {
      this._ackTimer.stop()
      await this._ackTimer.flush()
    }

    await this._wakeup.close()

    if (this._hasClose) await this._handlers.close(this.view)
    if (this._primaryBootstrap) await this._primaryBootstrap.close()
    await this.activeWriters.clear()
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
      this._coupler.remove(w.core)

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
    let p = this.progress()
    while (!this.closing && this.reindexing) {
      if (p.processed === p.total && !(this.linearizer.indexers.length === 1 && this.linearizer.indexers[0].core.length === 0)) break
      await this._waiting.wait(2000)
      await this._advancing
      const next = this.progress()
      if (next.processed === p.processed && next.total === p.total) break
      p = next
    }

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

    // if a reset is scheduled await those
    while (this._queueViewReset && !this._interrupting) await this._bump()

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

  async checkpoint () {
    await this.ready()
    const all = []

    for (const w of this.activeWriters) {
      all.push(w.getCheckpoint())
    }

    const checkpoints = await Promise.all(all)
    let best = null

    for (const c of checkpoints) {
      if (!c) continue
      if (best === null || c.length > best.length) best = c
    }

    return best
  }

  static getLocalCore (store, handlers, encryptionKey) {
    const opts = { ...handlers, compat: false, active: false, exclusive: true, valueEncoding: messages.OplogMessage, encryptionKey }
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
    await this._presystem
    const core = this._primaryBootstrap === null ? this.local : this._primaryBootstrap

    await core.setUserData(key, val)
  }

  async getUserData (key) {
    await this._presystem
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
        if (isAdded && w.core.writable && this.localWriter === null) this._setLocalWriter(w)
        if (w.isRemoved && isAdded) w.isRemoved = false
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
    const pooled = this.corePool.get(key)
    if (pooled) {
      pooled.valueEncoding = messages.OplogMessage
      return pooled
    }

    const local = b4a.equals(key, this.local.key)

    const core = local
      ? this.local.session({ valueEncoding: messages.OplogMessage, encryptionKey: this.encryptionKey, active: false })
      : this.store.get({ key, compat: false, writable: false, valueEncoding: messages.OplogMessage, encryptionKey: this.encryptionKey, active: false })

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
    for (const writer of indexers) writer.isActiveIndexer = true

    if (this._isActiveIndexer() && !wasActiveIndexer) {
      this._setLocalIndexer()
    } else if (!this._isActiveIndexer() && wasActiveIndexer) {
      this._unsetLocalIndexer()
      this._clearLocalIndexer()
    }

    this.linearizer = new Linearizer(indexers, { heads, writers: this.activeWriters })
    this._addCheckpoints = !!(this.localWriter && (this.localWriter.isActiveIndexer || this._isPending()))
    this._updateAckThreshold()
  }

  _resumeWriter (w) {
    if (!this._isFastForwarding()) w.resume()
  }

  async _loadLocalWriter (sys) {
    if (this.localWriter !== null) return
    await this._getWriterByKey(this.local.key, -1, 0, false, false, sys)
    this._tryLoadingLocal = false
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
    this._tryLoadingLocal = true

    if (sys === null) {
      return this._bootstrapLinearizer()
    }

    if (this.opened || await sys.hasLocal(this.local.key)) {
      await this._loadLocalWriter(sys)
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

  async _refreshSystemState () {
    if (!(await this.system.update())) return

    for (const w of this.activeWriters) {
      const data = await this.system.get(w.core.key)
      w.isRemoved = data ? data.isRemoved : false
    }
  }

  async _reindex () {
    if (this._updates.length) {
      this._undoAll()
      await this._refreshSystemState()
    }

    const sameIndexers = this.system.sameIndexers(this.linearizer.indexers)

    await this._makeLinearizer(this.system)
    if (!sameIndexers) await this._viewStore.migrate()

    this.version = this.system.version

    this.queueFastForward()

    for (const w of this.activeWriters) {
      const value = await this.system.get(w.core.key)
      const length = value ? value.length : 0
      w.reset(length)
      this._resumeWriter(w)
    }
  }

  _onUpgrade (version) {
    if (version > this.maxSupportedVersion) throw new Error('Autobase upgrade required')
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
    this._addCheckpoints = true // unset once indexer is cleared
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

    this.localWriter.isActiveIndexer = false

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

  async _advanceBootRecord (length) {
    if (length) { // TODO: remove when we are 100% we never hit the return in this if
      const { views } = await this.system.getIndexedInfo(length)
      for (const { key, length } of views) {
        const view = this._viewStore.getByKey(key)
        if (!view || (view.core.flushedLength < length)) {
          // TODO: this fires in some FF scenarios cause the core above is another core, should be fine
          return
        }
      }
    }

    this._systemPointer = length

    const views = this._viewStore.indexedViewsByName()

    await this._setBootRecord(this.system.core.key, length, this.system.heads, views)
  }

  async _updateBootRecordHeads (heads) {
    if (this._systemPointer === 0) return // first tick

    const views = this._viewStore.indexedViewsByName()

    await this._setBootRecord(this.system.core.key, this._systemPointer, heads, views)
  }

  async _setBootRecord (key, length, heads, views) {
    const pointer = c.encode(messages.BootRecord, {
      indexed: { key, length },
      heads,
      views
    })

    await this.local.setUserData('autobase/boot', pointer)
  }

  async _drain () {
    const writable = this.writable

    while (!this._interrupting && !this.paused) {
      if (this.opened && this.fastForwardTo !== null) {
        await this._applyFastForward()
        this.system.requestWakeup()
      }

      if (this.localWriter === null && this._tryLoadingLocal === true) {
        // in case we cleared system blocks we need to defer loading of the local writer
        await this._loadLocalWriter(this.system)
      }

      const remoteAdded = this.opened ? await this._addRemoteHeads() : null
      const localNodes = this.opened && this._appending !== null ? this._addLocalHeads() : null

      if (this._maybeStaticFastForward === true && this.fastForwardEnabled === true) await this._checkStaticFastForward()
      if (this._interrupting) return

      if (remoteAdded > 0 || localNodes !== null) {
        this.updating = true
      }

      const u = this.linearizer.update()
      const changed = u ? await this._applyUpdate(u) : null
      const indexed = !!this._updatingCores

      if (this._interrupting) return

      if (this.localWriter !== null && localNodes !== null) {
        await this._flushLocal(localNodes)
      }

      if (this.opened) await this._updateBootRecordHeads(this.system.heads)

      if (this._interrupting) return

      const flushed = (await this._flushIndexes()) ? this.system.core.getBackingCore().flushedLength : this._systemPointer
      if (this.updating || flushed > this._systemPointer) await this._advanceBootRecord(flushed)

      if (indexed) await this.onindex(this)

      if (this._interrupting) return

      // force reset state in worst case
      if (this._queueViewReset && this._appending === null) {
        this._queueViewReset = false
        const sysCore = this.system.core.getBackingCore()
        await this._forceResetViews(sysCore.indexedLength)
        continue
      }

      if (!changed) {
        if (this._checkWriters.length > 0) {
          await this._gcWriters()
          if (!this.opened) break // at most one tick preready
          continue // rerun the update loop as a writer might have been added
        }
        if (remoteAdded >= REMOTE_ADD_BATCH) continue
        break
      }

      await this._gcWriters()
      await this._reindex()
    }

    // emit state changes post drain
    if (writable !== this.writable) this.emit(writable ? 'unwritable' : 'writable')
  }

  progress () {
    let processed = 0
    let total = 0

    for (const w of this.activeWriters) {
      processed += w.length
      total += w.core.length
    }

    return {
      processed,
      total
    }
  }

  async _getLocallyStoredHeads () {
    const buffer = await this.local.getUserData('autobase/boot')
    if (!buffer) return []
    return c.decode(messages.BootRecord, buffer).heads
  }

  _wakeupPeer (peer) {
    this.system.sendWakeup(peer.remotePublicKey)
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

        for (const { key } of await this._getLocallyStoredHeads()) {
          await this._wakeupWriter(key)
        }
      }
    }

    for (const [hex, length] of this._wakeupHints) {
      const key = b4a.from(hex, 'hex')
      if (length !== -1) {
        const info = await this.system.get(key)
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
    if (!this._addCheckpoints) return false // ack has no impact

    // flush any pending indexers
    if (this.system.pendingIndexers.length > 0) {
      for (const key of this.system.pendingIndexers) {
        if (b4a.equals(key, this.local.key) && !b4a.equals(key, this.bootstrap)) {
          return true
        }
      }
    }

    // flush any pending migrates
    for (const view of this._viewStore.opened.values()) {
      if (view.queued === -1) continue

      const checkpoint = view.signer.bestCheckpoint(this.localWriter)
      const length = checkpoint ? checkpoint.length : 0

      if (length < view.queued && length < view.indexedLength) {
        this._hasPendingCheckpoint = true
        return true
      }
    }

    // flush if threshold is reached and we are not already acking
    if (this._ackTickThreshold && !this._acking && this._ackTick >= this._ackTickThreshold) {
      if (this._ackTimer) { // the bool in this case is implicitly an "asap" signal
        for (const w of this.linearizer.indexers) {
          if (w.core.length > w.length) return false // wait for the normal ack cycle in this case
        }

        return this.linearizer.shouldAck(this.localWriter, this.hasUnflushedIndexers())
      }

      return true
    }

    return false
  }

  async forceResetViews () {
    if (!this.opened) await this.ready()

    this._queueViewReset = true
    this._queueBump()
    this._advanced = this._advancing
    await this._advanced
  }

  async _forceResetViews (length) {
    const info = await this.system.getIndexedInfo(length)

    this._undoAll()
    this._systemPointer = length

    const pointer = await this.local.getUserData('autobase/boot')
    const { views } = c.decode(messages.BootRecord, pointer)

    await this._setBootRecord(this.system.core.key, length, info.heads, views)

    for (const { key, length } of info.views) {
      const core = this._viewStore.getByKey(key)
      await core.reset(length)
    }

    await this._closeAllActiveWriters(false)

    await this._refreshSystemState()
    await this._makeLinearizer(this.system)
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

    const encryptionKey = this._viewStore.getBlockKey(this._viewStore.getSystemCore().name)

    const core = this.store.get({ key, encryptionKey, isBlockKey: true })
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

    const target = await this._preFastForward(core, length, timeout)
    await core.close()

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

    const core = this.system.core.getBackingCore()

    if (core.session.length <= core.length + FF_THRESHOLD) return
    if (this.fastForwardTo !== null && core.session.length <= this.fastForwardTo.length + FF_THRESHOLD) return
    if (!core.session.length) return

    this.fastForwarding++
    const target = await this._preFastForward(core.session, core.session.length, DEFAULT_FF_TIMEOUT)

    // fast-forward failed
    if (target === null) {
      this.doneFastForwarding()
      return
    }

    // if it migrated underneath us, ignore for now
    if (core !== this.system.core.getBackingCore()) {
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

    try {
      // sys runs open with wait false, so get head block first for low complexity
      if (!(await core.has(length - 1))) {
        await core.get(length - 1, { timeout })
      }

      const system = new SystemView(core.session(), {
        checkout: length,
        maxCacheSize: this.maxCacheSize
      })

      await system.ready()

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

      const promises = []

      for (const { key, core, length } of indexers) {
        if (core.length === 0 && length > 0) promises.push(core.get(length - 1, { timeout }))
        promises.push(system.get(key))
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

      // handle system migration
      if (systemShouldMigrate) {
        const hash = system.core.core.tree.hash()
        const name = this.system.core._source.name
        const prologue = { hash, length }

        info.key = this.deriveKey(name, indexers, prologue)

        const core = this.store.get(info.key)
        await core.get(length - 1, { timeout })

        closing.push(core.close())
      }

      for (const { core } of pendingViews) {
        closing.push(core.close())
      }

      closing.push(system.close())

      await Promise.allSettled(closing)
    } catch (err) {
      safetyCatch(err)
      return null
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
    const encryptionKey = this._viewStore.getBlockKey(name)

    const core = this.store.get({ key, encryptionKey, isBlockKey: true })
    await core.ready()

    const from = this.system.core.getBackingCore().length

    // just extra sanity check that we are not going back in time, nor that we cleared the storage needed for ff
    if (from >= length || core.length < length) {
      this._clearFastForward(true)
      return
    }

    const system = new SystemView(core, {
      checkout: length,
      maxCacheSize: this.maxCacheSize
    })

    await system.ready()

    const opened = []
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

    const sysView = this.system.core._source
    const sysInfo = { key, length, systemIndex: -1 }

    views.set(sysView, sysInfo)

    for (let i = 0; i < system.views.length; i++) {
      const v = system.views[i]

      // TODO: check behaviour if new view keys (+ double FF)
      let view = this._viewStore.getByKey(v.key)

      // search for corresponding view
      if (!view) {
        for (view of this._viewStore.opened.values()) {
          const key = this.deriveKey(view.name, indexers, prologues[i])
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

      views.set(view, { key: v.key, length: v.length, systemIndex: i })
    }

    await system.close()
    await this._closeAllActiveWriters(false)

    this._undoAll()

    for (const view of this._viewStore.opened.values()) {
      const info = views.get(view)
      if (info) await view.catchup(info)
      else if (migrated) await view.migrateTo(indexers, 0)
    }

    await this._refreshSystemState()

    if (this.localWriter) {
      if (localLength < 0) this._unsetLocalWriter()
      else this.localWriter.reset(localLength)
    }

    await this._makeLinearizer(this.system)
    await this._advanceBootRecord(length)

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

  async _flushIndexes () {
    let complete = true
    this._updatingCores = false

    for (const core of this._viewStore.opened.values()) {
      if (!await core.flush()) complete = false
    }

    // updates emitted sync
    for (const core of this._viewStore.opened.values()) {
      if (core.indexing === 0) continue
      const indexing = core.indexing
      core.indexing = 0
      core._onindex(indexing)
    }

    return complete
  }

  // triggered from linearized core
  _onviewappend (core, blocks) {
    assert(this._applying !== null, 'Append is only allowed in apply')

    if (core.appending === 0) {
      this._applying.views.push({ core, appending: 0 })
    }

    core.appending += blocks
  }

  // triggered from apply
  async addWriter (key, { indexer = true, isIndexer = indexer } = {}) { // just compat for old version
    assert(this._applying !== null, 'System changes are only allowed in apply')
    await this.system.add(key, { isIndexer })

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

  removeable (key) {
    if (this.system.indexers.length !== 1) return true
    return !b4a.equals(this.system.indexers[0].key, key)
  }

  // triggered from apply
  async removeWriter (key) { // just compat for old version
    assert(this._applying !== null, 'System changes are only allowed in apply')

    if (!this.removeable(key)) {
      throw new Error('Not allowed to remove the last indexer')
    }

    await this.system.remove(key)

    if (b4a.equals(key, this.local.key)) {
      if (this.isIndexer) this._unsetLocalIndexer()
    }

    const w = this.activeWriters.get(key)
    if (w) w.isRemoved = true

    this._queueBump()
  }

  _undoAll () {
    let count = 0
    for (const u of this._updates) {
      count += u.batch
    }
    return this._undo(count)
  }

  _undo (popped) {
    const truncating = []

    while (popped > 0) {
      const u = this._updates.pop()

      popped -= u.batch

      for (const { core, appending } of u.views) {
        if (core.truncating === 0) truncating.push(core)
        core.truncating += appending
      }
    }

    for (const core of truncating) {
      const truncating = core.truncating
      core.truncating = 0
      core._onundo(truncating)
    }
  }

  async _getManifest (indexer, len) {
    for (const w of this.linearizer.indexers) {
      const d = await w.getDigest(len)
      if (!d) continue
      if (d.indexers.length > indexer) return d.indexers[indexer]
    }

    return null
  }

  _bootstrap () {
    return this.system.add(this.bootstrap, { isIndexer: true, isPending: false })
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

  async _applyUpdate (u) {
    assert(await this._viewStore.flush(), 'Views failed to open')

    if (u.undo) this._undo(u.undo)

    // if anything was indexed reset the ticks
    if (u.indexed.length) this._resetAckTick()

    // make sure the latest changes is reflected on the system...
    await this._refreshSystemState()

    // todo: refresh the active writer set in case any were removed

    let batch = 0
    let applyBatch = []
    let versionUpgrade = false

    let j = 0

    let i = 0
    while (i < Math.min(u.indexed.length, u.shared)) {
      const node = u.indexed[i++]

      if (node.batch > 1) continue
      this._shiftWriter(node.writer)

      const update = this._updates[j++]

      // autobase version was bumped
      let upgraded = false
      if (update.version > this.version) {
        this._onUpgrade(update.version) // throws if not supported
        upgraded = true
      }

      if (!update.indexers && !upgraded) continue

      this._queueIndexFlush(i)

      // we have to set the digest here so it is
      // flushed to local appends in same iteration
      await this._updateDigest()

      return true
    }

    for (i = u.shared; i < u.length; i++) {
      if (this.fastForwardTo !== null && this.fastForwardTo.length > this.system.core.length && b4a.equals(this.fastForwardTo.key, this.system.core.key)) {
        return false
      }

      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      if (node.version > this.system.version) versionUpgrade = true

      if (node.writer === this.localWriter) {
        this._resetAckTick()
      } else if (!indexed) {
        this._ackTick++
      }

      batch++

      this.system.addHead(node)

      if (node.value !== null && !node.writer.isRemoved) {
        applyBatch.push({
          indexed,
          from: node.writer.core,
          length: node.length,
          value: node.value,
          heads: node.actualHeads
        })
      }

      if (node.batch > 1) continue

      if (versionUpgrade) {
        const version = await this._checkVersion()
        this.system.version = version === -1 ? node.version : version
      }

      const update = {
        batch,
        indexers: false,
        views: [],
        version: this.system.version
      }

      this._updates.push(update)
      this._applying = update

      if (this.system.bootstrapping) await this._bootstrap()

      if (applyBatch.length && this._hasApply === true) {
        await this._handlers.apply(applyBatch, this.view, this)
      }

      update.indexers = !!this.system.indexerUpdate

      await this.system.flush(await this._getViewInfo(update.indexers))

      this._applying = null

      batch = 0
      applyBatch = []

      for (let k = 0; k < update.views.length; k++) {
        const u = update.views[k]
        u.appending = u.core.appending
        u.core.appending = 0
      }

      if (!indexed) continue

      this._shiftWriter(node.writer)

      // autobase version was bumped
      let upgraded = false
      if (update.version > this.version) {
        this._onUpgrade(update.version) // throws if not supported
        upgraded = true
      }

      if (!update.indexers && !upgraded) continue

      // indexer set has updated
      this._queueIndexFlush(i + 1)
      await this._updateDigest() // see above

      return true
    }

    if (u.indexed.length) {
      this._queueIndexFlush(u.indexed.length)
      await this._updateDigest() // see above
    }

    return false
  }

  async _getViewInfo (indexerUpdate) {
    const indexers = []

    for (const { key, length } of this.system.indexers) {
      const indexer = await this._getWriterByKey(key, length, 0, false, false, null)
      indexers.push(indexer)
    }

    // construct view keys to be passed to system
    const views = []
    for (const view of this._viewStore.opened.values()) {
      if (!view.length || view._isSystem()) continue // system is omitted

      const length = view.systemIndex !== -1
        ? this.system.views[view.systemIndex].length
        : 0

      // TODO: the first part of this condition could be make clearer with a !this._isBootstrapping() condition instead
      const key = (indexers.length > 1 || this.linearizer.indexers.length > indexers.length) && indexerUpdate
        ? await view.deriveKey(indexers, length + view.appending)
        : view.systemIndex === -1
          ? view.key
          : null

      views.push({ view, key })
    }

    return views
  }

  async _checkVersion () {
    if (!this.system.indexers.length) return -1

    const maj = (this.system.indexers.length >> 1) + 1

    const fetch = []

    let localUnflushed = false
    for (const { key, length } of this.system.indexers) {
      const w = await this._getWriterByKey(key, length, 0, false, false, null)

      if (length > w.core.length) localUnflushed = true // local writer has nodes in mem
      else fetch.push(w.core.get(length - 1))
    }

    const heads = await Promise.all(fetch)

    const tally = new Map()
    const versions = []

    // count ourself
    if (localUnflushed) {
      const local = { version: this.maxSupportedVersion, n: 1 }
      versions.push(local)
      tally.set(this.maxSupportedVersion, local)
    }

    for (const { maxSupportedVersion: version } of heads) {
      let v = tally.get(version)

      if (!v) {
        v = { version, n: 0 }

        tally.set(version, v)
        versions.push(v)
      }

      if (++v.n >= maj) return version
    }

    let count = 0
    for (const { version, n } of versions.sort(descendingVersion)) {
      if ((count += n) >= maj) return version
    }

    assert(false, 'Failed to determine version')
  }

  _shiftWriter (w) {
    w.shift()
    if (w.flushed()) this._checkWriters.push(w)
  }

  _queueIndexFlush (indexed) {
    assert(this._updatingCores === false, 'Updated cores not flushed')
    this._updatingCores = true

    while (indexed > 0) {
      const u = this._updates.shift()

      indexed -= u.batch

      for (const { core, appending } of u.views) {
        core.indexing += appending
      }
    }
  }

  deriveKey (name, indexers, prologue) {
    return this._viewStore.deriveKey(name, indexers, prologue)
  }

  async _updateDigest () {
    if (!this._addCheckpoints) return

    if (this._localDigest === null) {
      this._localDigest = await this.localWriter.getDigest()

      // no previous digest available
      if (this._localDigest === null) {
        this._setDigest(this.system.core.key)
      }

      return
    }

    // we predict what the system key will be after flushing
    const pending = this.system.core._source.pendingIndexedLength
    const info = await this.system.getIndexedInfo(pending)

    let same = info.indexers.length === this.linearizer.indexers.length
    if (same) {
      for (let i = 0; i < info.indexers.length; i++) {
        if (!b4a.equals(info.indexers[i].key, this.linearizer.indexers[i].core.key)) {
          same = false
          break
        }
      }
    }

    let key = this.system.core.key

    if (!same) {
      const p = []
      for (const { key } of info.indexers) {
        p.push(await this._getWriterByKey(key, -1, 0, false, false, null))
      }

      const indexers = await p
      const sys = this._viewStore.getSystemCore()
      key = await sys.deriveKey(indexers, pending)
    }

    if (this._localDigest.key && b4a.equals(key, this._localDigest.key)) return

    this._setDigest(key)
  }

  _setDigest (key) {
    if (this._localDigest === null) this._localDigest = {}
    this._localDigest.key = key
    this._localDigest.pointer = 0
  }

  _generateDigest () {
    return {
      pointer: this._localDigest.pointer,
      key: this._localDigest.key
    }
  }

  async _generateCheckpoint (cores) {
    if (!this._addCheckpoints) return null

    if (this._firstCheckpoint) {
      this._firstCheckpoint = false
      // TODO: unsafe, use an array instead for views as the order is important
      return generateCheckpoint(this._viewStore.opened.values())
    }

    return generateCheckpoint(cores)
  }

  async _flushLocal (localNodes) {
    if (!this._localDigest) await this._updateDigest()

    const cores = this._addCheckpoints ? this._viewStore.getIndexedCores() : []
    const blocks = new Array(localNodes.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch } = localNodes[i]

      blocks[i] = {
        version: 1,
        maxSupportedVersion: this.maxSupportedVersion,
        checkpoint: this._addCheckpoints ? await generateCheckpoint(cores) : null,
        digest: this._addCheckpoints ? this._generateDigest() : null,
        node: {
          heads,
          batch,
          value: value === null ? null : c.encode(this.valueEncoding, value)
        },
        trace: []
      }

      if (this._addCheckpoints) this._localDigest.pointer++
    }

    await this.local.append(blocks)

    if (this._addCheckpoints) {
      const { checkpoint } = blocks[blocks.length - 1]
      this.localWriter._addCheckpoints(checkpoint)
      this._hasPendingCheckpoint = false
    }
  }
}

function generateCheckpoint (cores) {
  const checkpoint = []

  for (const core of cores) {
    checkpoint.push(core.checkpoint())
    core.checkpointer++
  }

  return Promise.all(checkpoint)
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

function descendingVersion (a, b) {
  return b.version - a.version
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
