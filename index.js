const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const hypercoreId = require('hypercore-id-encoding')
const assert = require('nanoassert')
const SignalPromise = require('signal-promise')

const Linearizer = require('./lib/linearizer')
const AutoStore = require('./lib/store')
const SystemView = require('./lib/system')
const messages = require('./lib/messages')
const Timer = require('./lib/timer')
const Writer = require('./lib/writer')
const ActiveWriters = require('./lib/active-writers')
const CorePool = require('./lib/core-pool')
const AutoWakeup = require('./lib/wakeup')
const mutexify = require('mutexify/promise')

const inspect = Symbol.for('nodejs.util.inspect.custom')

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

    this.encrypted = handlers.encrypted || !!handlers.encryptionKey
    this.encryptionKey = handlers.encryptionKey || null

    this._tryLoadingLocal = true
    this._primaryBootstrap = null
    if (this.bootstrap) {
      this._primaryBootstrap = this.store.get({ key: this.bootstrap, compat: false, encryptionKey: this.encryptionKey })
      this.store = this.store.namespace(this._primaryBootstrap, { detach: false })
    }

    this.local = null
    this.localWriter = null

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

    this._checkWriters = []
    this._appending = null
    this._wakeup = new AutoWakeup(this)
    this._wakeupHints = new Set()
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
    this._pendingRemoval = false
    this._completeRemovalAt = null
    this._systemPointer = 0

    this._updates = []
    this._handlers = handlers || {}
    this._warn = emitWarning.bind(this)

    this._draining = false
    this._advancing = null
    this._advanced = null
    this.reindexing = false

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

    this._initialSystem = null
    this._initialViews = null
    this._waiting = new SignalPromise()

    this.system = new SystemView(this._viewStore.get({ name: '_system', exclusive: true, cache: true }))
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
    return this.localWriter !== null
  }

  get key () {
    return this._primaryBootstrap === null ? this.local.key : this._primaryBootstrap.key
  }

  get discoveryKey () {
    return this._primaryBootstrap === null ? this.local.discoveryKey : this._primaryBootstrap.discoveryKey
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

  hintWakeup (keys) {
    if (!Array.isArray(keys)) keys = [keys]
    for (const key of keys) {
      this._wakeupHints.add(b4a.toString(key, 'hex'))
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
      this._primaryBootstrap = this.store.get({ key: ref, compat: false, encryptionKey: this.encryptionKey })
      this.store = this.store.namespace(this._primaryBootstrap, { detach: false })
    }

    await this.local.setUserData('referrer', this.key)

    if (this._primaryBootstrap) {
      await this._primaryBootstrap.ready()
      this._primaryBootstrap.setUserData('autobase/local', this.local.key)
      if (this.encryptionKey) await this._primaryBootstrap.setUserData('autobase/encryption', this.encryptionKey)
    } else {
      this.local.setUserData('autobase/local', this.local.key)
    }

    const { bootstrap, system } = await this._loadSystemInfo()

    this.version = system
      ? system.version
      : this.bootstrap && !b4a.equals(this.bootstrap, this.local.key)
        ? -1
        : this.maxSupportedVersion

    this.bootstrap = bootstrap
    this._initialSystem = system

    await this._makeLinearizer(system)
  }

  async _loadSystemInfo () {
    const pointer = await this.local.getUserData('autobase/boot')
    const bootstrap = this.bootstrap || (await this.local.getUserData('referrer')) || this.local.key
    if (!pointer) return { bootstrap, system: null }

    const { indexed, views } = c.decode(messages.BootRecord, pointer)
    const { key, length } = indexed

    this._systemPointer = length

    if (!length) return { bootstrap, system: null }

    const encryptionKey = AutoStore.getBlockKey(bootstrap, this.encryptionKey, '_system')
    const actualCore = this.store.get({ key, exclusive: false, cache: true, compat: false, encryptionKey, isBlockKey: true })

    await actualCore.ready()

    const core = actualCore.batch({ checkout: length, session: false })

    // safety check the batch is not corrupt
    if (length === 0 || !(await core.has(length - 1))) {
      await this.local.setUserData('autobase/boot', null)
      this._systemPointer = 0
      return { bootstrap, system: null }
    }

    const system = new SystemView(core, length)
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
      system
    }
  }

  async flush () {
    if (this.opened === false) await this.ready()
    await this._advancing
  }

  getSystemKey () {
    const core = this.system.core.getBackingCore()
    return core ? core.key : null
  }

  async _openPreBump () {
    this._presystem = this._openPreSystem()

    try {
      await this._presystem
      await this._viewStore.flush()
    } catch (err) {
      safetyCatch(err)
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
    const system = new SystemView(core, length)
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

    await this._wakeup.ready()

    this.system.requestWakeup()

    // queue a full bump that handles wakeup etc (not legal to wait for that here)
    this._queueBump()
    this._advanced = this._advancing

    if (this.reindexing) this._setReindexed()

    this.queueFastForward()
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
      if (this.closing) return
      await this.local.setUserData('autobase/reindexed', b4a.from([0]))
      this.reindexing = false
      this.emit('reindexed')
    } catch (err) {
      safetyCatch(err)
    }
  }

  async _close () {
    await Promise.resolve() // defer one tick
    this._waiting.notify(null)

    const closing = this._advancing.catch(safetyCatch)

    if (this._ackTimer) {
      this._ackTimer.stop()
      await this._ackTimer.flush()
    }

    await this._wakeup.close()

    if (this._hasClose) await this._handlers.close(this.view)
    if (this._primaryBootstrap) await this._primaryBootstrap.close()
    await this.corePool.clear()
    await this.store.close()
    await closing
  }

  _onError (err) {
    if (this.closing) return
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
      if (!unqueued || w.isIndexer || this.localWriter === w) continue

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
      if (this.closing) return
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
      if (!this.closing) throw err
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

    const isIndexer = this.localWriter.isIndexer || isPendingIndexer

    if (!isIndexer || this._acking || this.closing) return

    this._acking = true

    try {
      await this._bump()
    } catch (err) {
      if (!this.closing) throw err
    }

    // avoid lumping acks together due to the bump wait here
    if (this._ackTimer && bg) await this._ackTimer.asapStandalone()
    if (this.closing) return

    const unflushed = this._hasPendingCheckpoint || this.hasUnflushedIndexers()
    if (!this.closing && (isPendingIndexer || this.linearizer.shouldAck(this.localWriter, unflushed))) {
      try {
        await this.append(null)
      } catch (err) {
        if (!this.closing) throw err
      }

      if (!this.closing) {
        this._updateAckThreshold()
        this._bumpAckTimer()
      }
    }

    this._acking = false
  }

  async append (value) {
    if (!this.opened) await this.ready()
    if (this.closing) throw new Error('Autobase is closing')

    await this._advanced // ensure all local state has been applied, only needed until persistent batches

    // if a reset is scheduled await those
    while (this._queueViewReset && !this.closing) await this._bump()

    if (this.localWriter === null) {
      throw new Error('Not writable')
    }

    // make sure all local nodes are processed before continuing
    while (!this.closing && this.localWriter.core.length > this.localWriter.length) {
      await this.localWriter.waitForSynced()
      await this._bump() // make sure its all flushed...
    }

    if (this._appending === null) this._appending = []

    if (Array.isArray(value)) {
      for (const v of value) this._append(v)
    } else {
      this._append(value)
    }

    await this._bump()
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
    const opts = { ...handlers, compat: false, exclusive: true, valueEncoding: messages.OplogMessage, encryptionKey }
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

  async _getWriterByKey (key, len, seen, allowGC, system) {
    assert(this._draining === true || (this.opening && !this.opened))

    const release = await this._lock()

    if (this.closing) {
      release()
      throw new Error('Autobase is closing')
    }

    try {
      let w = this.activeWriters.get(key)
      if (w !== null) {
        w.seen(seen)
        return w
      }

      let isActive = true

      if (len === -1) {
        const sys = system || this.system
        const writerInfo = await sys.get(key)

        // TODO: this indirectly disables backwards-dag-walk - we should reenable when FF is enabled
        //       this is because the remote writer might not have our next heads in mem if it knows
        //       that following the indexed sets makes that redundant. tmp(?) solution for now is to
        //       just inflate the writers anyway - if FF is enabled simply jumping ahead is likely a better solution
        // if (writerInfo === null) return null
        // len = writerInfo.length

        if (!allowGC && writerInfo === null) return null
        len = writerInfo === null ? 0 : writerInfo.length
        isActive = writerInfo !== null && !writerInfo.isRemoved
      }

      w = this._makeWriter(key, len, isActive)
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
    for (const w of this.activeWriters) p.push(w.update().catch(this._warn))
    return Promise.all(p)
  }

  _makeWriterCore (key, isActive) {
    const pooled = this.corePool.get(key)
    if (pooled) {
      pooled.valueEncoding = messages.OplogMessage
      return pooled
    }

    const local = b4a.equals(key, this.local.key)
    if (local && !isActive) return null

    const core = local
      ? this.local.session({ valueEncoding: messages.OplogMessage, encryptionKey: this.encryptionKey })
      : this.store.get({ key, compat: false, writable: false, valueEncoding: messages.OplogMessage, encryptionKey: this.encryptionKey })

    return core
  }

  _makeWriter (key, length, isActive) {
    const core = this._makeWriterCore(key, isActive)
    if (!core) return null

    const w = new Writer(this, core, length)

    if (core.writable) {
      this.localWriter = w
      if (this._ackInterval) this._startAckTimer()
      this.emit('writable')
    } else {
      core.on('append', this._onremotewriterchangeBound)
      core.on('download', this._onremotewriterchangeBound)
      core.on('manifest', this._onremotewriterchangeBound)
    }

    return w
  }

  _updateLinearizer (indexers, heads) {
    this.linearizer = new Linearizer(indexers, { heads, writers: this.activeWriters })
    this._addCheckpoints = !!(this.localWriter && (this.localWriter.isIndexer || this._isPending()))
    this._updateAckThreshold()
  }

  _resumeWriter (w) {
    if (!this._isFastForwarding()) w.resume()
  }

  async _loadLocalWriter (sys) {
    if (this.localWriter !== null) return
    await this._getWriterByKey(this.local.key, -1, 0, false, sys)
    this._tryLoadingLocal = false
  }

  async _bootstrapLinearizer () {
    const bootstrap = this._makeWriter(this.bootstrap, 0, true)

    this.activeWriters.add(bootstrap)
    this._checkWriters.push(bootstrap)
    bootstrap.isIndexer = true
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
      const writer = await this._getWriterByKey(head.key, head.length, 0, false, sys)
      writer.isIndexer = true
      writer.inflateBackground()
      indexers.push(writer)
    }

    this._updateLinearizer(indexers, sys.heads)

    for (const { key, length } of sys.heads) {
      await this._getWriterByKey(key, length, 0, false, sys)
    }
  }

  async _reindex (nodes) {
    if (nodes && nodes.length) {
      this._undoAll()
      await this.system.update()
    }

    const sameIndexers = this.system.sameIndexers(this.linearizer.indexers)

    await this._makeLinearizer(this.system)
    if (!sameIndexers) await this._viewStore.migrate()

    this.version = this.system.version

    this.queueFastForward()

    if (nodes) {
      for (const node of nodes) node.reset()
      for (const node of nodes) this.linearizer.addHead(node)
    }

    if (!this._pendingRemoval) return

    this._pendingRemoval = false
    for (const idx of this.linearizer.indexers) {
      if (idx !== this.localWriter) continue
      this._pendingRemoval = true
      break
    }

    if (this._pendingRemoval) return // still pending

    this._addCheckpoints = false
    this.localWriter = null
  }

  _onUpgrade (version) {
    if (version > this.maxSupportedVersion) throw new Error('Autobase upgrade required')
  }

  _addLocalHeads () {
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

    const cores = this._viewStore.getIndexedCores()
    const views = new Array(cores.length - 1)
    for (const core of cores) {
      if (core.systemIndex === -1) continue
      views[core.systemIndex] = core.name
    }

    await this._setBootRecord(this.system.core.key, length, this.system.heads, views)
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
    while (!this.closing) {
      if (this.fastForwardTo !== null) {
        await this._applyFastForward()
        this.system.requestWakeup()
      }

      if (this.localWriter === null && this._tryLoadingLocal === true) {
        // in case we cleared system blocks we need to defer loading of the local writer
        await this._loadLocalWriter(this.system)
      }

      const remoteAdded = await this._addRemoteHeads()
      const localNodes = this._appending === null ? null : this._addLocalHeads()

      if (this.closing) return

      if (remoteAdded > 0 || localNodes !== null) {
        this.updating = true
      }

      const u = this.linearizer.update()
      const changed = u ? await this._applyUpdate(u) : null
      const indexed = !!this._updatingCores

      if (this.closing) return

      if (this.localWriter !== null && localNodes !== null) {
        await this._flushLocal(localNodes)
      }

      if (this.closing) return

      const flushed = (await this._flushIndexes()) ? this.system.core.getBackingCore().flushedLength : this._systemPointer
      if (this.updating || flushed > this._systemPointer) await this._advanceBootRecord(flushed)

      if (indexed) await this.onindex(this)

      if (this.closing) return

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
          continue // rerun the update loop as a writer might have been added
        }
        if (remoteAdded >= REMOTE_ADD_BATCH) continue
        break
      }

      await this._gcWriters()
      await this._reindex(changed)
    }
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

  async _drainWakeup () { // TODO: parallel load the writers here later
    if (this._needsWakeup === true) {
      this._needsWakeup = false

      for (const { key } of this._wakeup) {
        await this._getWriterByKey(key, -1, 0, true, null)
      }

      if (this._needsWakeupHeads === true) {
        this._needsWakeupHeads = false

        for (const { key } of await this._getLocallyStoredHeads()) {
          await this._getWriterByKey(key, -1, 0, true, null)
        }
      }
    }

    for (const hex of this._wakeupHints) {
      const key = b4a.from(hex, 'hex')
      await this._getWriterByKey(key, -1, 0, true, null)
    }

    this._wakeupHints.clear()
  }

  async _advance () {
    if (this.opened === false) await this.ready()

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

    if (!this.closing && this.localWriter && this._ackIsNeeded()) {
      if (this._ackTimer) this._ackTimer.asap()
      else this.ack()
    }

    if (this.updating === true) {
      this.updating = false
      this.emit('update')
      this._waiting.notify(null)
    }

    await this._gcWriters()
  }

  _ackIsNeeded () {
    if (!this._addCheckpoints) return false // ack has no impact

    if (this._pendingRemoval) return true

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

    await this._closeAllActiveWriters()

    await this.system.update()
    await this._makeLinearizer(this.system)
  }

  doneFastForwarding () {
    if (--this.fastForwarding === 0 && !this._isFastForwarding()) {
      for (const w of this.activeWriters) w.resume()
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

    if (!length) {
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

      const system = new SystemView(core.session(), length)
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

      const indexers = []
      const pendingViews = []

      for (const { key, length } of system.indexers) {
        if (length === 0) continue
        const core = this.store.get(key)
        await core.ready()
        indexers.push({ core, length })
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

      for (const { core, length } of indexers) {
        if (core.length === 0 && length > 0) promises.push(core.get(length - 1, { timeout }))
      }

      for (const { core, length } of pendingViews) {
        // we could just get the hash here, but likely user wants the block so yolo
        promises.push(core.get(length - 1, { timeout }))
      }

      await Promise.all(promises)

      if (localLookup) {
        const value = await localLookup
        if (value) info.localLength = value.length
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

    const system = new SystemView(core, length)
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
    const sysInfo = { key, length }

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

      views.set(view, v)
      view.systemIndex = i
    }

    await system.close()
    await this._closeAllActiveWriters()

    this._undoAll()

    for (const view of this._viewStore.opened.values()) {
      if (!views.has(view)) continue
      await view.catchup(views.get(view))
    }

    await this.system.update()

    if (this.localWriter) this.localWriter.reset(localLength)

    await this._makeLinearizer(this.system)
    await this._advanceBootRecord(length)

    // manually set the digest
    if (migrated) this._setDigest(key)

    if (b4a.equals(this.fastForwardTo.key, key) && this.fastForwardTo.length === length) {
      this._clearFastForward(false)
    }

    this.updating = true
    this.emit('fast-forward', length, from)

    // requeue in case we can do another jump!
    this.queueFastForward()

    await closeAll(opened)
  }

  async _closeAllActiveWriters () {
    for (const w of this.activeWriters) {
      if (this.localWriter === w) continue
      await this._closeWriter(w, true)
    }
    await this.corePool.clear()
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
    await this.system.add(key, { isIndexer, isPending: true })

    const writer = (await this._getWriterByKey(key, -1, 0, false, null)) || this._makeWriter(key, 0, true)
    await writer.ready()

    if (!this.activeWriters.has(key)) {
      this.activeWriters.add(writer)
      this._checkWriters.push(writer)
      this._resumeWriter(writer)
    }

    // If we are getting added as indexer, already start adding checkpoints while we get confirmed...
    if (writer === this.localWriter && isIndexer) this._addCheckpoints = true

    // fetch any nodes needed for dependents
    this._queueBump()
  }

  // triggered from apply
  async removeWriter (key) { // just compat for old version
    assert(this._applying !== null, 'System changes are only allowed in apply')
    await this.system.remove(key)

    if (b4a.equals(key, this.local.key)) {
      if (this._addCheckpoints) this._pendingRemoval = true
      else this.localWriter = null // immediately remove
    }

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
    await this.system.update()

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

      return u.indexed.slice(i).concat(u.tip)
    }

    for (i = u.shared; i < u.length; i++) {
      if (this.fastForwardTo !== null && this.fastForwardTo.length > this.system.core.length && b4a.equals(this.fastForwardTo.key, this.system.core.key)) {
        return null
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

      if (node.value !== null) {
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

      return u.indexed.slice(i + 1).concat(u.tip)
    }

    if (u.indexed.length) {
      this._queueIndexFlush(u.indexed.length)
      await this._updateDigest() // see above
    }

    return null
  }

  async _getViewInfo (indexerUpdate) {
    const indexers = []

    for (const { key, length } of this.system.indexers) {
      const indexer = await this._getWriterByKey(key, length, 0, false, null)
      indexers.push(indexer)
    }

    // construct view keys to be passed to system
    const views = []
    for (const view of this._viewStore.opened.values()) {
      if (!view.length || view._isSystem()) continue // system is omitted

      const length = view.systemIndex !== -1
        ? this.system.views[view.systemIndex].length
        : 0

      const key = indexers.length > 1 && indexerUpdate
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
      const w = await this._getWriterByKey(key, length, 0, false, null)

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
        p.push(await this._getWriterByKey(key, -1, 0, false, null))
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
