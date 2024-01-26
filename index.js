const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const hypercoreId = require('hypercore-id-encoding')
const assert = require('nanoassert')

const Linearizer = require('./lib/linearizer')
const AutoStore = require('./lib/store')
const SystemView = require('./lib/system')
const messages = require('./lib/messages')
const Timer = require('./lib/timer')
const Writer = require('./lib/writer')
const ActiveWriters = require('./lib/active-writers')
const AutoWakeup = require('./lib/wakeup')

const inspect = Symbol.for('nodejs.util.inspect.custom')

// default is to automatically ack
const DEFAULT_ACK_INTERVAL = 10 * 1000
const DEFAULT_ACK_THRESHOLD = 4

const FF_THRESHOLD = 16

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
    this.valueEncoding = c.from(handlers.valueEncoding || 'binary')
    this.store = store
    this.encryptionKey = handlers.encryptionKey || null
    this._primaryBootstrap = null

    if (this.bootstrap) {
      this._primaryBootstrap = this.store.get({ key: this.bootstrap, compat: false, encryptionKey: this.encryptionKey })
      this.store = this.store.namespace(this._primaryBootstrap, { detach: false })
    }

    this.local = Autobase.getLocalCore(this.store, handlers, this.encryptionKey)
    this.localWriter = null
    this.activeWriters = new ActiveWriters()
    this.linearizer = null
    this.updating = false

    this.fastForwardingTo = handlers.fastForward !== false ? 0 : -1
    this.fastForwardTo = null

    this._checkWriters = []
    this._appending = null
    this._wakeup = new AutoWakeup(this)
    this._wakeupHints = new Set()
    this._queueViewReset = false

    this._applying = null
    this._updatingCores = false
    this._localDigest = null
    this._maybeUpdateDigest = true
    this._needsWakeup = true
    this._needsWakeupHeads = true
    this._addCheckpoints = false
    this._firstCheckpoint = true
    this._hasPendingCheckpoint = false
    this._systemPointer = 0

    this._updates = []
    this._handlers = handlers || {}

    this._advancing = null
    this._advanced = null

    this._bump = debounceify(() => {
      this._advancing = this._advance()
      return this._advancing
    })

    this._onremotewriterchangeBound = this._onremotewriterchange.bind(this)

    this.version = 0 // todo: set version

    this._presystem = null
    this._prebump = null

    this._hasApply = !!this._handlers.apply
    this._hasOpen = !!this._handlers.open
    this._hasClose = !!this._handlers.close

    this.onindex = handlers.onindex || noop

    this._viewStore = new AutoStore(this)

    this.view = null
    this.system = null

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
    await this.store.ready()

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

    // stateless open
    const ref = await this.local.getUserData('referrer')
    if (ref && !b4a.equals(ref, this.local.key) && !this._primaryBootstrap) {
      this._primaryBootstrap = this.store.get({ key: ref, compat: false, encryptionKey: this.encryptionKey })
      this.store = this.store.namespace(this._primaryBootstrap, { detach: false })
    }

    await this.local.setUserData('referrer', this.key)

    if (this._primaryBootstrap) {
      await this._primaryBootstrap.ready()
      if (this.encryptionKey) await this._primaryBootstrap.setUserData('autobase/encryption', this.encryptionKey)
    }

    const { bootstrap, system } = await this._loadSystemInfo()

    this.bootstrap = bootstrap
    this._initialSystem = system

    await this._makeLinearizer(system)
  }

  async _loadSystemInfo () {
    const pointer = await this.local.getUserData('autobase/system')
    const bootstrap = this.bootstrap || (await this.local.getUserData('referrer')) || this.local.key
    if (!pointer) return { bootstrap, system: null }

    const { indexed } = c.decode(messages.SystemPointer, pointer)
    const { key, length } = indexed

    const encryptionKey = AutoStore.getBlockKey(bootstrap, this.encryptionKey, '_system')
    const actualCore = length ? this.store.get({ key, exclusive: false, cache: true, compat: false, encryptionKey, isBlockKey: true }) : null

    this._systemPointer = length

    // TODO: remove this post our migration
    const isBorkedRoom = b4a.equals(this.key, b4a.from('26644f779a94f615b91edb72faa7f01d610364ec56b6f49dcffc89a0a9cff058', 'hex'))

    if (actualCore) await actualCore.ready()
    if (isBorkedRoom && (length === 0 || !actualCore.manifest || actualCore.manifest.signers.length < 3)) {
      await clearAndCloseCore(this.store.get('7e50ebd21b7edee041f02d05a0924b1b5f1f1a06a2e08d020d9b46dbcd209889'))
      await clearAndCloseCore(this.store.get('eb2e2bb3d7a3ee6ab6b96ece6f3d7758ba7dedc49eca73a2e648954dc2f12d01'))
      await clearAndCloseCore(this.store.get('f4a2de93568b68bfbaa95ba6b831c4fa7199813d94ee7f2edcbb183d33545843'))
      if (actualCore) await clearAndCloseCore(actualCore)

      await this.local.setUserData('autobase/system', null)
      this._systemPointer = 0
      return { bootstrap, system: null }
    }

    const core = length ? actualCore.batch({ checkout: length, session: false }) : null
    const system = length ? new SystemView(core, length) : null

    if (system) await system.ready()

    return {
      bootstrap,
      system
    }
  }

  async _openPreBump () {
    this._presystem = this._openPreSystem()

    try {
      await this._presystem
      await this._viewStore.flush()
    } catch (err) {
      safetyCatch(err)
      await this.local.setUserData('autobase/system', null)
      throw err
    }

    // see if we can load from indexer checkpoint
    await this.system.ready()

    if (this._initialSystem) {
      await this._initialSystem.close()
      this._initialSystem = null
    }

    // load previous digest if available
    if (this.localWriter && !this.system.bootstrapping) {
      await this._updateDigest()
    }

    if (this.localWriter && this._ackInterval) this._startAckTimer()
  }

  async _open () {
    this._prebump = this._openPreBump()
    await this._prebump

    await this._wakeup.ready()

    this.system.requestWakeup()

    // queue a full bump that handles wakeup etc (not legal to wait for that here)
    this._queueBump()
    this._advanced = this._advancing

    this.queueFastForward()
  }

  async _close () {
    await Promise.resolve() // defer one tick

    const closing = this._advancing.catch(safetyCatch)

    if (this._ackTimer) {
      this._ackTimer.stop()
      await this._ackTimer.flush()
    }

    await this._wakeup.close()

    if (this._hasClose) await this._handlers.close(this.view)
    if (this._primaryBootstrap) await this._primaryBootstrap.close()
    await this.store.close()
    await closing
  }

  _onError (err) {
    if (this.closing) return
    this.close().catch(safetyCatch)
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
      if (!w.flushed()) continue

      const unqueued = this._wakeup.unqueue(w.core.key, w.core.length)
      if (!unqueued || w.isIndexer || this.localWriter === w) continue

      // TODO: keep a random set around also for less cache churn...

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
    return this.fastForwardingTo > 0 && this.fastForwardingTo > this.system.core.getBackingCore().length
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
    await this._advanced // ensure all local state has been applied, only needed until persistent batches

    // if a reset is scheduled await those
    while (this._queueViewReset && !this.closing) await this._bump()

    if (this.localWriter === null) {
      throw new Error('Not writable')
    }

    // make sure all local nodes are processed before continuing
    while (!this.closing && this.localWriter.core.length > this.localWriter.length) {
      await this.bump()
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
    const viewName = await core.getUserData('autobase/view')
    return {
      referrer: await core.getUserData('referrer'),
      view: viewName ? b4a.toString(viewName) : null
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
    await this._primaryBootstrap.setUserData(key, val)
  }

  async getUserData (key) {
    await this._presystem
    return await this._primaryBootstrap.getUserData(key)
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

  // note: not parallel safe!
  async _getWriterByKey (key, len, seen, allowGC, system) {
    let w = this.activeWriters.get(key)
    if (w !== null) {
      w.seen(seen)
      return w
    }

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
    }

    w = this._makeWriter(key, len)
    w.seen(seen)
    await w.ready()

    if (allowGC && w.flushed()) {
      this._wakeup.unqueue(key, len)
      await w.close()
      return w
    }

    this.activeWriters.add(w)
    this._checkWriters.push(w)

    return w
  }

  _updateAll () {
    const p = []
    for (const w of this.activeWriters) p.push(w.update())
    return Promise.all(p)
  }

  _makeWriterCore (key) {
    const local = b4a.equals(key, this.local.key)

    const core = local
      ? this.local.session({ valueEncoding: messages.OplogMessage, encryptionKey: this.encryptionKey })
      : this.store.get({ key, compat: false, writable: false, valueEncoding: messages.OplogMessage, encryptionKey: this.encryptionKey })

    return core
  }

  _makeWriter (key, length) {
    const core = this._makeWriterCore(key)
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
    this._maybeUpdateDigest = true
    this._updateAckThreshold()
  }

  _migrateViews () {
    this._viewStore.migrate(this.linearizer.indexers)
  }

  async _bootstrapLinearizer () {
    const bootstrap = this._makeWriter(this.bootstrap, 0)

    this.activeWriters.add(bootstrap)
    this._checkWriters.push(bootstrap)
    bootstrap.isIndexer = true
    bootstrap.inflateBackground()
    await bootstrap.ready()

    this._updateLinearizer([bootstrap], [])
    this._updateDigest([bootstrap])
  }

  async _makeLinearizer (sys) {
    this._maybeUpdateDigest = true

    if (sys === null) {
      return this._bootstrapLinearizer()
    }

    // always load local to see if relevant...
    await this._getWriterByKey(this.local.key, -1, 0, false, sys)

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
    this._maybeUpdateDigest = true

    if (nodes) {
      this._undoAll()
      await this.system.update()
    }

    await this._makeLinearizer(this.system)

    if (nodes) {
      for (const node of nodes) node.reset()
      for (const node of nodes) this.linearizer.addHead(node)
    }
  }

  _addLocalHeads () {
    const nodes = new Array(this._appending.length)
    for (let i = 0; i < this._appending.length; i++) {
      const heads = this.linearizer.getHeads()
      const deps = new Set(this.linearizer.heads)
      const batch = this._appending.length - i
      const value = this._appending[i]

      const node = this.localWriter.append(value, heads, batch, deps)

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

  async _advanceSystemPointer (length) {
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

    await this._setSystemPointer(this.system.core.key, length, this.system.heads)
  }

  async _setSystemPointer (key, length, heads) {
    const pointer = c.encode(messages.SystemPointer, { indexed: { key, length }, heads })
    await this.local.setUserData('autobase/system', pointer)
  }

  async _drain () {
    while (!this.closing) {
      if (this.fastForwardTo !== null) {
        await this._applyFastForward()
        this.system.requestWakeup()
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
      if (this.updating || flushed > this._systemPointer) await this._advanceSystemPointer(flushed)

      if (indexed) await this.onindex(this)

      if (this.closing) return

      // force reset state in worst case
      if (this._queueViewReset && this._appending === null) {
        this._queueViewReset = false
        await this._forceResetViews()
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
      this._migrateViews()
    }
  }

  async _getLocallyStoredHeads () {
    const buffer = await this.local.getUserData('autobase/system')
    if (!buffer) return []
    return c.decode(messages.SystemPointer, buffer).heads
  }

  async _drainWakeup () { // TODO: parallel load the writers here later
    if (this._needsWakeup === true) {
      this._needsWakeup = false

      for (const { key } of this._wakeup) {
        await this._getWriterByKey(key, -1, 0, true)
      }

      if (this._needsWakeupHeads === true) {
        this._needsWakeupHeads = false

        for (const { key } of await this._getLocallyStoredHeads()) {
          await this._getWriterByKey(key, -1, 0, true)
        }
      }
    }

    for (const hex of this._wakeupHints) {
      const key = b4a.from(hex, 'hex')
      await this._getWriterByKey(key, -1, 0, true)
    }

    this._wakeupHints.clear()
  }

  async _advance () {
    if (this.opened === false) await this.ready()

    // note: this might block due to network i/o
    if (this._needsWakeup === true || this._wakeupHints.size > 0) await this._drainWakeup()

    try {
      await this._drain()
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
    }

    await this._gcWriters()
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

  async _forceResetViews () {
    const length = this.system.core.getBackingCore().indexedLength
    const info = await this.system.getIndexedInfo(length)

    this._undoAll()
    this._systemPointer = length
    await this._setSystemPointer(this.system.core.key, length, info.heads)

    for (const { key, length } of info.views) {
      const core = this._viewStore.getByKey(key)
      await core.reset(length)
    }

    for (const w of this.activeWriters) {
      await w.close()
      this.activeWriters.delete(w)
    }

    await this.system.update()
    await this._makeLinearizer(this.system)
  }

  // NOTE: runs in parallel with everything, can never fail
  async queueFastForward () {
    // if already FFing, let the finish. TODO: auto kill the attempt after a while and move to latest?
    if (this.fastForwardingTo !== 0) return

    const core = this.system.core.getBackingCore()

    if (core.session.length <= core.length + FF_THRESHOLD) return
    if (this.fastForwardTo !== null && core.session.length <= this.fastForwardTo.length + FF_THRESHOLD) return

    this.fastForwardingTo = core.session.length
    this._bumpAckTimer()

    try {
      // sys runs open with wait false, so get head block first for low complexity
      if (!(await core.session.has(this.fastForwardingTo - 1))) {
        await core.session.get(this.fastForwardingTo - 1)
      }

      const system = new SystemView(core.session.session(), this.fastForwardingTo)
      await system.ready()

      const pendingViews = []

      for (const v of system.views) {
        const view = this.store.get(v.key)

        // same as below, we technically just need to check that we have the hash, not the block
        if (v.length === 0 || await view.has(v.length - 1)) {
          await view.close()
        } else {
          pendingViews.push({ view, length: v.length })
        }
      }

      const promises = []
      const indexers = []

      for (const { key, length } of system.indexers) {
        if (length === 0) continue
        const core = this.store.get(key)
        await core.ready()
        indexers.push({ core, length })
      }

      for (const { core, length } of indexers) {
        if (core.length === 0 && length > 0) promises.push(core.get(length - 1))
      }

      for (const { view, length } of pendingViews) {
        // we could just get the hash here, but likely user wants the block so yolo
        promises.push(view.get(length - 1))
      }

      await Promise.all(promises)

      for (const { view } of pendingViews) {
        await view.close()
      }

      await system.close()
    } catch (err) {
      this.fastForwardingTo = 0
      safetyCatch(err)
      return
    }

    // if it migrated underneath us, ignore for now
    if (core !== this.system.core.getBackingCore()) {
      this.fastForwardingTo = 0
      return
    }

    for (const w of this.activeWriters) w.pause()

    this.fastForwardTo = { key: core.session.key, length: this.fastForwardingTo }
    this.fastForwardingTo = 0

    this._bumpAckTimer()
    this._queueBump()
  }

  _clearFastForward () {
    for (const w of this.activeWriters) w.resume()
    this.fastForwardTo = null
    this.queueFastForward() // queue in case we lost an ff while applying this one
  }

  async _applyFastForward () {
    const core = this.system.core.getBackingCore()
    const from = core.length

    // remember these in case another fast forward gets queued
    const { key, length } = this.fastForwardTo

    // just extra sanity check
    // TODO: if we simply load the core from the corestore the key check isn't needed
    // getting rid of that is essential for dbl ff, but for now its ok with some safety from migrations
    if (!b4a.equals(core.key, key) || length <= from) {
      this._clearFastForward()
      return
    }

    const system = new SystemView(core.session.session(), length)
    await system.ready()

    const views = new Map()

    let i = 0
    for (const v of system.views) {
      let view = this._viewStore.getByKey(v.key)

      if (!view || view.core.session.length < v.length) {
        const fallback = TMP_KNOWN_FALLBACK(v.key, this._viewStore)
        if (fallback) view = fallback
        else {
          this._clearFastForward() // something wrong somewhere, likely a bug, just safety
          return
        }
      }

      views.set(view, v)
      view.likelyIndex = i++
    }

    await system.close()

    for (const w of this.activeWriters) {
      await w.close()
      this.activeWriters.delete(w)
    }

    this._undoAll()

    for (const view of this._viewStore.opened.values()) {
      if (!views.has(view)) continue
      await view.catchup(views.get(view))
    }

    await this.system.update()

    await this._makeLinearizer(this.system)
    await this._advanceSystemPointer(length)

    const to = length

    if (b4a.equals(this.fastForwardTo.key, key) && this.fastForwardTo.length === length) {
      this.fastForwardTo = null
    }

    this.updating = true

    this.emit('fast-forward', to, from)

    // requeue in case we can do another jump!
    this.queueFastForward()
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

    const writer = (await this._getWriterByKey(key, -1, 0, false)) || this._makeWriter(key, 0)
    await writer.ready()

    // If we are getting added as indexer, already start adding checkpoints while we get confirmed...
    if (writer === this.localWriter && isIndexer) this._addCheckpoints = true
    if (isIndexer) this._maybeUpdateDigest = true

    // fetch any nodes needed for dependents
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
    await this._viewStore.flush()

    if (u.popped) this._undo(u.popped)

    // if anything was indexed reset the ticks
    if (u.indexed.length) this._resetAckTick()

    // make sure the latest changes is reflected on the system...
    await this.system.update()

    let batch = 0
    let applyBatch = []

    let j = 0

    let i = 0
    while (i < Math.min(u.indexed.length, u.shared)) {
      const node = u.indexed[i++]

      if (node.batch > 1) continue
      this._shiftWriter(node.writer)

      const update = this._updates[j++]
      if (!update.indexers) continue

      this._queueIndexFlush(i)

      return u.indexed.slice(i).concat(u.tip)
    }

    for (i = u.shared; i < u.length; i++) {
      if (this.fastForwardTo !== null && this.fastForwardTo.length > this.system.core.length && b4a.equals(this.fastForwardTo.key, this.system.core.key)) {
        return null
      }

      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      if (node.writer === this.localWriter) {
        this._resetAckTick()
      } else if (!indexed) {
        this._ackTick++
      }

      batch++

      if (this.system.addHead(node)) this._maybeUpdateDigest = true

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

      const update = { batch, indexers: false, views: [] }

      this._updates.push(update)
      this._applying = update

      if (this.system.bootstrapping) await this._bootstrap()

      if (applyBatch.length && this._hasApply === true) {
        try {
          await this._handlers.apply(applyBatch, this.view, this)
        } catch (err) {
          this._onError(err)
          return null
        }
      }

      update.indexers = await this.system.flush(update)

      if (indexed) this._shiftWriter(node.writer)

      this._applying = null

      batch = 0
      applyBatch = []

      for (let k = 0; k < update.views.length; k++) {
        const u = update.views[k]
        u.appending = u.core.appending
        u.core.appending = 0
      }

      if (update.indexers && indexed) {
        this._queueIndexFlush(i + 1)

        return u.indexed.slice(i + 1).concat(u.tip)
      }
    }

    if (u.indexed.length) {
      this._queueIndexFlush(u.indexed.length)
    }

    return null
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

  async _updateDigest () {
    this._maybeUpdateDigest = false
    if (!this._addCheckpoints) return

    if (this._localDigest === null) {
      this._localDigest = await this.localWriter.getDigest()

      if (this._localDigest === null) {
        this._localDigest = {
          pointer: 0,
          indexers: []
        }
      }
      return
    }

    const indexers = []

    const pending = this.system.core._source.pendingIndexedLength
    const info = await this.system.getIndexedInfo(pending)

    for (const { key } of info.indexers) {
      const w = await this._getWriterByKey(key)
      indexers.push({
        signature: 0,
        namespace: w.core.manifest.signers[0].namespace,
        publicKey: w.core.manifest.signers[0].publicKey
      })
    }

    let same = indexers.length === this._localDigest.indexers.length

    for (let i = 0; i < indexers.length; i++) {
      if (!same) break

      const a = indexers[i]
      const b = this._localDigest.indexers[i]

      if (a.signature !== b.signature || !b4a.equals(a.namespace, b.namespace) || !b4a.equals(a.publicKey, b.publicKey)) {
        same = false
      }
    }

    if (same) return

    this._localDigest.pointer = 0
    this._localDigest.indexers = indexers
  }

  _generateDigest () {
    return {
      pointer: this._localDigest.pointer,
      indexers: this._localDigest.indexers
    }
  }

  _generateCheckpoint (cores) {
    if (this._firstCheckpoint) {
      this._firstCheckpoint = false
      // TODO: unsafe, use an array instead for views as the order is important
      return generateCheckpoint(this._viewStore.opened.values())
    }

    return generateCheckpoint(cores)
  }

  async _flushLocal (localNodes) {
    if (this._maybeUpdateDigest) await this._updateDigest()

    const cores = this._addCheckpoints ? this._viewStore.getIndexedCores() : []
    const blocks = new Array(localNodes.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch } = localNodes[i]

      blocks[i] = {
        version: this.version,
        digest: this._addCheckpoints ? this._generateDigest() : null,
        checkpoint: this._addCheckpoints ? generateCheckpoint(cores) : null,
        node: {
          heads,
          batch,
          value: value === null ? null : c.encode(this.valueEncoding, value)
        },
        additional: {
          pointer: 0,
          data: {}
        }
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

  return checkpoint
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

async function clearAndCloseCore (core) {
  const b = core.batch({ clear: true })
  await b.flush()
  await b.close()
  await core.truncate(0, { fork: 0 })
  await core.close()
}

function TMP_KNOWN_FALLBACK (key, store) {
  return b4a.equals(key, b4a.from('f4a2de93568b68bfbaa95ba6b831c4fa7199813d94ee7f2edcbb183d33545843', 'hex'))
    ? store.getByKey(b4a.from('eb2e2bb3d7a3ee6ab6b96ece6f3d7758ba7dedc49eca73a2e648954dc2f12d01', 'hex'))
    : b4a.equals(key, b4a.from('ff894ffc4f44ec3e62fff9f38c43669105b18487c63ba4f46fb33883854f75f1', 'hex'))
      ? store.getByKey(b4a.from('4e7774b5f94e58b62bf0250af64f5369f387eec6c85672733898cfb082dc7a76', 'hex'))
      : null
}
