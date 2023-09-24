const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
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

    this._checkWriters = []
    this._appending = null
    this._wakeup = new AutoWakeup(this)

    this._applying = null
    this._updatedCores = null
    this._localDigest = null
    this._maybeUpdateDigest = true
    this._needsWakeup = true
    this._addCheckpoints = false
    this._firstCheckpoint = true

    this._updates = []
    this._handlers = handlers || {}

    this._bump = debounceify(this._advance.bind(this))
    this._onremotewriterchangeBound = this._onremotewriterchange.bind(this)

    this.version = 0 // todo: set version

    this._presystem = null
    this._prebump = null

    this._hasApply = !!this._handlers.apply
    this._hasOpen = !!this._handlers.open
    this._hasClose = !!this._handlers.close

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

    this._pendingCheckpoints = new Map()
    this._needsFlush = new Set()
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

  async _openPreSystem () {
    await this.store.ready()

    await this.local.ready()
    await this.local.setUserData('referrer', this.key)

    if (this.encryptionKey) {
      await this.local.setUserData('autobase/encryption', this.encryptionKey)
    } else {
      this.encryptionKey = await this.local.getUserData('autobase/encryption')
      if (this.encryptionKey) {
        this.local.setEncryptionKey(this.encryptionKey)
        // not needed but, just for good meassure
        if (this._primaryBootstrap) this._primaryBootstrap.setEncryptionKey(this.encryptionKey)
      }
    }

    if (this._primaryBootstrap) {
      await this._primaryBootstrap.ready()
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

    const { key, length } = c.decode(messages.SystemPointer, pointer)
    const encryptionKey = this.encryptionKey
    const system = length ? new SystemView(this.store.get({ key, exclusive: false, cache: true, compat: false, encryptionKey }), length) : null

    if (system) await system.ready()

    return {
      bootstrap,
      system
    }
  }

  async _openPreBump () {
    this._presystem = this._openPreSystem()
    await this._presystem

    await this._viewStore.flush()

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

  async _wakeupHeads () {
    const buffer = await this.local.getUserData('autobase/heads')
    if (!buffer) return
    const keys = c.decode(c.array(c.fixed32), buffer)
    for (const key of keys) await this._getWriterByKey(key, -1, 0, true)
  }

  async _open () {
    this._prebump = this._openPreBump()
    await this._prebump

    await this._wakeup.ready()
    await this._wakeupHeads()

    await this._bump()
  }

  async _close () {
    if (this._ackTimer) {
      this._ackTimer.stop()
      await this._ackTimer.flush()
    }

    await this._wakeup.close()
    if (this._hasClose) await this._handlers.close(this.view)
    if (this._primaryBootstrap) await this._primaryBootstrap.close()
    await this.store.close()
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

  async _advanceSystemPointer () {
    await this.local.ready() // todo: remove
    await this.local.setUserData('autobase/system', c.encode(messages.SystemPointer, {
      key: this.system.core.key,
      length: this.system.core._source.signedLength,
      signature: this.system.core._source.core.session.core.tree.signature
    }))
  }

  _startAckTimer () {
    if (this._ackTimer) return
    this._ackTimer = new Timer(this.ack.bind(this), this._ackInterval)
    this._bumpAckTimer()
  }

  _bumpAckTimer () {
    if (!this._ackTimer) return
    this._ackTimer.bump()
  }

  _triggerAck () {
    if (this._ackTimer) this._ackTimer.bump()
    return this.ack()
  }

  async update () {
    if (this.opened === false) await this.ready()

    try {
      await this._bump()
      if (this._acking) await this._bump() // if acking just rebump incase it was triggered from above...
    } catch (err) {
      if (this.closing) return false
      throw err
    }

    return true
  }

  // runs in bg, not allowed to throw
  // TODO: refactor so this only moves the writer affected to a updated set
  async _onremotewriterchange () {
    try {
      await this._bump()
    } catch (err) {
      if (!this.closing) throw err
    }

    this._bumpAckTimer()
  }

  _onwakeup () {
    this._needsWakeup = true
    this._bump().catch(safetyCatch)
  }

  _isPending () {
    for (const key of this.system.pendingIndexers) {
      if (b4a.equals(key, this.local.key)) return true
    }
    return false
  }

  async ack () {
    if (this.localWriter === null) return

    const isPendingIndexer = this._isPending()
    const isIndexer = this.localWriter.isIndexer || isPendingIndexer

    if (!isIndexer || this._acking || this.closing) return

    this._acking = true

    try {
      await this._bump()
    } catch (err) {
      if (!this.closing) throw err
    }

    const unflushed = this.hasUnflushedIndexers()
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

    if (this.localWriter === null) {
      throw new Error('Not writable')
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

  getNamespace (key, core) {
    const w = this.activeWriters.get(key)
    if (!w) return null

    const namespace = w.deriveNamespace(core.name)
    const publicKey = w.core.manifest.signer.publicKey

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
      this._undo(this._updates.length) // undo all!
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

  async _checkpointHeads () {
    if (!this.linearizer.updated) return

    const state = { start: 0, end: 0, buffer: null }

    c.uint.preencode(state, this.linearizer.heads.size)
    state.buffer = b4a.allocUnsafe(state.end + this.linearizer.heads.size * 32)

    c.uint.encode(state, this.linearizer.heads.size)
    for (const head of this.linearizer.heads) c.fixed32.encode(state, head.writer.core.key)

    await this.local.setUserData('autobase/heads', state.buffer)
  }

  async _advance () {
    if (this.opened === false) await this._prebump

    if (this._needsWakeup) {
      this._needsWakeup = false
      for (const { key } of this._wakeup) await this._getWriterByKey(key, -1, 0, true)
    }

    this.updating = false

    while (!this.closing) {
      const remoteAdded = await this._addRemoteHeads()
      const localNodes = this._appending === null ? null : this._addLocalHeads()

      if (this.closing) return

      if (remoteAdded > 0 || localNodes !== null) {
        this.updating = true
        await this._checkpointHeads()
      }

      const u = this.linearizer.update()
      const changed = u ? await this._applyUpdate(u) : null

      if (this.closing) return

      if (this.localWriter !== null && localNodes !== null) {
        await this._flushLocal(localNodes)
      }

      if (this.closing) return

      if (this._updatedCores !== null || this._needsFlush.size) {
        if (await this._flushIndexes()) {
          await this._advanceSystemPointer()
        }
      }

      if (this.closing) return

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

    // skip threshold check while acking
    if (!this.closing && this._ackTickThreshold && !this._acking && this._ackTick >= this._ackTickThreshold) {
      if (this._ackTimer) this._ackTimer.asap()
      else this._triggerAck()
    }

    if (!this.closing && this.system.pendingIndexers.length > 0) {
      for (const key of this.system.pendingIndexers) {
        if (b4a.equals(key, this.local.key) && !b4a.equals(key, this.bootstrap)) {
          this._triggerAck()
          break
        }
      }
    }

    if (this.updating === true) {
      this.updating = false
      this.emit('update')
    }

    return this._gcWriters()
  }

  async _flushIndexes () {
    const needsFlush = this._needsFlush
    this._needsFlush = new Set()

    let complete = !!(this._updatedCores && this._updatedCores.length)

    if (this._updatedCores) {
      const updatedCores = this._updatedCores
      this._updatedCores = null

      for (const core of updatedCores) {
        if (!await core.flush()) complete &&= false
        needsFlush.delete(core)
      }

      for (const core of updatedCores) {
        const indexing = core.indexing
        core.indexing = 0
        core._onindex(indexing)
      }
    }

    for (const core of needsFlush) {
      if (!await core.flush()) complete &&= false
    }

    return complete
  }

  _onindexercheckpoint (indexer, checkpoints) {
    if (!checkpoints) return // todo: this shouldn't fire without checkpoints

    const indexed = this._viewStore.getIndexedCores()

    for (let i = 0; i < checkpoints.length; i++) {
      const { checkpoint, checkpointer } = checkpoints[i]
      if (checkpointer > 0) continue

      if (i < indexed.length) {
        if (indexed[i].signer._oncheckpoint({ indexer, checkpoint })) {
          this._needsFlush.add(indexed[i])
        }
        continue
      }

      this._pendingCheckpoints.set(i, { indexer, checkpoint })
    }
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
    this._bump().catch(safetyCatch)
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
          await this.close()
          this.emit('error', err)
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
    assert(this._updatedCores === null, 'Updated cores not flushed')
    this._updatedCores = []

    let sys = -1

    while (indexed > 0) {
      const u = this._updates.shift()

      indexed -= u.batch

      for (const { core, appending } of u.views) {
        const start = core.indexing
        core.indexing += appending
        if (start === 0) {
          if (core._isSystem()) sys = this._updatedCores.length // system ALWAYS goes last
          this._updatedCores.push(core)

          const pending = this._pendingCheckpoints.get(core.likelyIndex)
          if (!pending) continue

          core.signer._oncheckpoint(pending)
          this._pendingCheckpoints.delete(core.likelyIndex)
        }
      }
    }

    // ensure system is always last
    if (sys > -1 && sys < this._updatedCores.length - 1) {
      const a = this._updatedCores[sys]
      const b = this._updatedCores[this._updatedCores.length - 1]

      this._updatedCores[sys] = b
      this._updatedCores[this._updatedCores.length - 1] = a
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
        namespace: w.core.manifest.signer.namespace,
        publicKey: w.core.manifest.signer.publicKey
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
          abi: 0,
          batch,
          value: value === null ? null : c.encode(this.valueEncoding, value)
        }
      }

      if (this._addCheckpoints) {
        this._localDigest.pointer++
        this._onindexercheckpoint(this.local.key, blocks[i].checkpoint)
      }
    }

    await this.local.append(blocks)
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
  return b4a.isBuffer(k) ? k : b4a.from(k, 'hex')
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
