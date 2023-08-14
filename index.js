const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const FIFO = require('fast-fifo')
const debounceify = require('debounceify')
const c = require('compact-encoding')
const safetyCatch = require('safety-catch')
const assert = require('nanoassert')

const Linearizer = require('./lib/linearizer')
const Autocore = require('./lib/core')
const SystemView = require('./lib/system')
const messages = require('./lib/messages')
const Timer = require('./lib/timer')
const Writer = require('./lib/writer')

const inspect = Symbol.for('nodejs.util.inspect.custom')
const REFERRER_USERDATA = 'referrer'
const VIEW_NAME_USERDATA = 'autobase/view'

// default is to automatically ack
const DEFAULT_ACK_INTERVAL = 10 * 1000
const DEFAULT_ACK_THRESHOLD = 4

class LinearizedStore {
  constructor (base) {
    this.base = base
    this.opened = new Map()
    this.waiting = []
  }

  get (opts, moreOpts) {
    if (typeof opts === 'string') opts = { name: opts }
    if (moreOpts) opts = { ...opts, ...moreOpts }

    const name = opts.name
    const valueEncoding = opts.valueEncoding || null

    if (this.opened.has(name)) return this.opened.get(name).createSession(valueEncoding)

    const core = this.base.store.get({ name: 'view/' + name, cache: opts.cache, exclusive: true })
    const ac = new Autocore(this.base, core, name)

    this.waiting.push(ac)
    this.opened.set(name, ac)

    return ac.createSession(valueEncoding)
  }

  async update () {
    while (this.waiting.length) {
      const core = this.waiting.pop()
      await core.ready()
    }
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

    this.bootstrap = bootstrap ? toKey(bootstrap) : null
    this.valueEncoding = c.from(handlers.valueEncoding || 'binary')
    this.store = store
    this._primaryBootstrap = null
    this._mainStore = null

    if (this.bootstrap) {
      this._primaryBootstrap = this.store.get({ key: this.bootstrap })
      this._mainStore = this.store
      this.store = this.store.namespace(this._primaryBootstrap)
    }

    this.local = Autobase.getLocalCore(this.store)
    this.localWriter = null
    this.linearizer = null

    this.writers = []

    this._appending = new FIFO()

    this._applying = null
    this._updatedCores = null

    this._needsReady = []
    this._updates = []
    this._handlers = handlers || {}

    this._bump = debounceify(this._advance.bind(this))
    this._onremotewriterchangeBound = this._onremotewriterchange.bind(this)

    this.version = 0 // todo: set version

    this._openingCores = null

    this._hasApply = !!this._handlers.apply
    this._hasOpen = !!this._handlers.open
    this._hasClose = !!this._handlers.close

    this._viewStore = new LinearizedStore(this)

    this.view = null
    this.system = null

    const {
      ackInterval = DEFAULT_ACK_INTERVAL,
      ackThreshold = DEFAULT_ACK_THRESHOLD
    } = handlers

    this._ackInterval = ackInterval
    this._ackThreshold = ackThreshold
    this._ackTimer = null
    this._ackSize = 0
    this._acking = false

    this._restarts = 0

    // view opens after system is loaded
    this.system = new SystemView(this._viewStore.get({ name: 'system', exclusive: true, cache: true }))
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

  async _openSystem () {
    await this.system.ready()
  }

  async _openCores () {
    if (!this._openingCores) this._openingCores = this._openCoresPromise()
    return this._openingCores
  }

  async _openCoresPromise () {
    await this.store.ready()
    await this.local.ready()

    const sysCore = this.system.core._backingCore()
    await sysCore.ready()

    const bootstrapping = sysCore.length < 2 // record 0 is just metadata...

    if (bootstrapping && !this.bootstrap) {
      this.bootstrap = this.local.key // new autobase!
    }
  }

  async _open () {
    await this._openCores()

    // see if we can load from indexer checkpoint
    await this.system.ready()

    // reindex to load writers
    await this._reindex(null)

    await this._ensureUserData(this.system.core, null)

    if (this.localWriter && this._ackInterval) this._startAckTimer()
  }

  async _close () {
    if (this._hasClose) await this._handlers.close(this.view)
    if (this._primaryBootstrap) await this._primaryBootstrap.close()
    await this.store.close()
    if (this._ackTimer) this._ackTimer.stop()
    if (this._mainStore) await this._mainStore.close()
  }

  async _ensureUserData (core, name) {
    await core.setUserData(REFERRER_USERDATA, this.key)
    if (name) {
      await core.setUserData(VIEW_NAME_USERDATA, b4a.from(name))
    }
  }

  async _ensureAllCores () {
    while (this._needsReady.length > 0) {
      const core = this._needsReady.pop()
      await core.ready()
      await this._ensureUserData(core, null)
    }
  }

  _startAckTimer () {
    if (this._ackTimer) return
    this._ackTimer = new Timer(this._ack.bind(this), this._ackInterval)
    this._bumpAckTimer()
  }

  _bumpAckTimer () {
    if (!this._ackTimer) return
    this._ackTimer.bump()
  }

  _triggerAck () {
    if (this._ackTimer) {
      return this._ackTimer.trigger()
    } else {
      return this._ack(true)
    }
  }

  async update (opts) {
    await this.ready()
    await this._bump()
  }

  // runs in bg, not allowed to throw
  async _onremotewriterchange () {
    await this._bump()
    this._bumpAckTimer()
  }

  ack () {
    return this._ack(false)
  }

  async _ack (triggered) {
    if (this.localWriter === null || !this.localWriter.isIndexer || this._acking) return

    this._acking = true

    await this.update({ wait: true })

    if (this._ackTimer && !triggered) {
      const ackSize = this.linearizer.size
      if (this._ackSize < ackSize) {
        this._ackTimer.extend()
      } else {
        this._ackTimer.reset()
      }

      this._ackSize = ackSize
    }

    if (this.linearizer.shouldAck(this.localWriter)) {
      await this.append(null)
    }

    this._acking = false
  }

  async append (value) {
    if (!this.opened) await this.ready()

    if (this.localWriter === null) {
      throw new Error('Not writable')
    }

    if (Array.isArray(value)) {
      for (const v of value) this._appending.push(v)
    } else {
      this._appending.push(value)
    }

    await this._bump()
  }

  async checkpoint () {
    await this.ready()
    const all = []

    for (const w of this.writers) {
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

  static getLocalCore (store) {
    return store.get({ name: 'local', exclusive: true, valueEncoding: messages.OplogMessage })
  }

  static async getUserData (core) {
    const viewName = await core.getUserData(VIEW_NAME_USERDATA)
    return {
      referrer: await core.getUserData(REFERRER_USERDATA),
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

  _getWriterByKey (key, len = 0) {
    for (const w of this.writers) {
      if (b4a.equals(w.core.key, key)) return w
    }

    const w = this._makeWriter(key, len)
    this.writers.push(w)

    return w
  }

  _updateAll () {
    const p = []
    for (const w of this.writers) p.push(w.update())
    return Promise.all(p)
  }

  _makeWriter (key, length) {
    const local = b4a.equals(key, this.local.key)

    const core = local
      ? this.local.session({ valueEncoding: messages.OplogMessage })
      : this.store.get({ key, valueEncoding: messages.OplogMessage })

    // Small hack for now, should be fixed in hypercore (that key is set immediatly)
    core.key = key
    this._needsReady.push(core)

    const w = new Writer(this, core, length)

    if (local) {
      this.localWriter = w
      if (this._ackInterval) this._startAckTimer()
      this.emit('writable')
    } else {
      core.on('append', this._onremotewriterchangeBound)
      core.on('download', this._onremotewriterchangeBound)
    }

    return w
  }

  _startIndex () {
    const bootstrap = this._makeWriter(this.bootstrap, 0)

    this.writers = [bootstrap]
    bootstrap.isIndexer = true

    this.linearizer = new Linearizer([bootstrap], [], [])
  }

  async _reindex (change) {
    if (this.system.bootstrapping) {
      this._startIndex()
      return
    }

    if (change) {
      this._undo(change.count)
      await this.system.update()
    }

    const indexers = []
    const heads = []
    const clock = []

    for await (const { key, value } of this.system.list()) {
      const writer = this._getWriterByKey(key, value.length)

      clock.push({ writer, length: value.length })

      if (!value.isIndexer) continue

      indexers.push(writer)
      writer.isIndexer = true
    }

    for (const head of this.system.heads) {
      const writer = this._getWriterByKey(head.key)
      const headNode = Linearizer.createNode(writer, head.length, null, [], 1, [])
      headNode.yielded = true
      heads.push(headNode)
    }

    this.linearizer = new Linearizer(indexers, {
      heads,
      removed: clock,
      cacheIndex: this._restarts++
    })

    if (change) this._reloadUpdate(change, heads)
  }

  _reloadUpdate (change, heads) {
    for (const node of change.nodes) {
      node.yielded = false
      node.dependents.clear()

      for (let i = 0; i < node.heads.length; i++) {
        const link = node.heads[i]

        const writer = this._getWriterByKey(link.key)
        if (node.clock.get(writer.core.key) < link.length) {
          node.clock.set(writer.core.key, link.length)
        }

        for (const head of heads) {
          if (compareHead(link, head)) {
            node.dependencies.add(head)
            break
          }
        }
      }

      heads.push(node)
    }

    for (const node of change.nodes) {
      this.linearizer.addHead(node)
    }
  }

  async _addHeads () {
    let added = 0

    while (added < 50) { // 50 here is just to reduce the bulk batches
      await this._updateAll()

      let advanced = 0

      for (const w of this.writers) {
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
  }

  async _advance () {
    while (!this.closing) {
      while (!this._appending.isEmpty()) {
        const batch = this._appending.length
        const value = this._appending.shift()

        // filter out pointless acks
        if (value === null && !this._appending.isEmpty()) continue

        const heads = new Set(this.linearizer.heads)

        const node = this.localWriter.append(value, heads, batch)
        this.linearizer.addHead(node)
      }

      await this._addHeads()

      const u = this.linearizer.update()
      const changed = u ? await this._applyUpdate(u) : null

      if (this.closing) break

      if (this.localWriter !== null && this.localWriter.length > this.local.length) {
        await this._flushLocal()
      }

      if (this.closing) break

      if (this._updatedCores !== null) {
        await this._flushIndexes()
      }

      if (this.closing || !changed) break

      await this._reindex(changed)
    }

    // skip threshold check while acking
    if (!this.closing && this._ackThreshold && !this._acking) {
      const n = this._ackThreshold * this.linearizer.indexers.length

      // await here would cause deadlock, fine to run in bg
      if (this.linearizer.size >= (1 + Math.random()) * n) this._triggerAck()
    }

    return this._ensureAllCores()
  }

  async _flushIndexes () {
    const updatedCores = this._updatedCores
    this._updatedCores = null

    for (const core of updatedCores) {
      const batch = core.indexBatch(0, core.indexing)
      await core.core.append(batch)
    }

    for (const core of updatedCores) {
      const indexing = core.indexing
      core.indexing = 0
      core._onindex(indexing)
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

    await this.system.add(key, { isIndexer })

    for (const w of this.writers) {
      if (b4a.equals(w.core.key, key)) return
    }

    const writer = this._makeWriter(key, 0)
    if (isIndexer) writer.isIndexer = true

    this.writers.push(writer)

    // fetch any nodes needed for dependents
    this._bump()
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

  _bootstrap () {
    return this.system.add(this.bootstrap, { isIndexer: true })
  }

  async _applyUpdate (u) {
    await this._viewStore.update()

    if (u.popped) this._undo(u.popped)

    // make sure the latest changes is reflected on the system...
    await this.system.update()

    let batch = 0
    let applyBatch = []

    let j = 0

    let i = 0
    while (i < Math.min(u.indexed.length, u.shared)) {
      const node = u.indexed[i++]

      node.writer.indexed++
      node.writer.shift().clear()

      if (node.batch > 1) continue

      const update = this._updates[j++]
      if (!update.indexers) continue

      this._queueIndexFlush(i)

      const nodes = u.indexed.slice(i).concat(u.tip)
      return { count: u.shared - i, nodes }
    }

    for (i = u.shared; i < u.length; i++) {
      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      if (indexed) node.writer.indexed++

      batch++
      this.system.addHead(node)

      if (node.value !== null) {
        applyBatch.push({
          indexed,
          from: node.writer.core,
          length: node.length,
          value: node.value,
          heads: node.heads
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
          this.close()
          this.emit('error', err)
          return null
        }
      }

      update.indexers = await this.system.flush(update)

      // local flushed in _flushLocal
      if (indexed && node.writer !== this.localWriter) {
        node.writer.shift()
        node.clear()
      }

      this._applying = null

      batch = []
      applyBatch = []

      for (let k = 0; k < update.views.length; k++) {
        const u = update.views[k]
        u.appending = u.core.appending
        u.core.appending = 0
      }

      if (update.indexers && indexed) {
        this._queueIndexFlush(i + 1)

        const nodes = u.indexed.slice(i + 1).concat(u.tip)
        return { count: 0, nodes }
      }
    }

    if (u.indexed.length) {
      this._queueIndexFlush(u.indexed.length)
    }

    return null
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
          if (core.name === 'system') sys = this._updatedCores.length // system ALWAYS goes last
          this._updatedCores.push(core)
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

  async _flushLocal () {
    const digest = { // TODO: use this
      pointer: 0,
      seed: Buffer.alloc(32),
      indexers: this.system.indexers.map(onlyKey)
    }

    const cores = this._getCheckpointCores()
    const blocks = new Array(this.localWriter.length - this.local.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch, yielded } = this.localWriter.get(this.local.length + i)

      if (yielded) this.localWriter.shift().clear()

      blocks[i] = {
        version: this.version,
        digest: this.localWriter.isIndexer ? digest : null,
        checkpoint: this.localWriter.isIndexer ? generateCheckpoint(cores) : null,
        node: {
          heads,
          abi: 0,
          batch,
          value: value === null ? null : c.encode(this.valueEncoding, value)
        }
      }
    }

    return this.local.append(blocks)
  }

  _getCheckpointCores () {
    if (!this.localWriter.isIndexer) return []

    const cores = []

    for (let i = 0; i < this.system.views.length; i++) {
      const v = this.system.views[i]
      const core = this._viewStore.opened.get(v.name)
      if (!core) break
      core.index = i // just in case its out of date...
      if (!core.indexedLength && !core.indexing) continue
      cores.push(core)
    }

    return cores
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

function onlyKey (o) {
  return o.key
}

function toKey (k) {
  return b4a.isBuffer(k) ? k : b4a.from(k, 'hex')
}

function compareHead (head, node) {
  return head.length === node.length && b4a.equals(head.key, node.writer.core.key)
}

function isAutobaseMessage (msg) {
  return msg.checkpoint ? msg.checkpoint.length > 0 : msg.checkpoint === null
}
