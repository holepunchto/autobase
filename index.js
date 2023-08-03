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

const { FLAG_OPLOG_IS_CHECKPOINTER } = messages

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

    this.sparse = false
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

    // preloader will be replaced
    this.system = new SystemView(this.store.get({ name: 'view/system', exclusive: true, cache: true }))

    const {
      ackInterval = DEFAULT_ACK_INTERVAL,
      ackThreshold = DEFAULT_ACK_THRESHOLD
    } = handlers

    this._ackInterval = ackInterval
    this._ackThreshold = ackThreshold
    this._ackTimer = null
    this._ackSize = 0
    this._acking = false

    // view opens after system is loaded
    this.view = this._hasOpen ? this._handlers.open(this._viewStore, this) : null

    this._restarts = 0

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
    await this.system.core.close()

    const autocore = this._viewStore.get({ name: 'system', exclusive: true, cache: true })
    await autocore.ready()

    this.system = new SystemView(autocore)

    await this.system.ready()
  }

  async _openCores () {
    if (!this._openingCores) this._openingCores = this._openCoresPromise()
    return this._openingCores
  }

  async _openCoresPromise () {
    await this.store.ready()
    await this.local.ready()
    await this.system.ready()

    if (this.system.bootstrapping && !this.bootstrap) {
      this.bootstrap = this.local.key // new autobase!
    }
  }

  async _open () {
    await this._openCores()

    // reindex to load writers
    this._reindex()

    // see if we can load from indexer checkpoint
    await this._openSystem()
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

    for (const w of this.writers) {
      await w.core.update(opts)
      if (!this.sparse) await downloadAll(w.core)
    }

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
      : this.store.get({ key, sparse: this.sparse, valueEncoding: messages.OplogMessage })

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
    }

    return w
  }

  _startIndex () {
    const bootstrap = this._makeWriter(this.bootstrap, 0)

    this.writers = [bootstrap]
    bootstrap.isIndexer = true

    this.linearizer = new Linearizer([bootstrap], [], [])
  }

  _reindex (change) {
    if (this.system.bootstrapping) {
      this._startIndex()
      return
    }

    const indexers = []
    const heads = []
    const clock = []

    for (const { key, length, indexer } of this.system.digest.writers) {
      const writer = this._getWriterByKey(key, length)

      clock.push({ writer, length })

      if (!indexer) continue

      indexers.push(writer)
      writer.isIndexer = true
    }

    for (const head of this.system.digest.heads) {
      const writer = this._getWriterByKey(head.key)
      const headNode = Linearizer.createNode(writer, head.length, null, [], 1, [])
      headNode.yielded = true
      heads.push(headNode)
    }

    const removed = this.system.digest.writers.map(w => {
      const writer = this._getWriterByKey(w.key)
      return { writer, length: w.length }
    })

    this.linearizer = new Linearizer(indexers, {
      heads,
      removed,
      cacheIndex: this._restarts++
    })

    if (change) this._reloadUpdate(change, heads)
  }

  _reloadUpdate (change, heads) {
    const { count, nodes } = change

    this._undo(count)

    for (const node of nodes) {
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

    for (const node of nodes) {
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

      if (!changed) break

      this._reindex(changed)
    }

    // skip threshold check while acking
    if (!this.closing && this._ackThreshold && !this._acking) {
      const n = this._ackThreshold * this.linearizer.indexers.length

      // await here would cause deadlock, fine to run in bg
      if (this.linearizer.size >= (1 + Math.random()) * n) this._triggerAck()
    }

    return this._ensureAllCores()
  }

  // triggered from linearized core
  _onuserappend (core, blocks) {
    assert(this._applying !== null, 'Append is only allowed in apply')

    if (core.appending === 0) {
      this._applying.user.push({ core, appending: 0 })
    }

    core.appending += blocks
  }

  // triggered from system
  addWriter (key, { indexer = true } = {}) {
    assert(this._applying !== null, 'System changes are only allowed in apply')

    this.system.addWriter(key, indexer)

    for (const w of this.writers) {
      if (b4a.equals(w.core.key, key)) return
    }

    const writer = this._makeWriter(key, 0)
    if (indexer) writer.isIndexer = true

    this.writers.push(writer)

    // fetch any nodes needed for dependents
    this._bump()
  }

  _undo (popped) {
    const truncating = []

    while (popped > 0) {
      const u = this._updates.pop()

      popped -= u.batch

      for (const { core, appending } of u.user) {
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
    this.system.addWriter(this.bootstrap, true)
  }

  async _applyUpdate (u) {
    await this._viewStore.update()

    if (u.popped) this._undo(u.popped)

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
      if (!update.system) continue

      await this._flushIndexes(i)

      const nodes = u.indexed.slice(i).concat(u.tip)
      return { count: u.shared - i, nodes }
    }

    for (i = u.shared; i < u.length; i++) {
      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      if (indexed) node.writer.indexed++

      batch++

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

      const update = { batch, system: 0, user: [] }

      this._updates.push(update)
      this._applying = update

      if (this.system.bootstrapping) this._bootstrap()

      if (applyBatch.length && this._hasApply === true) {
        try {
          await this._handlers.apply(applyBatch, this.view, this)
        } catch (err) {
          this.close()
          this.emit('error', err)
          return null
        }
      }

      update.system = await this.system.flush(update, node, this.writers)

      // local flushed in _flushLocal
      if (indexed && node.writer !== this.localWriter) {
        node.writer.shift()
        node.clear()
      }

      this._applying = null

      batch = []
      applyBatch = []

      for (let k = 0; k < update.user.length; k++) {
        const u = update.user[k]
        u.appending = u.core.appending
        u.core.appending = 0
      }

      if (update.system && indexed) {
        await this._flushIndexes(i + 1)

        const nodes = u.indexed.slice(i + 1).concat(u.tip)
        return { count: 0, nodes }
      }
    }

    if (u.indexed.length) {
      await this._flushIndexes(u.indexed.length)
    }

    return null
  }

  async _flushIndexes (indexed) {
    const updatedCores = []

    while (indexed > 0) {
      const u = this._updates.shift()

      indexed -= u.batch

      for (const { core, appending } of u.user) {
        const start = core.indexing
        const blocks = core.indexBatch(start, core.indexing += appending)
        if (start === 0) updatedCores.push(core)

        await core.core.append(blocks)
      }
    }

    for (const core of updatedCores) {
      const indexing = core.indexing
      core.indexing = 0
      core._onindex(indexing)

      if (core === this.system.core._source) {
        await this.system._onindex()
      }
    }
  }

  async _flushLocal () {
    const checkpoint = []
    if (this.localWriter.isIndexer) {
      for (const [, core] of this._viewStore.opened) {
        if (!core.indexedLength) continue
        checkpoint.push(core.checkpoint())
      }
    }

    const blocks = new Array(this.localWriter.length - this.local.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch, yielded } = this.localWriter.get(this.local.length + i)

      if (yielded) this.localWriter.shift().clear()

      let flags = 0
      if (this.localWriter.isIndexer) flags |= FLAG_OPLOG_IS_CHECKPOINTER

      blocks[i] = {
        flags,
        version: this.version,
        checkpoint: checkpoint.sort(cmpCheckpoint),
        node: {
          heads,
          abi: 0,
          batch,
          value: value === null ? null : c.encode(this.valueEncoding, value)
        }
      }

      if (this.localWriter.isIndexer) {
        for (const [, core] of this._viewStore.opened) {
          if (core.indexedLength) core.checkpointer++
        }
      }
    }

    return this.local.append(blocks)
  }

  async loadIndex (name) {
    if (this.system.bootstrapping) return 0

    const idx = await this.system.getIndex(name)
    if (idx < 0) return 0

    // only indexers have checkpoints
    if (this.localWriter && this.localWriter.isIndexer) {
      const checkpoint = await this.localWriter.getCheckpoint(idx)
      if (checkpoint) return checkpoint.length
    }

    // todo: checkpoints should be reactive so no need for lookup
    for (const head of this.system.digest.heads) {
      const indexer = this._getWriterByKey(head.key)
      const checkpoint = await indexer.getCheckpoint(idx, head.length)

      if (checkpoint) return checkpoint.length
    }

    return 0
  }
}

function toKey (k) {
  return b4a.isBuffer(k) ? k : b4a.from(k, 'hex')
}

function downloadAll (core) {
  const start = core.length
  const end = core.core.tree.length

  return core.download({ start, end, ifAvailable: true }).done()
}

function compareHead (head, node) {
  return head.length === node.length && b4a.equals(head.key, node.writer.core.key)
}

function isAutobaseMessage (msg) {
  const indexer = msg.flags & FLAG_OPLOG_IS_CHECKPOINTER
  return indexer ? msg.checkpoint.length > 0 : msg.checkpoint.length === 0
}

function cmpCheckpoint (a, b) {
  return a.index - b.index
}
