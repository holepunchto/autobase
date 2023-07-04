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
const NodeBuffer = require('./lib/node-buffer')
const Timer = require('./lib/timer')

const inspect = Symbol.for('nodejs.util.inspect.custom')
const REFERRER_USERDATA = 'referrer'
const VIEW_NAME_USERDATA = 'autobase/view'

// default is not to ack
const DEFAULT_ACK_INTERVAL = 0
const DEFAULT_ACK_THRESHOLD = 0

class Writer {
  constructor (base, core, length) {
    this.base = base
    this.core = core
    this.nodes = new NodeBuffer(length)
    this.indexed = length

    this.next = null
    this.nextCache = []
  }

  get length () {
    return this.nodes.length
  }

  compare (writer) {
    return b4a.compare(this.core.key, writer.core.key)
  }

  head () {
    return this.nodes.get(this.nodes.length - 1)
  }

  shift () {
    return this.nodes.shift()
  }

  getCached (seq) {
    return this.nodes.get(seq)
  }

  advance (node = this.next) {
    this.nodes.push(node)
    this.next = null
    return node
  }

  append (value, dependencies, batch) {
    const node = Linearizer.createNode(this, this.length + 1, value, [], batch, dependencies)

    for (const dep of dependencies) {
      if (!dep.yielded) {
        node.clock.add(dep.clock)
      }

      node.heads.push({
        key: dep.writer.core.key,
        length: dep.length
      })
    }

    node.clock.set(node.writer, node.length)

    this.advance(node)
    return node
  }

  async ensureNext () {
    if (this.length >= this.core.length || this.core.length === 0) return null
    if (this.next !== null) return this.next

    const cache = this.nextCache

    if (!cache.length && !(await this.core.has(this.length + cache.length))) return null

    while (!cache.length || cache[cache.length - 1].batch !== 1) {
      const { node } = await this.core.get(this.length + cache.length)
      const value = node.value == null ? null : c.decode(this.base.valueEncoding, node.value)
      cache.push(Linearizer.createNode(this, this.length + cache.length + 1, value, node.heads, node.batch, []))
    }

    this.next = await this.ensureNode(cache)
    return this.next
  }

  async ensureNode (batch) {
    const last = batch[batch.length - 1]
    if (last.batch !== 1) return null

    const node = batch.shift()

    while (node.dependencies.size < node.heads.length) {
      const rawHead = node.heads[node.dependencies.size]

      const headWriter = await this.base._getWriterByKey(rawHead.key)
      if (headWriter === null || headWriter.length < rawHead.length) {
        return null
      }

      const headNode = headWriter.getCached(rawHead.length - 1)

      if (headNode === null) { // already yielded
        popAndSwap(node.heads, node.dependencies.size)
        continue
      }

      node.dependencies.add(headNode)

      await this._addClock(node.clock, headNode)
    }

    node.clock.set(node.writer, node.length)

    return node
  }

  async getCheckpoint () {
    await this.core.update()

    let length = this.core.length
    if (length === 0) return null

    let node = await this.core.get(length - 1)
    if (node.checkpointer !== 0) {
      length -= node.checkpointer
      node = await this.core.get(length - 1)
    }

    return node.checkpoint
  }

  async _addClock (clock, node) {
    if (node.yielded) return // gc'ed
    for (const [writer, length] of node.clock) {
      if (clock.get(writer) < length && !(await this.base.system.isIndexed(writer.core.key, length))) {
        clock.set(writer, length)
      }
    }
  }
}

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

    const core = this.base.store.get({ name: 'view/' + name, exclusive: true })
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
  constructor (store, bootstrap, handlers) {
    if (Array.isArray(bootstrap)) bootstrap = bootstrap[0] // TODO: just a quick compat, lets remove soon
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
    this.system = new SystemView(this, this.store.get({ name: 'system', exclusive: true }))

    this._appending = new FIFO()

    this._applying = null

    this._needsReady = []
    this._updates = []
    this._handlers = handlers || {}

    this._bump = debounceify(this._advance.bind(this))
    this._onremotewriterchangeBound = this._onremotewriterchange.bind(this)

    this.version = 0 // todo: set version
    this._checkpointer = 0
    this._checkpoint = null

    this._openingCores = null

    this._hasApply = !!this._handlers.apply
    this._hasOpen = !!this._handlers.open
    this._hasClose = !!this._handlers.close

    this._viewStore = new LinearizedStore(this)
    this.view = null

    this._ackInterval = handlers.ackInterval || DEFAULT_ACK_INTERVAL
    this._ackThreshold = handlers.ackThreshold || DEFAULT_ACK_THRESHOLD
    this._ackTimer = null
    this._acking = false

    this.ready().catch(safetyCatch)

    this.view = this._hasOpen ? this._handlers.open(this._viewStore, this) : null
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

  async _openCores () {
    await this.store.ready()
    await this.local.ready()
    await this.system.ready()

    if (this.system.bootstrapping && !this.bootstrap) {
      this.bootstrap = this.local.key // new autobase!
    }

    if (this.local.length > 0) {
      const head = await this.local.get(this.local.length - 1)
      this._checkpointer = head.checkpoint ? 1 : head.checkpointer + 1
    }

    await this._ensureUserData(this.system.core, null)
  }

  async _open () {
    await (this._openingCores = this._openCores())
    this._reindex()
    if (this.localWriter && this._ackInterval) this._startAckTimer()
    await this._bump()
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
    this._ackTimer = new Timer(this.ack.bind(this), this._ackInterval)
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
      return this.ack()
    }
  }

  async update (opts) {
    if (!this.opened) await this.ready()

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

  async ack () {
    if (!this.localWriter || this._acking) return

    this._acking = true

    await this._bump()

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

  _ensureAll () {
    const p = []
    for (const w of this.writers) {
      if (w.next === null) p.push(w.ensureNext())
    }
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
    } else {
      core.on('append', this._onremotewriterchangeBound)
    }

    return w
  }

  _reindex (change) {
    const indexers = []

    if (this.system.bootstrapping) {
      indexers.push(this._makeWriter(this.bootstrap, 0))
      this.writers = indexers.slice()
    } else {
      for (const { key, length } of this.system.digest.writers) {
        indexers.push(this._getWriterByKey(key, length))
      }
    }

    const heads = []

    for (const head of this.system.digest.heads) {
      for (const w of indexers) {
        if (b4a.equals(w.core.key, head.key)) {
          const headNode = Linearizer.createNode(w, head.length, null, [], 1, [])
          headNode.yielded = true
          heads.push(headNode)
        }
      }
    }

    const clock = this.system.digest.writers.map(w => {
      const writer = this._getWriterByKey(w.key)
      return { writer, length: w.length }
    })

    this.linearizer = new Linearizer(indexers, heads, clock)

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
        if (node.clock.get(writer) < link.length) {
          node.clock.set(writer, link.length)
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
    let active = true
    let added = 0

    while (active && added < 50) { // 50 here is just to reduce the bulk batches
      await this._ensureAll()

      active = false
      for (const w of this.writers) {
        if (!w.next) continue

        while (w.next) {
          const node = w.advance()
          this.linearizer.addHead(node)
          if (node.batch === 1) {
            added++
            break
          }
        }

        active = true
        break
      }
    }
  }

  async _advance () {
    while (true) {
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

      if (this.localWriter !== null && this.localWriter.length > this.local.length) {
        await this._flushLocal()
      }

      if (!changed) break

      this._reindex(changed)
    }

    // skip threshold check while acking
    if (this.localWriter && this._ackThreshold && !this._acking) {
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

  _onsystemappend (blocks) {
    assert(this._applying !== null, 'System changes are only allowed in apply')

    this._applying.system += blocks
  }

  // triggered from system
  _onaddwriter (key) {
    for (const w of this.writers) {
      if (b4a.equals(w.core.key, key)) return
    }

    this.writers.push(this._makeWriter(key, 0))
    this._pendingWriters = true

    // fetch any nodes needed for dependents
    this._bump()
  }

  _undo (popped) {
    const truncating = []
    let systemPop = 0

    while (popped > 0) {
      const u = this._updates.pop()

      popped -= u.batch
      systemPop += u.system

      for (const { core, appending } of u.user) {
        if (core.truncating === 0) truncating.push(core)
        core.truncating += appending
      }
    }

    if (systemPop > 0) {
      this.system._onundo(systemPop)
    }

    for (const core of truncating) {
      const truncating = core.truncating
      core.truncating = 0
      core._onundo(truncating)
    }
  }

  _bootstrap () {
    this.system.addWriter(this.bootstrap)
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
      if (update.system === 0) continue

      await this._flushAndCheckpoint(i, node.indexed)

      const nodes = u.indexed.slice(i).concat(u.tip)
      return { count: u.shared - i, nodes }
    }

    for (i = u.shared; i < u.length; i++) {
      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      if (indexed) {
        node.writer.indexed++

        // local flushed in _flushLocal
        if (node.writer !== this.localWriter) {
          node.writer.shift().clear()
        }
      }

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
          // todo: recover/shutdown?
          this.emit('error', err)
          return null
        }
      }

      this._applying = null

      batch = []
      applyBatch = []

      for (let k = 0; k < update.user.length; k++) {
        const u = update.user[k]
        u.appending = u.core.appending
        u.core.appending = 0
      }

      if (update.system > 0 && indexed) {
        await this._flushAndCheckpoint(i + 1, node.indexed)

        const nodes = u.indexed.slice(i + 1).concat(u.tip)
        return { count: 0, nodes }
      }
    }

    if (u.indexed.length) {
      await this._flushAndCheckpoint(u.indexed.length, u.indexed[u.indexed.length - 1].indexed)
    }

    return null
  }

  async _flushAndCheckpoint (indexed, heads) {
    const checkpoint = await this._flushIndexes(indexed, heads)

    if (checkpoint === null) return

    this._checkpoint = checkpoint
    this._checkpointer = 0
  }

  async _flushIndexes (indexed, heads) {
    const updatedCores = []
    let updatedSystem = 0

    while (indexed > 0) {
      const u = this._updates.shift()
      const user = []

      indexed -= u.batch
      updatedSystem += u.system

      for (const { core, appending } of u.user) {
        const start = core.indexing
        const blocks = core.indexBatch(start, core.indexing += appending)
        if (start === 0) updatedCores.push(core)

        await core.core.append(blocks)

        const tree = core.core.core.tree

        user.push({
          name: core.name,
          treeHash: tree.hash(),
          length: tree.length
        })
      }

      await this.system.flush(u.system, user, this.writers, heads)
    }

    for (const core of updatedCores) {
      const indexing = core.indexing
      core.indexing = 0
      core._onindex(indexing)
    }

    if (updatedSystem) {
      this.system._onindex(updatedSystem)
    }

    return this.system.checkpoint()
  }

  _flushLocal () {
    if (this.system.length > 0 && this._checkpoint === null && this._checkpointer === 0) {
      this._checkpoint = this.system.checkpoint()
    }

    const blocks = new Array(this.localWriter.length - this.local.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch, yielded } = this.localWriter.getCached(this.local.length + i)

      if (yielded) this.localWriter.shift().clear()

      blocks[i] = {
        version: this.version,
        checkpointer: this._checkpointer,
        checkpoint: this._checkpointer === 0 ? this._checkpoint : null,
        node: {
          heads,
          abi: 0,
          batch,
          value: value === null ? null : c.encode(this.valueEncoding, value)
        }
      }

      if (this._checkpointer > 0 || this._checkpoint !== null) {
        this._checkpointer++
        this._checkpoint = null
      }
    }

    return this.local.append(blocks)
  }
}

function toKey (k) {
  return b4a.isBuffer(k) ? k : b4a.from(k, 'hex')
}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
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
  if (msg.checkpointer) return !msg.checkpoint
  return msg.checkpoint && msg.checkpoint.length > 0
}
