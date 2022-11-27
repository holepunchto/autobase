const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')

const Linearizer = require('./lib/linearizer')
const LinearizedCore = require('./lib/core')
const SystemView = require('./lib/system')

const inspect = Symbol.for('nodejs.util.inspect.custom')

class Writer {
  constructor (base, core) {
    this.base = base
    this.core = core
    this.nodes = []
    this.length = 0
    this.offset = 0

    this.next = null
    this.nextCache = null
  }

  compare (writer) {
    return b4a.compare(this.core.key, writer.core.key)
  }

  head () {
    const len = this.length - this.offset
    return len === 0 ? null : this.nodes[len - 1]
  }

  shift () {
    if (this.offset === this.length) return null
    this.offset++
    return this.nodes.shift()
  }

  getCached (seq) {
    return seq >= this.offset ? this.nodes[seq - this.offset] : null
  }

  advance (node = this.next) {
    this.nodes.push(node)
    this.next = null
    this.nextCache = null
    this.length++
    return node
  }

  append (value, dependencies, batch) {
    const node = Linearizer.createNode(this, this.length + 1, value, [], batch, dependencies)

    for (const dep of dependencies) {
      this._addClock(node.clock, dep)
      node.heads.push({
        key: b4a.toString(dep.writer.core.key, 'hex'),
        length: dep.length
      })
    }

    node.clock.set(node.writer, node.length)

    this.advance(node)
    return node
  }

  async ensureNext () {
    if (this.length >= this.core.length || this.core.length === 0) return null
    if (this.next !== null || !(await this.core.has(this.length))) return this.next

    if (this.nextCache === null) {
      const block = await this.core.get(this.length)
      this.nextCache = Linearizer.createNode(this, this.length + 1, block.value, block.heads, block.batch, [])
    }

    this.next = await this.ensureNode(this.nextCache)
    return this.next
  }

  async ensureNode (node) {
    while (node.dependencies.length < node.heads.length) {
      const rawHead = node.heads[node.dependencies.length]

      const headWriter = await this.base._getWriterByKey(rawHead.key)
      if (headWriter.length < rawHead.length) {
        return null
      }

      const headNode = headWriter.getCached(rawHead.length - 1)

      if (headNode === null) { // already yielded
        popAndSwap(node.heads, node.dependencies.length)
        continue
      }

      node.dependencies.push(headNode)

      this._addClock(node.clock, headNode)
    }

    node.clock.set(node.writer, node.length)

    return node
  }

  _addClock (clock, node) {
    for (const [writer, length] of node.clock) {
      if (clock.get(writer) < length && this.base.linearizer.clock.get(writer) < length) {
        clock.set(writer, length)
      }
    }
  }
}

class LinearizedStore {
  constructor (base) {
    this.base = base
    this.opened = new Map()
  }

  get (opts) {
    if (typeof opts === 'string') opts = { name: opts }

    const name = opts.name
    if (this.opened.has(name)) return this.opened.get(name)

    const core = this.base.store.get({ name: 'view/' + name, valueEncoding: 'json' })
    const l = new LinearizedCore(this.base, core, name, 0)

    this.opened.set(name, l)
    return l
  }
}

module.exports = class Autobase extends ReadyResource {
  constructor (store, bootstraps, handlers) {
    super()

    this.sparse = false

    this.store = store
    this.linearizer = new Linearizer([])
    this.local = store.get({ name: 'local', valueEncoding: 'json' })
    this.localWriter = new Writer(this, this.local)
    this.system = new SystemView(this, store.get({ name: 'system' }))
    this.bootstraps = [].concat(bootstraps || []).map(toKey)

    this._appending = []
    this._applying = null
    this._updates = []
    this._handlers = handlers || {}
    this._bump = debounceify(this._advance.bind(this))

    this._checkpointer = 0
    this._checkpoint = null

    this._hasApply = !!this._handlers.apply
    this._hasOpen = !!this._handlers.open

    this._viewStore = new LinearizedStore(this)

    this.view = this._hasOpen ? this._handlers.open(this._viewStore, this) : null

    this.ready().catch(noop)
  }

  [inspect] (depth, opts) {
    let indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    return indent + 'Autobase { ... }'
  }

  async _open () {
    await this.store.ready()
    await this.local.ready()

    const writers = []
    for (const key of this.bootstraps) {
      if (b4a.equals(key, this.local.key)) {
        writers.push(this.localWriter)
        continue
      }

      const core = this.store.get({ key, valueEncoding: 'json', sparse: this.sparse })

      await core.ready()

      writers.push(new Writer(this, core))
    }

    if (writers.length === 0) {
      writers.push(this.localWriter)
    }

    this.linearizer.setIndexers(writers)
  }

  async update () {
    if (!this.opened) await this.ready()

    for (const w of this.linearizer.indexers) {
      await w.core.update()
    }

    await this._bump()
  }

  ack () {
    return this.append(null)
  }

  async append (value) {
    if (!this.opened) await this.ready()

    if (Array.isArray(value)) this._appending.push(...value)
    else this._appending.push(value)

    await this._bump()
  }

  _getWriterByKey (key) {
    for (const w of this.linearizer.indexers) {
      if (b4a.toString(w.core.key, 'hex') === key) return w
    }

    throw new Error('Unknown writer')
  }

  _ensureAll () {
    const p = []
    for (const w of this.linearizer.indexers) {
      if (w.next === null) p.push(w.ensureNext())
    }
    return Promise.all(p)
  }

  async _advance () {
    if (this._appending.length) {
      for (let i = 0; i < this._appending.length; i++) {
        const value = this._appending[i]
        const heads = this.linearizer.heads.slice(0)
        const node = this.localWriter.append(value, heads, this._appending.length - i)
        this.linearizer.addHead(node)
      }
      this._appending = []
    }

    let active = true

    while (active) {
      await this._ensureAll()

      active = false
      for (const w of this.linearizer.indexers) {
        if (!w.next) continue
        this.linearizer.addHead(w.advance())
        active = true
        break
      }
    }

    const u = this.linearizer.update()

    if (u) {
      await this._applyUpdate(u)
    }

    if (this.localWriter.length > this.local.length) {
      await this._flushLocal()
    }
  }

  // triggered from linearized core
  _onuserappend (core, blocks) {
    if (this._applying === null) throw new Error('Append is only allowed in apply')

    if (core.appending === 0) {
      this._applying.user.push({ core, appending: 0 })
    }

    core.appending += blocks
  }

  // triggered from system
  _onsystemappend (system, blocks) {
    if (this._applying === null) throw new Error('System changes are only allowed in apply')

    this._applying.system += blocks
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

    if (systemPop > 0) this.system._onundo(systemPop)

    for (const core of truncating) {
      core.truncating = 0
      core._onundo(core.truncating)
    }
  }

  async _applyUpdate (u) {
    if (u.popped) this._undo(u.popped)

    let batch = []

    for (let i = u.shared; i < u.length; i++) {
      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      batch.push({
        indexed,
        from: node.writer.core,
        value: node.value,
        heads: node.heads
      })

      if (node.batch > 1) continue

      const update = { batch: batch.length, system: 0, user: [] }

      this._updates.push(update)
      this._applying = update
      if (this._hasApply === true) await this._handlers.apply(batch, this.view, this)
      this._applying = null

      batch = []

      for (let i = 0; i < update.user.length; i++) {
        const u = update.user[i]
        u.appending = u.core.appending
        u.core.appending = 0
      }
    }

    if (u.indexed.length === 0) return

    const checkpoint = await this._flushIndexes(u.indexed.length)

    if (checkpoint === null) return

    this._checkpoint = checkpoint
    this._checkpointer = 0
  }

  async _flushIndexes (indexed) {
    const updatedCores = []
    let updatedSystem = 0

    while (indexed > 0) {
      const u = this._updates.shift()
      const user = []

      indexed -= u.batch
      updatedSystem += u.system

      for (const { core, appending } of u.user) {
        const start = core.indexing
        const blocks = core.tip.slice(start, core.indexing += appending)
        if (start === 0) updatedCores.push(core)

        await core.core.append(blocks)

        const tree = core.core.core.tree

        user.push({
          name: core.name,
          treeHash: tree.hash(),
          length: tree.length
        })
      }

      await this.system.flush(u.system, user)
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
    const blocks = new Array(this.localWriter.length - this.local.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch } = this.localWriter.getCached(this.local.length + i)

      blocks[i] = {
        value,
        heads,
        batch,
        checkpointer: this._checkpointer,
        checkpoint: this._checkpointer === 0 ? toJSONCheckpoint(this._checkpoint) : null
      }

      if (this._checkpointer > 0 || this._checkpoint !== null) {
        this._checkpointer++
        this._checkpoint = null
      }
    }

    return this.local.append(blocks)
  }
}

function noop () {}

function toKey (k) {
  return b4a.isBuffer(k) ? k : b4a.from(k, 'hex')
}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}

function toJSONCheckpoint (c) {
  return c && { treeHash: b4a.toString(c.treeHash, 'hex'), length: c.length }
}
