const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')
const Hyperbee = require('hyperbee')
const SubEncoder = require('sub-encoder')

const Linearizer = require('./lib/linearizer')
const LinearizedCore = require('./lib/core')

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

class SystemViewBatch {
  constructor (sys) {
    this.system = sys
    this.batch = sys.db.batch()
  }

  updateIndex (index) {
    const value = { treeHash: b4a.toString(index.treeHash, 'hex'), length: index.length }
    return this.batch.put(index.name, value, { keyEncoding: this.system.subs.indexes })
  }

  flush () {
    return this.batch.flush()
  }
}

class SystemView {
  constructor (core) {
    const enc = new SubEncoder()

    this.db = new Hyperbee(core, { valueEncoding: 'json' })
    this.subs = {
      writers: enc.sub('writers'),
      indexes: enc.sub('indexes', { keyEncoding: 'utf-8' })
    }

    this.unindexedWriters = []
  }

  checkpoint () {
    const tree = this.db.feed.core.tree

    return {
      treeHash: tree.hash(),
      length: tree.length
    }
  }

  async listIndexes () {
    const all = []
    for await (const data of this.db.createReadStream(this.subs.indexes.range())) {
      all.push({ name: data.key, treeHash: b4a.from(data.value.treeHash, 'hex'), length: data.value.length })
    }
    return all
  }

  batch () {
    return new SystemViewBatch(this)
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
    this.system = new SystemView(store.get({ name: 'system' }))
    this.bootstraps = [].concat(bootstraps || []).map(toKey)
    this.applying = false

    this._appending = []
    this._updatedCores = []
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
  _oncoreappend (core) {
    if (this.applying === false) throw new Error('Append is only allowed in apply')

    if (core.appending === 0) {
      this._updatedCores.push(core)
    }

    core.appending++
  }

  _undo (popped) {
    const truncating = []

    while (popped-- > 0) {
      for (const { core, appending } of this._updates.pop()) {
        if (core.truncating === 0) truncating.push(core)
        core.truncating += appending
      }
    }

    for (const core of truncating) {
      const oldLength = core.length - core.truncating
      core.truncating = 0
      core.truncate(oldLength)
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

      if (this._hasApply === true) {
        this.applying = true
        await this._handlers.apply(batch, this.view, this)
        this.applying = false
      }

      batch = []

      const update = []
      this._updates.push(update)

      // FIXME: sort me
      while (this._updatedCores.length > 0) {
        const core = this._updatedCores.pop()

        update.push({ core, appending: core.appending })
        core.appending = 0
      }
    }

    if (u.indexed.length === 0) return

    const checkpoint = await this._appendIndexes(u.indexed.length)

    if (checkpoint === null) return

    this._checkpoint = checkpoint
    this._checkpointer = 0
  }

  async _appendIndexes (indexed) {
    const indexing = []

    while (indexed-- > 0) {
      for (const u of this._updates.shift()) {
        if (u.core.indexing === 0) {
          indexing.push(u.core)
        }

        u.core.indexing += u.appending
      }
    }

    if (!indexing.length) return null

    for (const core of indexing) {
      const blocks = core.tip.slice(0, core.indexing)

      await core.core.append(blocks)

      core.indexing = 0
      core._onindexupdate(blocks.length)
    }

    const batch = this.system.batch()

    for (const core of indexing) {
      const tree = core.core.core.tree
      const index = {
        name: core.name,
        treeHash: tree.hash(),
        length: tree.length
      }

      await batch.updateIndex(index)
    }

    await batch.flush()

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
