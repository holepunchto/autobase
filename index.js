const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')

const Linearizer = require('./lib/linearizer')
const LinearizedCore = require('./lib/core')
const SystemView = require('./lib/system')

const inspect = Symbol.for('nodejs.util.inspect.custom')

class Writer {
  constructor (base, core, length) {
    this.base = base
    this.core = core
    this.nodes = []
    this.length = length
    this.offset = length

    this.indexed = 0

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
      if (dep.clock !== null) {
        for (const [writer, length] of dep.clock) {
          if (node.clock.get(writer) < length) {
            node.clock.set(writer, length)
          }
        }
      }

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
    if (node.clock === null) return // gc'ed
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
    this.bootstraps = [].concat(bootstraps || []).map(toKey).sort((a, b) => b4a.compare(a, b))
    this.store = store

    this.local = store.get({ name: 'local', valueEncoding: 'json' })
    this.localWriter = new Writer(this, this.local, 0)
    this.linearizer = new Linearizer([], [])
    this.system = new SystemView(this, store.get({ name: 'system' }))

    this._appending = []
    this._applying = null
    this._updates = []
    this._handlers = handlers || {}
    this._modified = false
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

      writers.push(new Writer(this, core, 0))
    }

    if (writers.length === 0) {
      writers.push(this.localWriter)
      this.bootstraps.push(this.local.key)
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

  async checkpoint () {
    await this.ready()
    const all = []

    for (const w of this.linearizer.indexers) {
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

  async _restart () {
    // TODO: close old...

    this.localWriter = null

    const indexers = []

    for (const { key, length } of this.system.digest.indexers) {
      const k = b4a.from(key, 'hex')
      const local = b4a.equals(k, this.local.key)

      const core = local
        ? this.local.session({ valueEncoding: 'json' })
        : this.store.get({ key: k, sparse: this.sparse, valueEncoding: 'json' })

      await core.ready()

      const w = new Writer(this, core, length)

      if (local) this.localWriter = w
      indexers.push(w)
    }

    if (!this.localWriter) {
      const core = this.local.session()

      await core.ready()

      this.localWriter = new Writer(this, core, 0)
    }

    const heads = []

    for (const head of this.system.digest.heads) {
      for (const w of indexers) {
        if (b4a.toString(w.core.key, 'hex') === head.key) {
          heads.push(Linearizer.createNode(w, head.length, null, [], 1, []))
        }
      }
    }

    this.linearizer = new Linearizer(indexers, heads)

    // TODO: this is a bit silly (hitting it with the biggest of hammers)
    // but an easy fix for now so cores are "up to date"
    this._undo(this._updates.length)

    this._modified = true
  }

  async _advance () {
    do {
      this._modified = false

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
      const needsRestart = u ? await this._applyUpdate(u) : false

      if (this.localWriter.length > this.local.length) {
        await this._flushLocal()
      }

      if (needsRestart) {
        await this._restart()
      }
    } while (this._modified === true)
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
      const truncating = core.truncating
      core.truncating = 0
      core._onundo(truncating)
    }
  }

  async _applyUpdate (u) {
    if (u.popped) this._undo(u.popped)

    let batch = []
    let j = 0

    for (let i = 0; i < Math.min(u.indexed.length, u.shared); i++) {
      const node = u.indexed[i]

      node.writer.indexed++
      if (node.batch > 1) continue

      const update = this._updates[j++]
      if (update.system === 0) continue

      await this._flushAndCheckpoint(i + 1, node.indexed)
      return true
    }

    for (let i = u.shared; i < u.length; i++) {
      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      if (indexed) {
        node.writer.indexed++
      }

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
      if (this.system.bootstrapping) this._bootstrap()
      if (this._hasApply === true) await this._handlers.apply(batch, this.view, this)
      this._applying = null

      batch = []

      for (let i = 0; i < update.user.length; i++) {
        const u = update.user[i]
        u.appending = u.core.appending
        u.core.appending = 0
      }

      if (update.system > 0 && indexed) {
        await this._flushAndCheckpoint(i + 1, node.indexed)
        return true
      }
    }

    if (u.indexed.length) {
      await this._flushAndCheckpoint(u.indexed.length, u.indexed[u.indexed.length - 1].indexed)
    }

    return false
  }

  async _flushAndCheckpoint (indexed, heads) {
    const checkpoint = await this._flushIndexes(indexed, heads)

    if (checkpoint === null) return

    this._checkpoint = checkpoint
    this._checkpointer = 0
  }

  _bootstrap () {
    for (const key of this.bootstraps) {
      this.system.addWriter(key)
    }
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

      await this.system.flush(u.system, user, this.linearizer.indexers, heads)
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
