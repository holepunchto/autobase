const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')

const Clock = require('./lib/clock')
const Linearizer = require('./lib/linearizer')
const LinearizedCore = require('./lib/core')

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
    const node = this._createNode(this.length + 1, value, [], batch, dependencies)

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
      this.nextCache = this._createNode(this.length + 1, block.value, block.heads, block.batch, [])
    }

    this.next = await this.ensureNode(this.nextCache)
    return this.next
  }

  _createNode (length, value, heads, batch, dependencies) {
    return {
      writer: this,
      length,
      heads,
      dependents: [],
      dependencies,
      value,
      batch,
      clock: new Clock()
    }
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

module.exports = class Autobase extends ReadyResource {
  constructor (store, bootstraps, handlers) {
    super()

    this.sparse = false

    this.store = store
    this.linearizer = new PendingNodes([])
    this.local = store.get({ name: 'local', valueEncoding: 'json' })
    this.localWriter = new Writer(this, this.local)
    this.bootstraps = [].concat(bootstraps || []).map(toKey)

    this._appending = []
    this._handlers = handlers || {}
    this._bump = debounceify(this._advance.bind(this))

    this._hasApply = !!this._handlers.apply

    this.viewCore = store.get({ name: 'view/0', valueEncoding: 'json' })
    this.view = new LinearizedCore(this.viewCore)

    this.ready().catch(noop)
  }

  async _open () {
    await this.store.ready()
    await this.local.ready()

    let writers = []
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

    this.pending.setIndexers(writers)
  }

  async update () {
    if (!this.opened) await this.ready()

    for (const w of this.pending.indexers) {
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
    for (const w of this.pending.indexers) {
      if (b4a.toString(w.core.key, 'hex') === key) return w
    }

    throw new Error('Unknown writer')
  }

  _ensureAll () {
    const p = []
    for (const w of this.pending.indexers) {
      if (w.next === null) p.push(w.ensureNext())
    }
    return Promise.all(p)
  }

  async _advance () {
    if (this._appending.length) {
      for (let i = 0; i < this._appending.length; i++) {
        const value = this._appending[i]
        const heads = this.pending.heads.slice(0)
        const node = this.localWriter.append(value, heads, this._appending.length - i)
        this.pending.addHead(node)
      }
      this._appending = []
    }

    let active = true

    while (active) {
      await this._ensureAll()

      active = false
      for (const w of this.pending.indexers) {
        if (!w.next) continue
        this.pending.addHead(w.advance())
        active = true
        break
      }
    }

    const u = this.pending.update()

    if (u) {
      await this._applyUpdate(u)
    }

    if (this.localWriter.length > this.local.length) {
      await this._flushLocal()
    }
  }

  async _applyUpdate (u) {
    if (u.popped) {
      this.view.truncate(u.shared)
    }

    let batch = []
    let missing = 1

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

      if (this._hasApply === true) await this._handlers.apply(batch, this.view, this)
      batch = []
    }
  }

  _flushLocal () {
    const blocks = new Array(this.localWriter.length - this.local.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch } = this.localWriter.getCached(this.local.length + i)

      blocks[i] = {
        value,
        heads,
        batch
      }
    }

    return this.local.append(blocks)
  }
}

function noop () {}

function clearNode (node) {
  node.length = 0
  node.dependencies = null
  node.dependents = null
  node.clock = null
  node.writer = null
}

function toKey (k) {
  return b4a.isBuffer(k) ? k : b4a.from(k, 'hex')
}

function toLink (node) {
  return {
    key: b4a.toString(node.writer.core.key, 'hex'),
    length: node.length
  }
}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}

function removeTail (tail, tails) {
  popAndSwap(tails, tails.indexOf(tail))

  const tailsLen = tails.length

  for (const dep of tail.dependents) {
    let isTail = true

    for (let i = 0; i < tailsLen; i++) {
      const t = tails[i]

      if (dep.clock.get(t.writer) >= t.length) {
        isTail = false
        break
      }
    }

    if (isTail) tails.push(dep)
  }
}
