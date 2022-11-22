const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const debounceify = require('debounceify')

class Clock {
  constructor () {
    this.seen = new Map()
  }

  get size () {
    return this.seen.size
  }

  has (w) {
    return this.seen.has(w)
  }

  get (w) {
    return this.seen.get(w) || 0
  }

  set (w, len) {
    this.seen.set(w, len)
    return len
  }

  [Symbol.iterator] () {
    return this.seen[Symbol.iterator]()
  }
}

class Writer {
  constructor (base, core) {
    this.base = base
    this.core = core
    this.nodes = []
    this.length = 0

    this.next = null
    this.nextCache = null
  }

  compare (writer) {
    return b4a.compare(this.core.key, writer.core.key)
  }

  head () {
    return this.length === 0 ? null : this.nodes[this.length - 1]
  }

  getCached (seq) {
    return this.nodes[seq]
  }

  advance (node = this.next) {
    this.nodes.push(node)
    this.next = null
    this.nextCache = null
    this.length++
    return node
  }

  append (value, heads) {
    const node = this._createNode(this.length + 1, value, [], heads)

    for (const head of heads) {
      this._addClock(node.clock, head)
      if (head.writer === this) continue
      node.rawHeads.push({
        key: b4a.toString(head.writer.core.key, 'hex'),
        length: head.length
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
      this.nextCache = this._createNode(this.length + 1, block.value, block.heads, [])
    }

    this.next = await this.ensureNode(this.nextCache)
    return this.next
  }

  _createNode (length, value, rawHeads, heads) {
    return {
      writer: this,
      length,
      rawHeads,
      heads,
      value,
      clock: new Clock()
    }
  }

  async ensureNode (node) {
    while (node.heads.length < node.rawHeads.length) {
      const rawHead = node.rawHeads[node.heads.length]

      const headWriter = await this.base._getWriterByKey(rawHead.key)
      if (headWriter.length < rawHead.length) {
        return null
      }

      const headNode = headWriter.getCached(rawHead.length - 1)

      if (headNode === null) { // already yielded
        const last = node.rawHeads.pop()
        if (last !== rawHead) node.rawHeads[node.heads.length] = last
        continue
      }

      node.heads.push(headNode)
      this._addClock(node.clock, headNode)
    }

    node.clock.set(node.writer, node.length)

    return node
  }

  _addClock (clock, node) {
    for (const [writer, length] of node.clock) {
      if (clock.get(writer) < length && this.base.pending.clock.get(writer) < length) {
        clock.set(writer, length)
      }
    }
  }
}

class PendingNodes {
  constructor (indexers) {
    this.tails = []
    this.heads = []
    this.clock = new Clock()
    this.indexers = indexers
  }

  setIndexers (indexers) {
    this.indexers = indexers
  }

  addHead (node) {
    let tail = true

    for (let i = 0; i < node.heads.length; i++) {
      if (node.heads[i].length) {
        tail = false
        break
      }
    }

    for (let i = 0; i < this.heads.length; i++) {
      const head = this.heads[i]

      if (node.clock.get(head.writer) >= head.length) {
        if (i === this.heads.length - 1) this.heads.pop()
        else this.heads[i--] = this.heads.pop()
      }
    }

    if (tail) this.tails.push(node)
    this.heads.push(node)
  }

  shift () {
    const all = new Map()
    const majority = Math.floor(this.indexers.length / 2) + 1

    for (const writer of this.indexers) {
      const tally = this._votesFor(writer)

      if (tally && tally.votes >= majority) {
        const cnt = all.get(tally.writer) || 0
        all.set(tally.writer, cnt + 1)
      }
    }

    for (const [writer, total] of all) {
      if (total >= majority) return writer
    }

    return null
  }


  _votesFor (writer) {
    const h = writer.head()
    if (!h) return null

    let best = null

    for (const [cand] of h.clock) {
      const c = this._countVotes(writer, cand)

      if (best === null || c > best.votes || (c === best.votes && best.writer.compare(cand) > 0)) {
        best = { writer: cand, votes: c }
      }
    }

    return best
  }

  _countVotes (writer, cand) {
    let votes = 0

    const h = writer.head()

    if (!h) return 0

    for (const [writer, length] of h.clock) {
      const node = writer.getCached(length - 1)

      if (node !== null && node.clock.has(cand)) votes++
    }

    return votes
  }
}

module.exports = class Autobase extends ReadyResource {
  constructor (store, bootstraps) {
    super()

    this.sparse = false

    this.store = store
    this.pending = new PendingNodes([])
    this.local = store.get({ name: 'local', valueEncoding: 'json' })
    this.localWriter = new Writer(this, this.local)
    this.bootstraps = [].concat(bootstraps || []).map(toKey)

    this._appending = []
    this._bump = debounceify(this._advance.bind(this))

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
console.log('append', value.debug)
    this._appending.push(value)

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
      for (const value of this._appending) {
        const node = this.localWriter.append(value, this.pending.heads.slice(0))
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

      const node = this.pending.shift()
      if (node) {
        console.log('node', !!node)
      }
    }


    if (this.localWriter.length > this.local.length) {
      await this._flushLocal()
    }
  }

  _flushLocal () {
    const batch = new Array(this.localWriter.length - this.local.length)

    for (let i = 0; i < batch.length; i++) {
      const { value, rawHeads } = this.localWriter.getCached(this.local.length + i)

      batch[i] = {
        value,
        heads: rawHeads
      }
    }

    return this.local.append(batch)
  }
}

function noop () {}

function toKey (k) {
  return b4a.isBuffer(k) ? k : b4a.from(k, 'hex')
}

function toLink (node) {
  return {
    key: b4a.toString(node.writer.core.key, 'hex'),
    length: node.length
  }
}
