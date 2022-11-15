const b4a = require('b4a')
const debounceify = require('debounceify')

class Clock {
  constructor () {
    this.seen = new Map()
  }

  get size () {
    return this.seen.size
  }

  contains (w, seq) {
    return this.get(w) > seq
  }

  get (w) {
    return this.seen.get(w) || 0
  }

  set (w, len) {
    this.seen.set(w, len)
    return len
  }

  subtract (clock) {
    for (const [w, len] of this.seen) {
      if (clock.get(w) >= len) this.seen.delete(w)
    }
  }

  add (clock, minClock = null) {
    for (const [w, len] of clock.seen) {
      if (this.get(w) < len && (minClock === null || minClock.get(w) < len)) {
        this.seen.set(w, len)
      }
    }
  }
}

class Writer {
  constructor (id, core) {
    this.id = id
    this.core = core
    this.nodes = []
    this.length = 0
    this.next = null
  }

  getCached (seq) {
    return this.nodes[seq]
  }

  advance (node = this.next) {
    this.nodes.push(node)
    this.next = null
    this.length++
  }

  async ensureNext () {
    if (this.next !== null || !(await this.core.has(this.length))) return this.next
    this.next = await this.get(this.length)
    return this.next
  }

  async get (seq) {
    if (seq >= this.core.length || seq < 0) return null

    const blk = await this.core.get(seq)
    return toNode(blk, seq, this)
  }
}

module.exports = class Autobase {
  constructor (store, { apply } = {}) {
    this.sparse = false
    this.store = store
    this.locked = store.get({ name: 'locked', valueEncoding: 'json' })
    this.local = store.get({ name: 'local', valueEncoding: 'json' })

    this.heads = []
    this.tails = []

    this._appending = []
    this._latestIndex = null
    this._latestCheckpoint = 0
    this._indexClock = new Clock()
    this._indexBatch = []
    this._writers = []
    this._writersByKey = new Map()
    this._localWriter = null
    this._quorum = 0
    this._advanceDebounced = debounceify(this._advance.bind(this))
  }

  async update () {
    await this.ready()

    for (const w of this._writers) {
      await w.core.update()
    }
  }

  async ready () {
    await this.store.ready()

    await this.locked.ready()
    await this.local.ready()

    for (const w of this._writers) {
      await w.core.ready()

      if (b4a.equals(this.local.key, w.core.key)) {
        this._localWriter = w
        break
      }
    }

    // TODO: build initial heads/tails here
  }

  setWriters (writers) {
    this._writers = []
    this._writersByKey.clear()
    this._localWriter = null
    this._quorum = Math.floor(writers.length / 2) + 1

    for (const key of writers) {
      const core = this.store.get({ key, valueEncoding: 'json', sparse: this.sparse })
      const w = new Writer(this._writers.length, core)

      this._writersByKey.set(b4a.toString(key, 'hex'), w)
      this._writers.push(w)

      if (b4a.equals(this.local.key, key)) {
        this._localWriter = w
      }
    }
  }

  async bump () {
    await this._advanceDebounced()
  }

  async append (value) {
    await this.ready()

    const blk = {
      value,
      heads: this.heads.map(toLink),
      checkpoint: 0,
      index: null,
      clock: null
    }

    const node = toNode(blk, this.local.length + this._appending.length, this._localWriter)

    this._appending.push(node)

    await this._advanceDebounced()
  }

  _ensureAll () {
    const p = []
    for (const w of this._writers) {
      if (w.next === null) p.push(w.ensureNext())
    }
    return Promise.allSettled(p)
  }

  async _advance () {
    await this.ready()

    let active = true

    while (active) {
      active = false

      await this._ensureAll()

      for (const w of this._writers) {
        const node = w.next
        if (node === null || !this._addNext(node)) continue

        w.advance()
        active = true

        if (node.clock.size >= this._quorum) {
          this._shiftQuorum(node.clock)
        }
      }
    }

    // Locally appending blocks...
    for (let i = 0; i < this._appending.length; i++) {
      const node = this._appending[i]

      this._addNext(node)
      this._localWriter.advance(node)

      if (node.clock.size >= this._quorum) {
        this._shiftQuorum(node.clock)
        if (this._indexBatch.length) await this._flushIndex()
      }
    }

    if (this._appending.length) {
      const batch = new Array(this._appending.length)

      for (let i = 0; i < this._appending.length; i++) {
        const blk = this._appending[i].block
        blk.checkpoint = this._latestCheckpoint
        batch[i] = blk
      }

      this._appending = []

      if (this._latestIndex) {
        const head = batch[batch.length - 1]

        head.index = this._latestIndex
        head.checkpoint = this._latestCheckpoint = this.local.length + batch.length

        this._latestIndex = null
      }

      await this.local.append(batch)
    }

    if (this._indexBatch.length) {
      await this._flushIndex()
    }

    console.log('done', this._indexBatch, this._latestIndex)
  }

  async _flushIndex () {
    await this.locked.append(this._indexBatch)

    this._indexBatch = []
    this._latestIndex = {
      system: null,
      user: [{
        length: this.locked.core.tree.length,
        treeHash: this.locked.core.tree.hash()
      }]
    }
  }

  _seenBy (node, clock) {
    let cnt = 0

    for (const [writer, length] of clock.seen) {
      const head = writer.getCached(length - 1)
      if (head.clock.contains(node.writer, node.seq)) cnt++
    }

    return cnt
  }

  _containsTail (node) {
    for (const tail of this.tails) {
      if (node.clock.contains(tail.writer, tail.seq)) return true
    }
    return false
  }

  _shiftQuorum (clock) {
    while (this.tails.length) {
      let b = 0
      let best = this.tails[0]
      let bestAcks = this._seenBy(best, clock)

      for (let i = 1; i < this.tails.length; i++) {
        const node = this.tails[i]
        const nodeAcks = this._seenBy(node, clock)

        if (nodeAcks > bestAcks || (bestAcks === nodeAcks && node.key < best.key)) {
          b = i
        }
      }

      if (bestAcks < this._quorum) return

      this.tails[b] = this.tails[this.tails.length - 1]
      this.tails.pop()

      for (const d of best.dependents) {
        if (!this._containsTail(d)) this.tails.push(d)

        const i = d.dependencies.indexOf(best)
        if (i < d.dependencies.length - 1) d.dependencies[i] = d.dependencies.pop()
        else d.dependencies.pop()
      }

      this._indexBatch.push(best.block)
      this._indexClock.add(best.clock)
    }
  }

  _isTail (node) {
    for (const d of node.dependencies) {
      if (!this._indexClock.contains(d.writer, d.seq)) return false
    }
    return true
  }

  _addNext (node) {
    for (let i = node.dependencies.length; i < node.block.heads.length; i++) {
      const link = node.block.heads[i]
      const writer = this._writersByKey.get(link.key)

      if (writer.length < link.length) {
        return false
      }

      node.dependencies.push(writer.getCached(link.length - 1))
    }

    this.heads.push(node)
    if (this._isTail(node)) this.tails.push(node)

    node.clock = new Clock()
    node.clock.set(node.writer, node.seq + 1)

    for (const dep of node.dependencies) {
      node.clock.add(dep.clock, this._indexClock)
      dep.dependents.push(node)

      // TODO: track this index on dep itself for constant time but also might not matter
      const i = this.heads.indexOf(dep)
      if (i === -1) continue

      this.heads[i] = this.heads.pop()
    }

    return true
  }
}

function toNode (block, seq, writer) {
  return {
    writer,
    seq,
    block,
    clock: null,
    dependents: [],
    dependencies: []
  }
}

function toLink (node) {
  return {
    key: node.writer.core.key.toString('hex'),
    length: node.seq + 1
  }
}
