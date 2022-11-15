const b4a = require('b4a')
const debounceify = require('debounceify')
const ReadyResource = require('ready-resource')
const Hyperbee = require('hyperbee')

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
  constructor (base, id, core) {
    this.base = base
    this.id = id
    this.core = core
    this.nodes = []
    this.length = 0
    this.next = null
    this.checkpointAt = 0
  }

  compare (writer) {
    return b4a.compare(this.core.key, writer.core.key)
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

  async latestCheckpoint () {
    await this.core.update()
    if (this.core.length === 0) return null
    const seq = this.core.length - 1
    const blk = await this.core.get(seq)
    if (blk.checkpointOffset === 0) return blk.checkpoint
    return (await this.core.get(seq - blk.checkpointOffset)).checkpoint
  }
}

module.exports = class Autobase extends ReadyResource {
  constructor (store, key, opts) {
    super()

    this.sparse = false
    this.store = store

    this.locked = store.get({ name: 'locked', valueEncoding: 'json' })
    this.local = store.get({ name: 'local', valueEncoding: 'json' })
    this.system = new Hyperbee(store.get({ name: 'system' }), { valueEncoding: 'json' })

    this.heads = []
    this.tails = []

    this._appending = []
    this._checkpoint = true
    this._checkpointOffset = 0
    this._checkpointClock = new Clock()
    this._checkpointBatch = []

    this._systemNeedsUpdate = true

    this._writers = []
    this._writersQuorum = new Set()
    this._writersByKey = new Map()
    this._localWriter = null
    this._quorum = 0

    this._linearizeCache = null
    this._bump = debounceify(this._advance.bind(this))
    this._bootstrapKey = key || null

    this._apply = (opts && opts.apply) || null

    this.ready().catch(noop)
  }

  async update () {
    await this.ready()

    for (const w of this._writers) {
      await w.core.update()
    }

    await this._bump()
  }

  async _open () {
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

    this._addWriter(this._bootstrapKey || this.local.key, true)

    // TODO: build initial heads/tails here
  }

  get length () {
    // TODO: kinda silly but whatevs
    if (this._linearizeCache === null) {
      this._linearizeCache = this._linearize(this._latestClock())
    }

    return this.locked.length + this._linearizeCache.length
  }

  async get (seq) {
    await this._bump()

    if (seq < this.locked.length) return this.locked.get(seq)

    seq -= this.locked.length

    if (this._linearizeCache === null) {
      this._linearizeCache = this._linearize(this._latestClock())
    }

    if (seq >= this._linearizeCache.length) throw new Error('Out of bounds')

    return this._linearizeCache[seq]
  }

  _latestClock () {
    const clock = new Clock()

    for (const head of this.heads) {
      clock.add(head.clock, null)
    }

    return clock
  }

  addWriter (key) {
    this._addWriter(key, false)
    this._systemNeedsUpdate = true
  }

  _addWriter (key, bootstrap = false) {
    const hex = b4a.toString(key, 'hex')

    if (this._writersByKey.has(hex)) return

    const core = this.store.get({ key, valueEncoding: 'json', sparse: this.sparse })
    const w = new Writer(this, this._writers.length, core)

    this._writersByKey.set(hex, w)
    this._writers.push(w)

    if (b4a.equals(this.local.key, key)) {
      this._localWriter = w
    }

    if (bootstrap) {
      w.checkpointAt = 1

      this._writersQuorum.add(w)
      this._updateQuorum()
    }
  }

  removeWriter (key) {

  }

  async latestCheckpoint () {
    let latest = null
    let len = 0

    for (const w of this._writers) {
      const c = await w.latestCheckpoint()
      if (c === null) continue

      let l = 0
      for (const { length } of c.clock) l += length

      if (latest === null || l > len) {
        latest = c
        len = l
      }
    }

    return latest
  }

  _updateQuorum () {
    this._quorum = Math.floor(this._writersQuorum.size / 2) + 1
  }

  setWriters (writers) {
    this._writers = []
    this._writersByKey.clear()
    this._localWriter = null
    this._quorum = 0

    for (const key of writers) {
      this._addWriter(key)
    }
  }

  async append (value) {
    if (!this.opened) await this.ready()

    const blk = {
      value,
      heads: this.heads.map(toLink),
      quorumable: this._localWriter.checkpointAt > 0,
      checkpointOffset: 0,
      checkpoint: null
    }

    const node = toNode(blk, this.local.length + this._appending.length, this._localWriter)

    this._appending.push(node)

    await this._bump()
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
      }
    }

    if (this._checkpointBatch.length) {
      await this._apply(this._checkpointBatch, this)

      this._checkpointBatch = []
      this._checkpoint = true

      await this._updateSystem()
    }

    if (this.tails.length) {
      if (this._linearizeCache === null) {
        this._linearizeCache = this._linearize(this._latestClock())
      }

      await this._apply(this._linearizeCache, this)
    } else {
      this._linearizeCache = null
    }

    // TODO: fix error handling here (ie _checkpointOffset/checkout resets)

    if (this._appending.length) {
      const batch = new Array(this._appending.length)

      for (let i = 0; i < this._appending.length; i++) {
        const blk = this._appending[i].block
        blk.checkpointOffset = this._checkpointOffset === 0 ? 0 : this._checkpointOffset++
        batch[i] = blk
      }

      this._appending = []

      if (this._checkpoint) {
        const head = batch[batch.length - 1]

        head.checkpoint = {
          clock: [...this._checkpointClock.seen].map(([writer, length]) => ({ key: writer.core.key.toString('hex'), length })),
          system: snapshot(this.system.feed),
          user: []
        }

        head.checkpointOffset = 0

        this._checkpoint = false
        this._checkpointOffset = 1
      }

      await this.local.append(batch)
    }
  }

  async _updateSystem () {
    const b = this.system.batch()
    const checkpoints = []

    for (const w of this._writers) {
      if (w.checkpointAt === 0) {
        await w.core.ready()
        await b.put(b4a.toString(w.core.key, 'hex'), null)
        checkpoints.push(b.length)
      }
    }

    await b.flush()

    // TODO: do a diff instead eventually...
    let i = 0
    for (const w of this._writers) {
      if (w.checkpointAt === 0) {
        w.checkpointAt = checkpoints[i++]
        this._writersQuorum.add(w)
      }
    }

    this._updateQuorum()
  }

  // async _flushIndex () {
  //   await this.locked.append(this._checkpointBatch)

  //   this._checkpointBatch = []
  //   this._checkpoint = {
  //     system: null,
  //     user: [{
  //       length: this.locked.core.tree.length,
  //       treeHash: this.locked.core.tree.hash()
  //     }]
  //   }
  // }

  _seenBy (node, clock) {
    let cnt = 0

    for (const [writer, length] of clock.seen) {
      const head = writer.getCached(length - 1)
      if (head.block.quorumable && head.clock.contains(node.writer, node.seq)) cnt++
    }

    return cnt
  }

  _containsTail (node, tails) {
    for (const tail of tails) {
      if (node.clock.contains(tail.writer, tail.seq)) return true
    }
    return false
  }

  _linearize (clock) {
    console.log('rebuliding _linearizeCache')
    const tails = [...this.tails]
    const result = []

    while (tails.length) {
      let b = 0
      let best = tails[0]
      let bestAcks = this._seenBy(best, clock)

      for (let i = 1; i < tails.length; i++) {
        const node = tails[i]
        const nodeAcks = this._seenBy(node, clock)

        if (nodeAcks > bestAcks || (bestAcks === nodeAcks && node.writer.compare(best.writer) < 0)) {
          b = i
          best = tails[i]
          bestAcks = nodeAcks
        }
      }

      result.push(best.block)

      tails[b] = tails[tails.length - 1]
      tails.pop()

      for (const d of best.dependents) {
        if (!this._containsTail(d, tails)) tails.push(d)
      }
    }

    return result
  }

  _shiftQuorum (clock) {
    while (this.tails.length) {
      let b = 0
      let best = this.tails[0]
      let bestAcks = this._seenBy(best, clock)

      for (let i = 1; i < this.tails.length; i++) {
        const node = this.tails[i]
        const nodeAcks = this._seenBy(node, clock)

        if (nodeAcks > bestAcks || (bestAcks === nodeAcks && node.writer.compare(best.writer) < 0)) {
          b = i
          best = this.tails[i]
          bestAcks = nodeAcks
        }
      }

      if (bestAcks < this._quorum) return

      this.tails[b] = this.tails[this.tails.length - 1]
      this.tails.pop()

      for (const d of best.dependents) {
        if (!this._containsTail(d, this.tails)) this.tails.push(d)

        const i = d.dependencies.indexOf(best)
        if (i < d.dependencies.length - 1) d.dependencies[i] = d.dependencies.pop()
        else d.dependencies.pop()
      }

      this._checkpointBatch.push(best.block)
      this._checkpointClock.add(best.clock)
    }
  }

  _isTail (node) {
    for (const d of node.dependencies) {
      if (!this._checkpointClock.contains(d.writer, d.seq)) return false
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
      node.clock.add(dep.clock, this._checkpointClock)
      dep.dependents.push(node)

      // TODO: track this index on dep itself for constant time but also might not matter
      const i = this.heads.indexOf(dep)
      if (i === -1) continue

      this.heads[i] = this.heads.pop()
    }

    if (this.heads.length !== 1) {
      console.log('forks exists')
      this._linearizeCache = null
    } else if (this._linearizeCache !== null) {
      this._linearizeCache.push(node.block)
    }

    return true
  }
}

function snapshot (core) {
  return {
    length: core.core.tree.length,
    treeHash: core.core.tree.hash().toString('hex')
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

function noop () {}
