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

  get length () {
    let length = 0
    for (const len of this.seen.values()) length += len
    return length
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

  append (value, heads) {
    const node = this._createNode(this.length + 1, value, [], heads)

    for (const head of heads) {
      this._addClock(node.clock, head)
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
        popAndSwap(node.rawHeads, node.heads.length)
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

    this.indexed = []
    this.unindexed = []

    this.pushed = 0
    this.popped = 0
  }

  update () {
    const node = this.shift()
    if (node) {
      console.log('TODO')
      process.exit()
    }

    let sharedClock = new Clock()
    let sharedLength = 0
    let diffedLength = 0

    addClock(sharedClock, this.clock)

    const old = this.unindexed
    const visited = new Set()
    const pushed = []

    console.log('\n\npre')
    visit(this.heads)
    console.log('post\n\n', sharedLength)

    const popped = old.length - sharedLength

    if (popped) {
      old.splice(sharedLength, popped)
    }

    console.log('NU', pushed.length, sharedLength, this.heads.map(h => h.value))

    for (const node of pushed) {
      old.push(node)
    }

    return {
      popped,
      pushed: pushed.length,
      unindexed: this.unindexed
    }

    process.exit()

    function getExpectedLength (node, clock, sharedLength) {
      for (const [writer, length] of node.clock) {
        sharedLength += (length - clock.get(writer))
      }

      return sharedLength
    }

    function addClock (a, b) {
      for (const [writer, length] of b) {
        if (a.get(writer) < length) a.set(writer, length)
      }
    }

    function sortHeads (nodes) {
      return nodes.sort((a, b) => a.writer.compare(b.writer))
    }

    function visit (heads) {
      for (const best of sortHeads(heads)) {
        if (visited.has(best)) continue
        visited.add(best)

        const expectedLength = getExpectedLength(best, sharedClock, sharedLength)

        if (expectedLength > old.length || diffedLength >= expectedLength) {
          pushed.push(best)
          visit(best.heads)
          return
        }

        if (old[expectedLength - 1] === best) {
          // all the save <= expectedIndex
          if (expectedLength > sharedLength) {
            sharedLength = expectedLength
            addClock(sharedClock, best.clock)
          }
        } else {
          pushed.push(best)
          if (diffedLength < expectedLength) diffedLength = expectedLength
          visit(best.heads)
        }
      }
    }

    /*


    console.log('linearlize')

    let popped = 0
    let pushed = 0

    const indexed = this.indexed

    this.indexed = []

    const stack = [this.heads.slice()]
    const pushing = []
    let minClock =

    while (stack.length) {
      const heads = stack[stack.length - 1]

      if (!heads.length) {
        stack.pop()
        continue
      }

      let best = null
      for (const h of heads) {
        if (best === null || h.writer.compare(best.writer) < 0) {
          best = h
        }
      }

      let l = 0
      for (const [writer, length] of best.clock) {
        l += length - this.clock.get(writer)
      }

      if (l > this.unindexed.length) {
        pushing.push(best)
        if (best.heads.length) stack.push(best.heads.slice())
      } else {
        const node = this.unindexed[l - 1]

        if (node !== best) {
          console.log('different', this.unindexed.length, l)
          while (l <= this.unindexed.length) {
            console.log('reordered!!!', node.value, best.value)
            this.unindexed.pop()
            popped++
          }

          pushing.push(best)
          if (best.heads.length) stack.push(best.heads.slice())
        }
      }

      popAndSwap(heads, heads.indexOf(best))
    }

    console.log('pusing', pushing.length)

    for (; pushed < pushing.length; pushed++) {
      this.unindexed.push(pushing[pushing.length - pushed - 1])
    }

    return { pushed, popped, indexed, unindexed: this.unindexed }
    */
  }

  setIndexers (indexers) {
    this.indexers = indexers
  }

  addHead (node) {
    for (let i = 0; i < this.heads.length; i++) {
      const head = this.heads[i]

      if (node.clock.get(head.writer) >= head.length) {
        if (popAndSwap(this.heads, i)) i--
      }
    }

    if (this._isTail(node)) this.tails.push(node)
    this.heads.push(node)
  }

  _isTail (node) {
    for (let i = 0; i < node.heads.length; i++) {
      if (node.heads[i].length) return false
    }
    return true
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
      if (total >= majority) return this._shiftWriter(writer)
    }

    return null
  }

  _shiftWriter (writer) {
    const node = writer.shift()

    this.clock.set(writer, node.length)

    popAndSwap(this.tails, this.tails.indexOf(node))
    clearNode(node)

    for (const w of this.indexers) {
      const first = w.getCached(w.offset)
      if (!first) continue
      const i = first.heads.indexOf(node)
      if (i === -1) continue
      popAndSwap(first.heads, i)
      this.tails.push(first)
    }

    return node
  }

  _votesFor (writer) {
    const h = writer.head()
    if (!h) return null

    let best = null

    for (const [cand] of h.clock) {
      const first = cand.getCached(cand.offset)
      if (!first || !this._isTail(first)) continue

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

class LinearizedCore {
  constructor () {
    this.nodes = []
  }

  get length () {
    return this.nodes.length
  }

  get (seq) {
    return this.nodes[seq]
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

    this._update = null

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
    }

    const u = this.pending.update()

    console.log({ ...u, unindexed: null, indexed: null })

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

function clearNode (node) {
  node.length = 0
  node.heads = null
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
