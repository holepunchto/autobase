// This is basically just a Map atm, but leaving it as an abstraction for now
// in case we wanna optimize it for our exact usecase

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

class Node {
  constructor (writer, length, value, heads, batch, dependencies) {
    this.writer = writer
    this.length = length
    this.value = value
    this.heads = heads
    this.dependents = new Set()
    this.dependencies = new Set(dependencies)
    this.batch = batch
    this.clock = new Clock()
    this.indexed = null
    this.yielded = false
  }

  clear () {
    this.clock = null
    this.dependencies = null
    this.dependents = null

    this.yielded = true

    return this
  }
}

module.exports = class Linearizer {
  constructor (indexers, heads) {
    this.tails = []

    this.heads = heads.map(h => h.clear())
    this.headsIndexed = heads.slice(0)
    this.indexers = indexers

    this.tip = []
    this.updated = false
  }

  static createNode (writer, length, value, heads, batch, dependencies) {
    return new Node(writer, length, value, heads, batch, dependencies)
  }

  update () {
    if (!this.updated) return null
    this.updated = false

    const indexed = []

    while (true) {
      const nodes = this._shiftBatch()
      if (nodes.length === 0) break

      for (const node of nodes) {
        indexed.push(node)

        if (node.batch === 1) {
          node.indexed = this.headsIndexed.slice(0)
        }
      }
    }

    let pushed = 0
    let popped = 0

    const tails = this.tails.slice(0)
    const list = []

    while (tails.length) {
      this._addNextBatch(tails, list)
    }

    const dirtyList = indexed.length ? indexed.concat(list) : list
    const min = Math.min(dirtyList.length, this.tip.length)

    let same = true
    let shared = 0

    for (; shared < min; shared++) {
      if (dirtyList[shared] === this.tip[shared]) {
        continue
      }

      same = false
      popped = this.tip.length - shared
      pushed = dirtyList.length - shared
      break
    }

    if (same) {
      pushed = dirtyList.length - this.tip.length
    }

    this.tip = list

    return {
      shared,
      popped,
      pushed,
      length: shared + pushed,
      indexed,
      tip: list
    }
  }

  addHead (node) {
    for (let i = 0; i < this.heads.length; i++) {
      const head = this.heads[i]
      if (node.clock.get(head.writer) >= head.length) {
        if (popAndSwap(this.heads, i)) i--
      }
    }

    for (const dep of node.dependencies) {
      if (!dep.yielded) dep.dependents.add(node)
      else node.dependencies.delete(dep)
    }

    if (node.dependencies.size === 0 || this._isTail(node)) {
      this.tails.push(node)
    }

    this.heads.push(node)
    this.updated = true
  }

  _isTail (node) {
    for (const dep of node.dependencies) {
      if (!dep.yielded) return false
    }

    return true
  }

  _addNextBatch (tails, list) {
    let node = this._next(tails).node

    while (true) {
      list.push(node)
      removeTail(node, tails)

      if (node.batch === 1) return

      // just a safety check
      if (node.dependents.size !== 1) {
        throw new Error('Batch is linked partially, which is not allowed')
      }

      // eslint-disable-next-line no-unreachable-loop
      for (const d of node.dependents) {
        node = d
        break
      }
    }
  }

  _next (tails) {
    const cache = new Map()
    const results = new Array(tails.length)
    const majority = Math.floor(this.indexers.length / 2) + 1

    for (let i = 0; i < tails.length; i++) {
      results[i] = {
        votes: 0,
        majorityVotes: 0,
        indexed: false,
        node: tails[i]
      }
    }

    for (const writer of this.indexers) {
      const head = writer.head()
      if (!head) continue

      const r = votesFor(head)
      const aggr = results[r.best]

      aggr.votes++
      if (r.tally[r.best] >= majority) {
        aggr.majorityVotes++
        if (aggr.majorityVotes >= majority) aggr.indexed = true
      }
    }

    // TODO: silly, fix
    return results.sort(cmp)[0]

    function cmp (a, b) {
      if (a.majorityVotes !== b.majorityVotes) return b.majorityVotes - a.majorityVotes
      if (a.votes !== b.votes) return b.votes - a.votes
      return a.node.writer.compare(b.node.writer)
    }

    function votesFor (node) {
      if (cache.has(node)) return cache.get(node)

      const result = { best: 0, tally: new Array(tails.length) }
      for (let i = 0; i < result.tally.length; i++) result.tally[i] = 0

      cache.set(node, result)

      const done = tails.indexOf(node)
      if (done > -1) {
        result.tally[done] = 1
        result.best = done
        return result
      }

      if (node.yielded) {
        // points to nothing...
        return result
      }

      for (const [writer, length] of node.clock) {
        const dep = node.writer === writer && node.length === length
          ? writer.getCached(length - 2)
          : writer.getCached(length - 1)

        if (dep === null || dep.yielded) continue

        const { best } = votesFor(dep)
        result.tally[best]++
      }

      for (let i = 1; i < result.tally.length; i++) {
        const b = result.best
        const vb = result.tally[b]
        const vi = result.tally[i]

        if (vi > vb || (vi === vb && tails[i].writer.compare(tails[b].writer) < 0)) {
          result.best = i
        }
      }

      result.tally[result.best]++

      return result
    }
  }

  _shiftBatch () {
    const batch = []

    let node = this._shift()
    if (node === null) return batch

    batch.push(node)

    while (node.batch !== 1) { // its a batch!
      if (node.dependents.size !== 1) {
        throw new Error('Batch is linked partially, which is not allowed')
      }

      // eslint-disable-next-line no-unreachable-loop
      for (const d of node.dependents) {
        node = d
        break
      }

      batch.push(this._shiftNode(node))
    }

    return batch
  }

  _shiftNode (node) {
    const dependents = node.dependents

    for (let i = 0; i < this.headsIndexed.length; i++) {
      const head = this.headsIndexed[i]
      if (node.clock.get(head.writer) >= head.length) {
        if (popAndSwap(this.headsIndexed, i)) i--
      }
    }

    this.headsIndexed.push(node)

    popAndSwap(this.tails, this.tails.indexOf(node))
    node.yielded = true

    for (const dep of dependents) {
      if (this._isTail(dep)) this.tails.push(dep)
    }

    return node
  }

  _shift () {
    if (this.tails.length === 0) return null

    const result = this._next(this.tails)
    if (!result.indexed) return null

    return this._shiftNode(result.node)
  }
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

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}
