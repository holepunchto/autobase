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

module.exports = class Linearizer {
  constructor (indexers) {
    this.tails = []
    this.heads = []
    this.clock = new Clock()
    this.indexers = indexers

    this.tip = []
    this.updated = false
    this.ontruncate = noop
  }

  static createNode (writer, length, value, heads, batch, dependencies) {
    return {
      writer,
      length,
      value,
      heads,
      dependents: [],
      dependencies,
      batch,
      clock: new Clock()
    }
  }

  setIndexers (indexers) {
    this.indexers = indexers
  }

  update () {
    if (!this.updated) return null
    this.updated = false

    const indexed = []

    while (true) {
      const node = this._shift()

      if (node) {
        indexed.push(node)
      } else {
        break
      }
    }

    let pushed = 0
    let popped = 0

    const tails = this.tails.slice(0)
    const list = []

    while (tails.length) {
      const best = this._next(tails).node
      list.push(best)
      removeTail(best, tails)
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

      if (popped > 0) this.ontruncate(shared, this.tip.length)
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

    for (let i = 0; i < node.dependencies.length; i++) {
      const dep = node.dependencies[i]

      if (dep.length > 0) dep.dependents.push(node)
      else if (popAndSwap(node.dependencies, i)) i--
    }

    if (node.dependencies.length === 0) this.tails.push(node)
    this.heads.push(node)
    this.updated = true
  }

  _isTail (node) {
    for (let i = 0; i < node.dependencies.length; i++) {
      if (node.dependencies[i].length) return false
    }

    return true
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

      for (const [writer, length] of node.clock) {
        const dep = node.writer === writer && node.length === length
          ? writer.getCached(length - 2)
          : writer.getCached(length - 1)

        if (dep === null || !dep.length) continue

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

      return result
    }
  }

  _shift () {
    const result = this._next(this.tails)
    if (!result.indexed) return null

    const dependents = result.node.dependents

    popAndSwap(this.tails, this.tails.indexOf(result.node))
    clearNode(result.node)

    for (const dep of dependents) {
      if (this._isTail(dep)) this.tails.push(dep)
    }

    return result.node
  }
}

function noop () {}

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

function clearNode (node) {
  node.length = 0
  node.dependencies = null
  node.dependents = null
  node.clock = null
  node.writer = null
}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}
