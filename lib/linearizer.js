const b4a = require('b4a')
const assert = require('nanoassert')

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

  includes (w, length) {
    return this.seen.has(w) && this.seen.get(w) >= length
  }

  get (w) {
    return this.seen.get(w) || 0
  }

  set (w, len) {
    this.seen.set(w, len)
    return len
  }

  add (clock) {
    for (const [w, l] of clock) {
      if (this.get(w) < l) this.set(w, l)
    }
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
    this.dependencies.clear()
    this.dependents.clear()

    this.yielded = true

    return this
  }

  get ref () {
    return this.writer.core.key.toString('hex').slice(0, 2) + ':' + this.length
  }
}

module.exports = class Linearizer {
  constructor (indexers, heads) {
    this.heads = new Set(heads.map(h => h && h.clear()))
    this.tails = new Set()
    this.merges = new Set()
    this.linearMajority = false
    this.majority = (indexers.length >>> 1) + 1
    this.indexers = indexers
    this.indexerHeads = new Map()
    this.removed = new Clock()
    this.pending = []
    this.headsIndexed = new Set()
    this.tip = []

    for (const head of this.heads) {
      this.removed.set(head.writer, head.length)
    }

    this.updated = false
  }

  static createNode (writer, length, value, heads, batch, dependencies) {
    return new Node(writer, length, value, heads, batch, dependencies)
  }

  addHead (node) {
    for (const dep of node.dependencies) {
      if (dep.yielded) {
        node.dependencies.delete(dep)
      } else {
        dep.dependents.add(node)
      }
    }

    if (!node.dependencies.size) {
      this.tails.add(node)
    }

    for (const head of this.heads) {
      if (node.clock.includes(head.writer, head.length)) {
        this.heads.delete(head)
      }
    }

    if (node.dependencies.size > 1) this.merges.add(node)

    this.heads.add(node)
    this.indexerHeads.set(node.writer, node)

    this.updated = true
    return node
  }

  update () {
    if (!this.updated) return null
    this.updated = false

    const indexed = []

    // get the indexed nodes
    while (true) {
      const batch = this._shift()
      if (batch.length === 0) break

      for (const node of batch) {
        indexed.push(node)
      }
    }

    let pushed = 0
    let popped = 0

    const list = []

    const removed = new Clock()
    removed.add(this.removed)

    // getting the tip
    while (true) {
      if (!this._addNextBatch(removed, list)) break
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

    const update = {
      shared,
      popped,
      pushed,
      length: shared + pushed,
      indexed,
      tip: list
    }

    return update
  }

  _addNextBatch (removed, list) {
    let node = this._next(removed)
    if (!node) return false

    while (true) {
      if (!list.includes(node)) list.push(node)

      removed.add(node.clock)

      if (node.batch === 1) break

      // just a safety check
      assert(node.dependents.size === 1, 'Batch is linked partially, which is not allowed')

      node = getFirst(node.dependents)
    }

    return true
  }

  // choose the which unindexed node to order next
  _next (removed) {
    let node = null

    // tie break between unordered heads
    for (const head of this.heads) {
      if (removed.includes(head.writer, head.length)) continue

      if (node && !tieBreak(head.writer, node.writer)) continue
      node = head
    }

    // no more nodes to order
    if (!node) return null

    // tie break down the dag until we reach a tail
    while (true) {
      let best = null
      for (const dep of node.dependencies) {
        if (removed.includes(dep.writer, dep.length)) continue

        if (best && !tieBreak(dep.writer, best.writer)) continue
        best = dep
      }

      if (!best) break
      node = best
    }

    return node
  }

  _tails (node) {
    const tails = new Set()
    for (const t of this.tails) {
      if (node.clock.includes(t.writer, t.length)) tails.add(t)
    }

    return tails
  }

  // parent is newer if for any node in parent's view,
  // either node can see object or object can see node
  _strictlyNewer (object, parent) {
    const stack = []
    const visited = new Set()

    for (const tail of this._tails(parent)) {
      stack.push(tail)
    }

    while (stack.length) {
      let node = stack.pop()

      while (node.dependencies.size === 1) {
        if (node === object || node.dependents.size === 0) break
        node = getFirst(node.dependents)
      }

      if (visited.has(node)) continue
      visited.add(node)

      if (!parent.clock.includes(node.writer, node.length)) continue

      const isSeen = object.clock.includes(node.writer, node.length)
      const isNewer = node.clock.includes(object.writer, object.length)

      if (!isSeen && !isNewer) return false

      for (const dep of node.dependents) {
        stack.push(dep)
      }
    }

    return true
  }

  // check if a node is confirmed and if parent
  // is set, also check if the node is preferred
  _isConfirmed (target, parent = null) {
    const self = this
    const idx = this.indexers
    const removed = this.removed
    const isLinear = this.linearMajority
    const thres = this.majority

    const stack = [[target, new Set(), new Set()]]

    while (stack.length) {
      const [node, seen, confs] = stack.pop()

      // if parent is set, we only consider nodes in parent clock
      if (parent && !parent.clock.includes(node.writer, node.length)) return false

      // writer has seen the node
      seen.add(node.writer)

      // target has been seen by a majority of writers
      if (seen.size >= thres) {
        confs.add(node.writer)

        // double majority has been seen
        if (confs.size >= thres) return true

        // if using parent view, check if target is preferred
        if (parent && preferred(confs)) {
          return true
        }
      }

      let last = null

      // continue up the dag until we get a result
      for (const dep of node.dependents) {
        // node must unambiguously vote for target
        if (!self._strictlyNewer(target, dep)) continue

        // in linear mode we continue with each dep independently
        const next = isLinear ? new Set(confs) : confs

        if (last) stack.push([last, new Set(seen), next])
        last = dep
      }

      if (last) stack.push([last, seen, confs])
    }

    return false

    // check for a set of writers that could yield this node
    function preferred (confs) {
      let available = 0

      for (const w of idx) {
        // continue if writer is already counted
        if (confs.has(w)) continue

        // get writer's length in parent clock
        const prev = parent.clock.get(w)

        // writer available if prev is yielded
        const isYielded = removed.includes(w, prev)

        // writer available if prev is seen by target
        const isSeen = target.clock.includes(w, prev)

        // writer would be in confs already if available
        if (!isYielded && !isSeen) continue

        // check if a majority set is possible
        if (confs.size + ++available >= thres) return true
      }
    }
  }

  _tailsAndMerges (node) {
    const all = this._tails(node)
    for (const m of this.merges) {
      if (m !== node && node.clock.includes(m.writer, m.length)) {
        all.add(m)
      }
    }
    return all
  }

  _isTail (node) {
    // a tail has no unyielded dependencies
    for (const dep of node.dependencies) {
      if (!dep.yielded) return false
    }

    return true
  }

  _shift () {
    for (const tail of this.tails) {
      if (this._isConfirmed(tail)) {
        return this._yield(tail)
      }
    }

    for (const merge of this.merges) {
      if (this._isConfirmed(merge)) {
        return this._yield(merge)
      }
    }

    return []
  }

  _yield (node) {
    // only stop when we find a tail
    while (node.dependencies.size) {
      // easy with only one dependency
      if (node.dependencies.size === 1) {
        node = getFirst(node.dependencies)
        continue
      }

      let next = null

      // for merges check if one fork is confirmed
      for (const t of this._tailsAndMerges(node)) {
        if (this._isConfirmed(t, node)) {
          next = t
          break
        }
      }

      // otherwise tiebreak between current tails
      if (!next) {
        for (const t of this._tails(node)) {
          if (next && !tieBreak(t.writer, next.writer)) continue
          next = t
        }
      }

      node = next
    }

    return this._removeBatch(node)
  }

  _removeNode (node) {
    // update the indexed heads
    for (const head of this.headsIndexed) {
      if (node.clock.includes(head.writer, head.length)) {
        this.headsIndexed.delete(head)
      }
    }

    this.headsIndexed.add(node)

    // clear node from linearizer state
    this.tails.delete(node)
    this.merges.delete(node)

    // update the removed clock
    this.removed.set(node.writer, node.length)

    node.yielded = true

    // update the tailset
    for (const d of node.dependents) {
      d.dependencies.delete(node)
      if (this._isTail(d)) {
        this.tails.add(d)
      }
    }

    return node
  }

  _removeBatch (node) {
    const batch = [node]

    while (node.batch !== 1) { // its a batch!
      assert(node.dependents.size === 1, 'Batch is linked partially, which is not allowed')

      node = getFirst(node.dependents)
      batch.push(node)
    }

    // only need to set for the head of the batch
    node.indexed = [...this.headsIndexed]

    return batch.map(this._removeNode.bind(this))
  }
}

function tieBreak (a, b) {
  return b4a.compare(a.core.key, b.core.key) < 0
}

function getFirst (set) {
  return set[Symbol.iterator]().next().value
}
