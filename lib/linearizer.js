const b4a = require('b4a')
const assert = require('nanoassert')
const BufferMap = require('tiny-buffer-map')

// This is basically just a Map atm, but leaving it as an abstraction for now
// in case we wanna optimize it for our exact usecase

class Clock {
  constructor () {
    this.seen = new BufferMap()
  }

  get size () {
    return this.seen.size
  }

  has (key) {
    return this.seen.has(key)
  }

  includes (key, length) {
    return this.seen.has(key) && this.seen.get(key) >= length
  }

  get (key) {
    return this.seen.get(key) || 0
  }

  set (key, len) {
    this.seen.set(key, len)
    return len
  }

  add (clock) {
    for (const [key, l] of clock) {
      if (this.get(key) < l) this.set(key, l)
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
    this.actualHeads = heads.slice(0) // TODO: we should remove this and just not mutate heads...

    this.dependents = new Set()
    this.dependencies = dependencies

    this.batch = batch

    this.clock = new Clock()
    this.yielded = false
  }

  clear () {
    this.clock = null
    this.dependencies = null
    this.dependents = null
    return this
  }

  reset () {
    this.yielded = false
    for (const dep of this.dependents) dep.dependencies.add(this)
    this.dependents.clear()
  }

  active () {
    for (const dep of this.dependencies) {
      if (dep.yielded) this.dependencies.delete(dep) // nodes might be yielded during buffering
      else dep.dependents.add(this)
    }
  }

  get ref () {
    return this.writer.core.key.toString('hex').slice(0, 2) + ':' + this.length
  }
}

module.exports = class Linearizer {
  constructor (indexers, { heads = [] } = {}) {
    this.heads = new Set()
    this.tails = new Set()
    this.merges = new Set()
    this.linearMajority = false
    this.majority = (indexers.length >>> 1) + 1
    this.indexers = indexers
    this.removed = new Clock()
    this.pending = []
    this.tip = []
    this.size = 0 // useful for debugging
    this.writers = new BufferMap() // tmp solution...
    this.updated = false

    this._initialHeads = heads.slice(0)

    for (const { key, length } of heads) {
      this.removed.set(key, length)
    }
  }

  static createNode (writer, length, value, heads, batch, dependencies) {
    return new Node(writer, length, value, heads, batch, dependencies)
  }

  // returns the global links of the dag, use this to link against the current state of the dag
  // TODO: rename to heads() and move the sets to _ props
  getHeads () {
    const heads = this._initialHeads.slice(0)
    for (const node of this.heads) heads.push({ key: node.writer.core.key, length: node.length })
    return heads
  }

  addHead (node) {
    node.active()

    // 99.99% of the time _initialHeads is empty...
    if (this._initialHeads.length > 0) this._updateInitialHeads(node)

    this.writers.set(node.writer.core.key, node.writer)

    if (!node.dependencies.size) {
      this.tails.add(node)
    }

    for (const head of this.heads) {
      if (node.clock.includes(head.writer.core.key, head.length)) {
        this.heads.delete(head)
      }
    }

    if (node.dependencies.size > 1) this.merges.add(node)

    this.size++
    this.heads.add(node)

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

  _updateInitialHeads (node) {
    for (const head of node.actualHeads) {
      for (let i = 0; i < this._initialHeads.length; i++) {
        const { key, length } = this._initialHeads[i]
        if (length !== head.length || !b4a.equals(key, head.key)) continue
        this._initialHeads.splice(i--, 1)
      }
    }
  }

  _addNextBatch (removed, list, heads) {
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
      if (removed.includes(head.writer.core.key, head.length)) continue

      if (node && !tieBreak(head.writer, node.writer)) continue
      node = head
    }

    // no more nodes to order
    if (!node) return null

    // tie break down the dag until we reach a tail
    while (true) {
      let best = null
      for (const dep of node.dependencies) {
        if (removed.includes(dep.writer.core.key, dep.length)) continue

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
      if (node.clock.includes(t.writer.core.key, t.length)) tails.add(t)
    }

    return tails
  }

  shouldAck (writer) {
    if (!this.indexers.includes(writer)) return false

    // all indexers have to flushed to the dag before we ack as a quick "debounce"
    for (const w of this.indexers) {
      if (w.length !== w.available) return false
    }

    // if ANY head is not an indexer ack
    for (const head of this.heads) {
      if (!head.writer.isIndexer) return true
    }

    const prev = writer.length ? writer.get(writer.length - 1) : null
    const visited = new Set()

    // check if there is non-null value
    let valueCheck = false

    for (const tail of this.tails) {
      // crawl the dag up from each tail
      valueCheck ||= this._nonNull(tail, visited)

      const seen = prev && prev.clock.includes(tail.writer.core.key, tail.length)

      // if not newer then ack is useless
      if (seen && !this._strictlyNewer(tail, prev)) continue

      if (valueCheck && this._shouldAckNode(tail, writer)) return true
    }

    if (!valueCheck) return false

    for (const merge of this.merges) {
      if (prev && prev.clock.includes(merge.writer.core.key, merge.length)) {
        return true // aggressively merge
      }
    }

    return false
  }

  // check if there is any value above this node
  _nonNull (target, visited) {
    const stack = [target]

    while (stack.length) {
      const node = stack.pop()

      if (visited.has(node)) continue
      if (node.value !== null) return true

      visited.add(node)

      for (const dep of node.dependents) {
        stack.push(dep)
      }
    }

    return false
  }

  _shouldAckNode (target, writer) {
    const next = target.clock.get(writer.core.key)
    const nextIndexNode = writer.get(next >= writer.indexed ? next : writer.indexed)

    // if we have no next node and we didn't write target then ack
    if (!nextIndexNode && writer !== target.writer) return true

    // shortcuts if we have next node
    if (nextIndexNode) {
      // if the next node does not see the target, should ack
      if (!nextIndexNode.clock.includes(target.writer.core.key, target.length)) return true

      // if the next node is not strictly newer, no point acking
      if (!this._strictlyNewer(target, nextIndexNode)) return false
    }

    // now check if we can double confirm
    const acks = this._acks(target)

    // need enough to double confirm
    if (acks.length >= this.majority) {
      return !this.confirms(writer, target, acks, writer.available)
    }

    return false
  }

  // parent is newer if for any node in parent's view,
  // either node can see object or object can see node
  _strictlyNewer (object, parent) {
    for (const [key, latest] of parent.clock) {
      const oldest = this.removed.get(key)
      if (latest <= oldest) continue // check quickly if we removed it

      // get the NEXT mode from the writer from the objects pov, adjust if its removed
      let length = object.clock.get(key)
      if (length <= oldest) length = oldest

      // sanity check, likely not needed as someone has checked this before, but whatevs, free
      if (latest < length) return false

      // if the same, they both seen it, continue
      if (latest === length) continue

      const writer = this.writers.get(key)

      // might not be in the removed set but the writer can tell us if it was indexed...
      const next = writer && writer.get(length >= writer.indexed ? length : writer.indexed)

      // no next, its been indexed, both seen it
      if (!next) continue

      // if the NEXT node has seen the object its fine - newer
      if (next.clock.includes(object.writer.core.key, object.length)) continue

      // otherwise the parent must also NOT has seen the next node
      if (!parent.clock.includes(next.writer.core.key, next.length)) continue

      return false
    }

    return true
  }

  _acks (target) {
    const acks = this.indexers.includes(target.writer) ? [target] : [] // TODO: can be cached on the target node in future (ie if we add one we dont have to check it again)

    for (const idx of this.indexers) {
      if (idx === target.writer) continue

      let next = target.clock.get(idx.core.key)
      if (next < idx.nodes.offset) next = idx.nodes.offset

      const nextIndexNode = idx.get(next >= idx.indexed ? next : idx.indexed)

      // no node - no ack
      if (!nextIndexNode) continue

      // if the next index node does not see the target, no ack
      if (!nextIndexNode.clock.includes(target.writer.core.key, target.length)) continue

      // if the next index node is not strictly newer, skip to avoid ambig...
      if (!this._strictlyNewer(target, nextIndexNode)) continue

      acks.push(nextIndexNode)
    }

    return acks
  }

  _ackedAt (acks, parent) {
    let seen = 0
    let missing = acks.length

    for (const node of acks) {
      missing--

      if (!parent.clock.includes(node.writer.core.key, node.length)) {
        if (seen + missing < this.majority) return false
        continue
      }

      if (++seen >= this.majority) return true
    }

    return false
  }

  confirms (indexer, target, acks, length) {
    if (!length || this.removed.get(indexer.core.key) >= length) return false
    // def feels like there is a smarter way of doing this part
    // ie we just wanna find a node from the indexer that is strictly newer than target
    // and seens a maj of the acks - thats it

    for (let i = length - 1; i >= 0; i--) {
      const head = indexer.get(i)
      if (head === null) return false

      let seen = 0

      for (const node of acks) {
        // if (node.writer === indexer) continue
        if (!head.clock.includes(node.writer.core.key, node.length)) continue
        if (++seen >= this.majority) break
      }

      if (seen < this.majority) {
        return false
      }

      if (!this._strictlyNewer(target, head)) {
        continue
      }

      return true
    }

    return false
  }

  _isConfirmed (target, parent = null) {
    const acks = this._acks(target)
    const confs = new Set()

    if (acks.length < this.majority) return false

    for (const indexer of this.indexers) {
      const length = parent
        ? (parent.writer === indexer) ? parent.length - 1 : parent.clock.get(indexer.core.key)
        : indexer.available

      if (this.confirms(indexer, target, acks, length)) {
        confs.add(indexer)
        if (confs.size >= this.majority) {
          return true
        }
      }
    }

    if (parent) return this._isConfirmableAt(target, parent, acks, confs)

    return false
  }

  _isConfirmableAt (target, parent, acks, confs) {
    if (!this._ackedAt(acks, parent)) return false

    let potential = confs.size

    for (const indexer of this.indexers) {
      if (confs.has(indexer)) continue

      const length = parent.clock.get(indexer.core.key)
      const isSeen = target.clock.includes(indexer.core.key, length)

      // if the target has seen the latest node, it can freely be used to confirm the target later
      // otherwise, check if a newer node is strictly newer...
      if (!isSeen) {
        const head = indexer.get(length - 1)

        // the next indexer head HAS to be strictly newer - meaning the current one has to be also.
        if (head && !this.removed.includes(head.writer.core.key, head.length) && !this._strictlyNewer(target, head)) {
          continue
        }
      }

      if (++potential >= this.majority) return true
    }

    return false
  }

  _tailsAndMerges (node) {
    const all = this._tails(node)
    for (const m of this.merges) {
      if (m !== node && node.clock.includes(m.writer.core.key, m.length)) {
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
    this.tails.delete(node)
    this.merges.delete(node)
    this.heads.delete(node)

    // update the removed clock
    this.removed.set(node.writer.core.key, node.length)

    // update the tailset
    for (const d of node.dependents) {
      d.dependencies.delete(node)
      if (this._isTail(d)) this.tails.add(d)
    }

    node.yielded = true
    this.size--

    if (this.heads.size === 0) {
      // in case of a single writer the dag might drain immediately...
      this._initialHeads.push({ key: node.writer.core.key, length: node.length })
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

    for (const node of batch) {
      this._removeNode(node)
    }

    return batch
  }
}

function tieBreak (a, b) {
  return b4a.compare(a.core.key, b.core.key) < 0
}

function getFirst (set) {
  return set[Symbol.iterator]().next().value
}
