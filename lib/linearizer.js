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
    this.ordering = 0
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
  constructor (indexers, { heads = [], writers = new Map() } = {}) {
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
    this.updated = false
    this.writers = writers

    this._initialHeads = heads.slice(0)
    this._strictlyAdded = null

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

    if (this.heads.size === 1 && (this.updated === false || this._strictlyAdded !== null)) {
      if (this._strictlyAdded === null) this._strictlyAdded = []
      this._strictlyAdded.push(node)
    } else {
      this._strictlyAdded = null
    }

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

    const diff = this._maybeStrictlyAdded(indexed)
    if (diff !== null) return diff

    let pushed = 0
    let popped = 0

    const list = this._orderTip()

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

  _maybeStrictlyAdded (indexed) {
    if (this._strictlyAdded === null) return null

    const added = this._strictlyAdded
    this._strictlyAdded = null

    for (let i = 0; i < indexed.length; i++) {
      const node = indexed[i]
      const other = i < this.tip.length ? this.tip[i] : added[i - this.tip.length]
      if (node !== other) return null
    }

    const shared = this.tip.length

    this.tip.push(...added)

    const length = this.tip.length

    if (indexed.length) this.tip = this.tip.slice(indexed.length)

    return {
      shared,
      popped: 0,
      pushed: added.length,
      length,
      indexed,
      tip: this.tip
    }
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

  _orderTip () {
    const tip = []
    const stack = [...this.tails]

    while (stack.length) {
      const node = stack.pop()
      if (node.ordering) continue

      node.ordering = node.dependencies.size
      stack.push(...node.dependents)
    }

    stack.push(...this.tails)
    stack.sort(keySort)

    while (stack.length) {
      const node = stack.pop()
      tip.push(node)

      const batch = []

      for (const dep of node.dependents) {
        if (--dep.ordering === 0) batch.push(dep)
      }

      if (batch.length > 0) stack.push(...batch.sort(keySort))
    }

    return tip
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

  shouldAck (writer, pending = false) {
    if (!writer || !writer.isIndexer) return false

    // all indexers have to flushed to the dag before we ack as a quick "debounce"
    for (const w of this.indexers) {
      if (w.length !== w.available) return false
    }

    let isHead = false

    // if ANY head is not an indexer ack
    for (const head of this.heads) {
      if (!head.writer.isIndexer) return true
      if (head.writer === writer) isHead = true
    }

    if (this.heads.size === 1 && isHead) {
      return false // never self-ack!
    }

    const visited = new Set()

    // check if there is non-null value
    let valueCheck = false

    for (const tail of this.tails) {
      if (pending || this._nonNull(tail, visited)) {
        valueCheck = true
        break
      }
    }

    if (!valueCheck) return false

    for (const tail of this.tails) {
      if (this._shouldAckNode(tail, writer)) return true
    }

    return this._shouldAckHeads(writer, pending)
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

  // ack if any head is closer to confirming a value
  _shouldAckHeads (writer) {
    const prev = writer.head()

    for (const head of this.heads) {
      // only check other writers heads
      if (head.writer === writer) continue

      const stack = [head]
      const visited = new Set()

      while (stack.length) {
        const node = stack.pop()

        if (visited.has(node)) continue
        visited.add(node)

        if (node.value !== null) {
          const acks = this._acksFromNode(node, head)
          const prevAcks = this._acksFromNode(node, prev)

          // head sees more acks
          if (acks.size > prevAcks.size) return true

          for (const idx of acks) {
            // head sees acks that writer does not
            if (!prevAcks.has(idx)) return true
          }

          // both seen, no point going any further down
          if (prevAcks.size && acks.size) continue
        }

        stack.push(...node.dependencies)
      }
    }

    return false
  }

  _shouldAckNode (target, writer) {
    const head = writer.head()
    const next = target.clock.get(writer.core.key)
    const nextIndexNode = writer.get(next >= writer.indexed ? next : writer.indexed)

    // if we have no next node and we didn't write target then ack
    if (!nextIndexNode && writer !== target.writer) return true

    // shortcuts if we have next node
    if (nextIndexNode) {
      // if the next node does not see the target, should ack
      if (!nextIndexNode.clock.includes(target.writer.core.key, target.length)) {
        return !head.clock.includes(target.writer.core.key, target.length)
      }

      // if the next node is not strictly newer, no point acking
      if (!this._strictlyNewer(target, nextIndexNode)) return false
    }

    // now check if we can double confirm
    const acks = this._acks(target)

    // need enough to double confirm
    if (acks.length >= this.majority) {
      return !this.confirms(writer, target, acks, writer.length)
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
    const acks = target.writer.isIndexer ? [target] : [] // TODO: can be cached on the target node in future (ie if we add one we dont have to check it again)

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

  _acksFromNode (target, view) {
    const acks = new Set()

    if (!view || !view.clock.includes(target.writer.core.key, target.length)) return acks

    acks.add(view.writer)

    for (const idx of this.indexers) {
      if (idx === view.writer) continue

      const next = view.clock.get(idx.core.key)

      if (target.clock.includes(idx.core.key, next)) continue

      const head = idx.get(next)
      if (!head) continue

      if (head.clock.includes(target.writer.core.key, target.length)) {
        acks.add(idx)
      }
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

    let jump = true

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
        // all strictly newer nodes are clustered together so bisect until we find the cluster
        if (jump) {
          jump = false

          let t = length - 1
          let b = 0

          while (t > b) {
            const mid = (t + b) >>> 1
            const node = indexer.get(mid)

            if (node === null || !node.clock.includes(target.writer.core.key, target.length) || this._strictlyNewer(target, node)) {
              b = mid + 1
            } else {
              t = mid - 1
            }
          }

          // + 2 in case we are off by one and the i--. its fine, just an optimisation
          if (b + 1 < i) i = b + 2
        }

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
        : indexer.length

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

function keySort (a, b) {
  return -b4a.compare(a.writer.core.key, b.writer.core.key)
}
