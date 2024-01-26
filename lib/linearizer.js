const b4a = require('b4a')
const assert = require('nanoassert')

const Clock = require('./clock')
const Consensus = require('./consensus')

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
      if (dep.yielded) {
        this.dependencies.delete(dep) // nodes might be yielded during buffering
      } else {
        dep.dependents.add(this)
        this.clock.add(dep.clock)
      }
    }

    if (this.writer.isIndexer) this.clock.set(this.writer.core.key, this.length)
  }

  tieBreak (node) {
    return tieBreak(this, node)
  }

  hasDependency (dep) {
    for (const h of this.actualHeads) {
      if (sameNode(h, dep)) return true
    }
    return false
  }

  get ref () {
    return this.writer.core.key.toString('hex').slice(0, 2) + ':' + this.length
  }
}

module.exports = class Linearizer {
  constructor (indexers, { heads = [], writers = new Map() } = {}) {
    this.heads = new Set()
    this.tails = new Set()
    this.tip = []
    this.size = 0 // useful for debugging
    this.updated = false
    this.indexersUpdated = false
    this.writers = writers

    this.consensus = new Consensus(indexers)
    this._initialHeads = heads.slice(0)
    this._strictlyAdded = null

    for (const { key, length } of heads) {
      this.consensus.removed.set(key, length)
    }
  }

  get indexers () {
    return this.consensus.indexers
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
      if (node.hasDependency(head)) {
        this.heads.delete(head)
      }
    }

    if (node.writer.isIndexer) this.consensus.addHead(node)

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
      const node = this.consensus.shift()
      if (!node) break

      const batch = this._yield(node)
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

  /* Tip ordering methods */

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

  /* Ack methods */

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
      if (this.consensus.shouldAckNode(tail, writer)) return true
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
          const acks = this.consensus.acksFromNode(node, head)
          const prevAcks = this.consensus.acksFromNode(node, prev)

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

  /* Full DAG methods */

  // yields full dags including non-indexer nodes
  _yield (node) {
    const nodes = []

    while (true) {
      let current = node
      while (current.dependencies.size) {
        // easy with only one dependency
        if (current.dependencies.size === 1) {
          current = getFirst(current.dependencies)
          continue
        }

        let next = null
        for (const t of current.dependencies) {
          if (next && next.tieBreak(t)) continue
          next = t
        }

        current = next
      }

      for (const removed of this._removeBatch(current)) {
        nodes.push(removed)
      }

      if (node === current) break
    }

    return nodes
  }

  _isTail (node) {
    // a tail has no unyielded dependencies
    for (const dep of node.dependencies) {
      if (!dep.yielded) return false
    }

    return true
  }

  _removeNode (node) {
    this.tails.delete(node)
    this.consensus.merges.delete(node)
    this.heads.delete(node)

    // update the removed clock
    if (node.writer.isIndexer) {
      this.consensus.removed.set(node.writer.core.key, node.length)
    }

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
      if (node.dependents.size === 0) { // bad batch node, auto correct
        const next = node.writer.get(node.length)
        if (next && next.batch === node.batch - 1) node.dependents.add(next)
      }

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

// if same key, earlier node is first
function tieBreak (a, b) {
  return keySort(a, b) > 0 // keySort sorts high to low
}

function getFirst (set) {
  return set[Symbol.iterator]().next().value
}

function keySort (a, b) {
  const cmp = b4a.compare(a.writer.core.key, b.writer.core.key)
  return cmp === 0 ? b.length - a.length : -cmp
}

function sameNode (a, b) {
  return b4a.equals(a.key, b.writer.core.key) && a.length === b.length
}
