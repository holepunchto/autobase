const b4a = require('b4a')
const assert = require('nanoassert')

const Clock = require('./clock')
const Consensus = require('./consensus')
const Topolist = require('./topolist')

class Node {
  constructor (writer, length, value, heads, batch, dependencies, version, optimistic) {
    this.writer = writer
    this.length = length
    this.value = value
    this.heads = heads
    this.actualHeads = heads.slice(0) // TODO: we should remove this and just not mutate heads...

    this.dependents = new Set()
    this.dependencies = dependencies

    this.version = version
    this.optimistic = optimistic && (batch === 1) && !!value

    this.batch = batch

    this.clock = new Clock()

    this.yielded = false
    this.yielding = false
    this.unlinked = false
  }

  relink () {
    if (!this.unlinked) return false
    if (this.isLinked()) this.unlinked = false
    return !this.unlinked
  }

  isLinked () {
    if (this.writer.isActiveIndexer) return true
    if (!this.writer.isRemoved) return true

    const visited = new Set()
    const stack = [this]

    while (stack.length) {
      const next = stack.pop()
      for (const dep of next.dependents) {
        if (dep.writer.isActiveIndexer) return true
        if (!dep.writer.isRemoved) return true

        if (!dep.dependents.size) continue
        if (visited.has(dep)) continue
        visited.add(dep)
        stack.push(dep)
      }
    }

    this.unlinked = true

    return false
  }

  clear () {
    this.clock = null
    this.dependencies = null
    this.dependents = null
    return this
  }

  reset () {
    this.yielded = false
    this.yielding = false
    for (const dep of this.dependents) dep.dependencies.add(this)
    this.dependents.clear()
  }

  active () {
    let relinked = false
    for (const dep of this.dependencies) {
      if (dep.unlinked) relinked = true

      if (dep.yielded) {
        this.dependencies.delete(dep) // nodes might be yielded during buffering
      } else {
        dep.dependents.add(this)
        this.clock.add(dep.clock)
      }
    }

    if (this.writer.isActiveIndexer) this.clock.set(this.writer.core.key, this.length)
    return relinked
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
    this.tip = new Topolist()
    this.size = 0 // useful for debugging
    this.updated = false
    this.relinked = false
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

  static createNode (writer, length, value, heads, batch, dependencies, version, optimistic) {
    return new Node(writer, length, value, heads, batch, dependencies, version, optimistic)
  }

  // returns the global links of the dag, use this to link against the current state of the dag
  // TODO: rename to heads() and move the sets to _ props
  getHeads () {
    const heads = this._initialHeads.slice(0)
    for (const node of this.heads) heads.push({ key: node.writer.core.key, length: node.length })
    return heads
  }

  // TODO: might contain dups atm, nbd for how we use it, returns an array of writers you can "pull"
  // to get the full dag view at any time
  getBootstrapWriters () {
    const writers = []

    for (const head of this.heads) writers.push(head.writer)
    for (let i = 0; i < this.consensus.indexers.length; i++) writers.push(this.consensus.indexers[i])

    return writers
  }

  addHead (node) {
    const relinked = node.active()
    if (relinked) this.relinked = true

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

    this.tip.add(node)
    if (node.writer.isActiveIndexer) this.consensus.addHead(node)

    this.size++
    this.heads.add(node)

    this.updated = true

    return node
  }

  update () {
    if (!this.updated) return null
    this.updated = false

    // get the indexed nodes
    const indexed = []
    while (true) {
      const nodes = this.consensus.shift()
      if (!nodes.length) break

      this._yield(nodes, indexed)
    }

    const u = this.tip.flush(indexed)

    if (this.relinked === false) return u
    this.relinked = false

    // we are relinking a node we prev ignored as unlinked (removed head)
    // if so we need to scan the tip and make sure that we reprocess the nodes we prev ignored
    // so the system is consistent

    // TODO: there is def some optimisations that can be done here to avoid a full scan
    // like tracking the nodes that triggered the relink and only traverse the immediate
    // removed deps from that. Note that the unlinked nodes will ALWAYS be mem state tho
    // so its prop fine to do something simple as they were ignored as untrusted.

    let undo = 0
    for (let i = u.shared - 1; i >= 0; i--) {
      const node = i < u.indexed.length ? u.indexed[i] : u.tip[i - u.indexed.length]
      undo++
      if (!node.relink()) continue
      node.unlinked = false
      u.shared = i
      u.undo += undo
      undo = 0
    }

    return u
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

  /* Ack methods */

  shouldAck (writer, pending = false) {
    if (!writer || !writer.isActiveIndexer) return false

    // all indexers have to flushed to the dag before we ack as a quick "debounce"
    for (const w of this.indexers) {
      if (w.length !== w.available) return false
    }

    let isHead = false

    // if ANY head is not an indexer ack
    for (const head of this.heads) {
      if (!head.writer.isActiveIndexer) return true
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

    if (this.consensus.shouldAck(writer)) return true

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
  _shouldAckHeads (writer, pending) {
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

        if (pending || node.value !== null) {
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

  _yield (nodes, indexed = []) {
    const offset = indexed.length
    const tails = []

    // determine which nodes are yielded
    while (nodes.length) {
      const node = nodes.pop()

      if (node.yielding) continue
      node.yielding = true

      if (!node.dependencies.size) tails.push(node)

      nodes.push(...node.dependencies)
    }

    while (tails.length) {
      let tail = tails.pop()

      for (tail of this._removeBatch(tail)) {
        Topolist.add(tail, indexed, offset)
      }

      for (const dep of tail.dependents) {
        if (!dep.dependencies.size && dep.yielding) tails.push(dep)
      }
    }

    return indexed
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
    this.heads.delete(node)
    this.consensus.remove(node)

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
    const batch = [this._removeNode(node)]

    while (node.batch !== 1) { // its a batch!
      if (node.dependents.size === 0) { // bad batch node, auto correct
        const next = node.writer.get(node.length)
        if (next && next.batch === node.batch - 1) node.dependents.add(next)
      }

      assert(node.dependents.size === 1, 'Batch is linked partially, which is not allowed')

      node = getFirst(node.dependents)
      batch.push(this._removeNode(node))
    }

    return batch
  }
}

function tieBreak (a, b) {
  return Topolist.compare(a, b) < 0 // lowest key wis
}

function getFirst (set) {
  return set[Symbol.iterator]().next().value
}

function sameNode (a, b) {
  return b4a.equals(a.key, b.writer.core.key) && a.length === b.length
}
