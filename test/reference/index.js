const b4a = require('b4a')
const BufferMap = require('tiny-buffer-map')

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

  set (k, len) {
    this.seen.set(k, len)
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
  constructor (seq, writer, deps = []) {
    this.seq = seq
    this.length = seq + 1
    this.writer = writer
    this.clock = new Clock()
    this.active = false

    this.dependencies = deps
    this.deps = [...deps]
    this.dependents = []

    for (const d of deps) {
      // if (d.indexed) throw new Error('stop')
      d.dependents.push(this)
      this.clock.add(d.clock)
    }
  }

  get ref () {
    return this.writer.key + ':' + this.seq
  }
}

class Writer {
  constructor (key) {
    this.key = key
    this.core = { key }
    this.nodes = {
      offset: 0,
      nodes: []
    }
  }

  get available () {
    return this.nodes.nodes.length
  }

  get length () {
    return this.nodes.nodes.length
  }

  get (seq) {
    return this.nodes.nodes[seq] || null
  }

  add (...deps) {
    const node = new Node(this.length, this, deps)
    this.nodes.nodes.push(node)
    if (node.clock.get(this) < this.length) node.clock.set(this.key, this.length)
    return node
  }
}

class Linearizer {
  constructor (indexers, { heads = [] } = {}) {
    this.heads = new Set()
    this.tails = new Set()
    this.merges = []
    this.linearMajority = false
    this.majority = (indexers.length >>> 1) + 1
    this.indexers = indexers
    this.indexerHeads = new Map()
    this.removed = new Clock()
    this.pending = []
    this.shifted = []
    this.writers = new Map() // tmp solution...
    this.yielding = null

    for (const idx of indexers) {
      this.writers.set(idx.core.key, idx)
    }
    for (const { writer, length } of heads) {
      this.writers.set(writer.core.key, writer)
      this.removed.set(writer.core.key, length)
    }
  }

  addHead (node) {
    node.active = true
    this.writers.set(node.writer.core.key, node.writer)
    for (const dep of node.dependencies) {
      if (dep.indexed) {
        node.dependencies.splice(node.dependencies.indexOf(dep), 1)
      }
    }

    if (!node.dependencies.length) {
      this.tails.add(node)
    }

    for (const dep of node.dependencies) {
      this.heads.delete(dep)
    }

    if (node.dependencies.length > 1) this.merges.push(node)

    this.heads.add(node)
    this.indexerHeads.set(node.writer, node)

    while (true) {
      const n = this._shift()
      if (!n) break
      this.shifted.push(n)
    }

    return node
  }

  _tails (node) {
    const tails = new Set()
    for (const t of this.tails) {
      if (t.seq < node.clock.get(t.writer.core.key)) tails.add(t)
    }

    return tails
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
      if (length <= writer.nodes.offset) length = writer.nodes.offset

      const next = writer.get(length)

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
    const acks = [target] // TODO: can be cached on the target node in future (ie if we add one we dont have to check it again)

    for (const idx of this.indexers) {
      if (idx === target.writer) continue

      const next = target.clock.get(idx.core.key)
      const nextIndexNode = idx.get(next)

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
    const all = []
    for (const m of this.merges) {
      if (m !== node && node.clock.includes(m.writer.core.key, m.length)) {
        all.push(m)
      }
    }
    for (const t of this._tails(node)) {
       all.push(t)
    }
    return all
  }

  _debug (...m) {
    if (this.debug) console.log(this.debug + ':', ...m)
  }

  _yield (node, skip) {
    this.yielding = node

    // only stop when we find a tail
    while (node.dependencies.length) {
      // easy with only one dependency
      if (node.dependencies.length === 1) {
        node = node.dependencies[0]
        continue
      }

      let next = null

      if (!skip) {
        // for merges check if one fork is confirmed
        for (const t of this._tailsAndMerges(node)) {
          if (this._isConfirmed(t, node)) {
            this.yielding = next = t
            break
          }
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

    if (this.yielding === node) {
      this.yielding = null
    }

    return this._remove(node)
  }

  shift () {
    // return this._shift()
    return this.shifted.length ? this.shifted.shift() : null
  }

  _sameTails (a, b) {
    const t1 = this._tails(a)
    const t2 = this._tails(b)

    if (t1.size !== t2.size) return false

    for (const t of t1) {
      if (!t2.has(t)) return false
    }

    return true
  }

  _shift () {
    if (this.yielding) {
      return this._yield(this.yielding, true)
    }

    for (const merge of this.merges) {
      if (this._isConfirmed(merge)) {
        return this._yield(merge, false)
      }
    }

    for (const tail of this.tails) {
      if (this._isConfirmed(tail)) {
        return this._yield(tail, false)
      }
    }

    return null
  }

  _remove (node) {
    if (node.indexed) throw new Error('stop2')
    node.indexed = true

    this.heads.delete(node)
    this.tails.delete(node)
    const index = this.merges.indexOf(node)
    if (index > -1) this.merges.splice(index, 1)

    this.removed.set(node.writer.core.key, node.seq + 1)

    for (const d of node.dependents) {
      const i = d.dependencies.indexOf(node)
      d.dependencies.splice(i, 1)
      if (d.dependencies.length === 0 && d.active) {
        this.tails.add(d)
      }

      const j = d.deps.indexOf(node)
      d.deps.splice(j, 1)
    }

    return node
  }

  print () {
    let str = '```mermaid\n'
    str += 'graph TD;\n'

    for (const node of this.pending) {
      for (const dep of node.deps) {
        str += '    ' + node.ref + '-->' + dep.ref + ';\n'
      }
    }

    const visited = new Set()
    const stack = [...this.tails]

    while (stack.length) {
      const node = stack.shift()
      if (visited.has(node)) continue
      visited.add(node)

      stack.push(...node.dependents)

      for (const dep of node.deps) {
        str += '    ' + node.ref + '-->' + dep.ref + ';\n'
      }
    }

    str += '```'
    return str
  }
}

module.exports = { Linearizer, Writer }

function tieBreak (a, b) {
  return a.key < b.key
}

function isSubset (a, b) {
  for (const e of a) {
    if (!b.has(e)) return false
  }
  return true
}
