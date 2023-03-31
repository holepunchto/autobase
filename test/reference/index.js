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

  includes (w, seq) {
    return this.seen.has(w) && this.seen.get(w) > seq
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
  constructor (seq, writer, deps = []) {
    this.seq = seq
    this.writer = writer
    this.clock = new Clock()

    this.dependencies = deps
    this.deps = [...deps]
    this.dependents = []

    for (const d of deps) {
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
    this.nodes = []
  }

  get length () {
    return this.nodes.length
  }

  get (seq) {
    return this.nodes[seq] || null
  }

  add (...deps) {
    const node = new Node(this.length, this, deps)
    this.nodes.push(node)
    if (node.clock.get(this) < this.length) node.clock.set(this, this.length)
    return node
  }
}

class Linearizer {
  constructor (indexers) {
    this.heads = new Set()
    this.tails = new Set()
    this.merges = new Set()
    this.linearMajority = false
    this.majority = (indexers.length >>> 1) + 1
    this.indexers = indexers
    this.indexerHeads = new Map()
    this.removed = new Clock()
    this.pending = []
  }

  addHead (node) {
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

    if (node.dependencies.length > 1) this.merges.add(node)

    this.heads.add(node)
    this.indexerHeads.set(node.writer, node)

    while (this._shiftPending()) {}

    return node
  }

  _tails (node) {
    const tails = new Set()
    for (const t of this.tails) {
      if (t.seq < node.clock.get(t.writer)) tails.add(t)
    }

    return tails
  }

  _strictlyNewer (node, parent) {
    if (node === parent) return true
    if (node.seq >= parent.clock.get(node.writer)) return false
    for (const d of parent.dependencies) {
      if (!this._strictlyNewer(node, d)) return false
    }
    return true
  }

  _isConfirmed (target, parent = null) {
    const self = this
    const idx = this.indexers
    const removed = this.removed
    const isLinear = this.linearMajority
    const thres = this.majority

    return confirms(target, new Set(), new Set())

    function confirms (node, seen, confs) {
      if (parent && node.seq >= parent.clock.get(node.writer)) return false

      seen.add(node.writer)

      if (seen.size >= thres) {
        confs.add(node.writer)

        if (confs.size >= thres) return true
        if (parent && preferred(confs)) return true
      }

      let last = null
      for (const dep of node.dependents) {
        if (!self._strictlyNewer(target, dep)) continue

        const next = isLinear ? new Set(confs) : confs

        if (last && confirms(last, new Set(seen), next)) return true
        last = dep
      }
      if (last && confirms(last, seen, confs)) return true

      return false
    }

    function preferred (confs) {
      let available = 0
      for (const w of idx) {
        if (confs.has(w)) continue
        const len = parent.clock.get(w)

        if (removed.get(w) < len) {
          if (target.clock.get(w) < len) {
            const check = w.get(len - 1)
            if (!self._strictlyNewer(target, check)) {
              continue
            }
          }
        }

        available++
        if (confs.size + available >= thres) return true
      }
    }
  }

  _tailsAndMerges (node) {
    const all = this._tails(node)
    for (const m of this.merges) {
      if (m !== node && node.clock.includes(m.writer, m.seq)) {
        all.add(m)
      }
    }
    return all
  }

  _debug (...m) {
    if (this.debug) console.log(this.debug + ':', ...m)
  }

  shift () {
    const node = this.pending.shift() || null
    if (!node) return null

    for (const d of node.dependents) {
      const i = d.deps.indexOf(node)
      d.deps.splice(i, 1)
    }

    return node
  }

  _yield (node) {
    if (node.dependencies.length === 0) return this._remove(node)
    if (node.dependencies.length === 1) return this._yield(node.dependencies[0])

    for (const t of this._tailsAndMerges(node)) {
      if (this._isConfirmed(t, node)) {
        return this._yield(t)
      }
    }

    let best = null
    for (const t of this._tails(node)) {
      if (best === null || tieBreak(t.writer, best.writer)) {
        best = t
      }
    }

    return this._yield(best)
  }

  _shiftPending () {
    for (const tail of this.tails) {
      this._debug('tail', tail.writer.key, tail.seq, this._isConfirmed(tail))

      if (this._isConfirmed(tail)) {
        this.pending.push(this._yield(tail))
        return true
      }
    }

    for (const merge of this.merges) {
      this._debug('merge', merge.writer.key, merge.seq, this._isConfirmed(merge))

      if (this._isConfirmed(merge)) {
        const node = this._yield(merge)
        this.pending.push(node)
        return true
      }
    }

    return false
  }

  _remove (node) {
    node.indexed = true

    this.tails.delete(node)
    this.merges.delete(node)

    this.removed.set(node.writer, node.seq + 1)

    for (const d of node.dependents) {
      const i = d.dependencies.indexOf(node)
      d.dependencies.splice(i, 1)
      if (d.dependencies.length === 0) this.tails.add(d)
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
