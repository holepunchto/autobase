const b4a = require('b4a')
const assert = require('nanoassert')

module.exports = class TopoList {
  constructor () {
    this.tip = []
    this.undo = 0
    this.shared = 0
  }

  static compare (a, b) {
    return cmp(a, b)
  }

  static add (node, indexed, offset) {
    addSortedOptimistic(node, indexed, offset)
  }

  mark () {
    this.shared = this.tip.length
    this.undo = 0
  }

  // todo: bump to new api that just tracks undo
  flush (indexed = []) {
    if (indexed.length) this._applyIndexed(indexed)

    const u = {
      shared: this.shared,
      undo: this.undo,
      length: indexed.length + this.tip.length,
      indexed,
      tip: this.tip
    }

    this.mark()

    return u
  }

  print () {
    return this.tip.map(n => n.writer.core.key.toString() + n.length)
  }

  _applyIndexed (nodes) {
    assert(nodes.length <= this.tip.length, 'Indexed batch cannot exceed tip')

    let shared = 0

    for (; shared < nodes.length; shared++) {
      if (this.tip[shared] !== nodes[shared]) break
    }

    // reordering
    if (shared < nodes.length) this._track(shared)

    const tip = []

    for (let i = shared; i < this.tip.length; i++) {
      const node = this.tip[i]
      if (node.yielded) continue
      const s = addSortedOptimistic(node, tip, 0)
      if (s === tip.length - 1) continue
      this._track(shared + s)
    }

    this.tip = tip
  }

  add (node) {
    const shared = addSortedOptimistic(node, this.tip, 0)
    this._track(shared)
  }

  _track (shared) {
    if (shared < this.shared) {
      this.undo += this.shared - shared
      this.shared = shared
    }
  }
}

function hasOptimisticNodes (node) {
  if (node.optimistic) return true

  for (const d of node.dependencies) {
    if (d.optimistic) return true
  }

  return false
}

function addSortedOptimistic (node, list, offset) {
  if (!hasOptimisticNodes(node)) return addSorted(node, list, offset)

  let shared = list.length

  if (!node.optimistic) {
    for (let i = 0; i < list.length; i++) {
      const d = list[i]
      if (!d.optimistic) continue

      const pos = sort(d, list, i, offset)
      if (pos !== i && pos < shared) {
        shared = pos
      }
    }
  }

  const pos = addSorted(node, list, offset)
  if (pos < shared) {
    shared = pos
  }

  for (let i = list.length - 1; i >= 0; i--) {
    const d = list[i]
    if (!d.optimistic) continue
    const pos = sortReverse(d, list, i, offset)
    if (pos !== i && i < shared) {
      shared = i // note, i here is shared cause backwards
    }
  }

  return shared
}

function addSorted (node, list, offset) {
  list.push(node)
  return sort(node, list, list.length - 1, offset)
}

function sort (node, list, i, offset) {
  while (i >= offset + 1) {
    const prev = list[i - 1]
    if (links(node, prev)) break
    list[i] = prev
    list[--i] = node
  }

  while (i < list.length - 1) {
    const next = list[i + 1]
    const c = cmp(node, next)
    if (c <= 0) break
    list[i] = next
    list[++i] = node
  }

  return i
}

function sortReverse (node, list, i, offset) {
  let linked = null

  while (i < list.length - 1) {
    const next = list[i + 1]
    if (links(next, node)) {
      linked = next
      break
    }
    list[i] = next
    list[++i] = node
  }

  while (i >= offset + 1) {
    const prev = list[i - 1]
    if (linked && !links(linked, prev)) break
    const c = cmpUnlinked(node, prev)
    if (c >= 0) break
    list[i] = prev
    list[--i] = node
  }

  return i
}

function links (a, b) {
  if (b.dependents.has(a)) return true
  return a.length > 0 && b.length === a.length - 1 && a.writer === b.writer
}

function cmp (a, b) {
  return links(b, a) ? -1 : cmpUnlinked(a, b)
}

function cmpUnlinked (a, b) {
  const c = b4a.compare(a.writer.core.key, b.writer.core.key)

  if (c !== 0) {
    // a node marked for optimistic execution always sorts AFTER one who didnt for security
    if (a.optimistic !== b.optimistic) return a.optimistic ? 1 : -1
  }

  return c === 0 ? a.length < b.length ? -1 : 1 : c
}
