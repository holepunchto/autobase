const b4a = require('b4a')
const assert = require('nanoassert')

module.exports = class TopoList {
  constructor() {
    this.tip = []
    this.undo = 0
    this.shared = 0
  }

  static compare(a, b) {
    return cmp(a, b)
  }

  static add(node, indexed, offset) {
    addSorted(node, indexed, offset)
  }

  mark() {
    this.shared = this.tip.length
    this.undo = 0
  }

  // todo: bump to new api that just tracks undo
  flush(indexed = []) {
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

  print() {
    return this.tip.map((n) => n.writer.core.key.toString() + n.length)
  }

  _applyIndexed(nodes) {
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
      const s = addSorted(node, tip, 0)
      if (s === tip.length - 1) continue
      this._track(shared + s)
    }

    this.tip = tip
  }

  add(node) {
    const shared = addSorted(node, this.tip, 0)
    this._track(shared)
  }

  _track(shared) {
    if (shared < this.shared) {
      this.undo += this.shared - shared
      this.shared = shared
    }
  }
}

function hasOptimisticDeps(node) {
  for (const d of node.dependencies) {
    if (d.optimistic) return true
  }
  return false
}

// this can be optimised to only return optimistic nodes that we will "touch" when moving down the node
function getOptimisticDeps(node, list) {
  const deps = new Set()
  const stack = [node]

  while (stack.length > 0) {
    const next = stack.pop()
    if (deps.has(next)) continue
    if (next.optimistic && next !== node) deps.add(next)
    for (const d of next.dependencies) {
      if (d.optimistic) stack.push(d)
    }
  }

  const result = []

  for (let i = list.length - 1; deps.size !== result.length && i >= 0; i--) {
    const n = list[i]
    if (deps.has(n)) result.push(n)
  }

  return result.reverse()
}

function addSortedOptimistic(node, list, offset) {
  const opt = getOptimisticDeps(node, list)
  const pos = new Uint32Array(opt.length)

  for (let i = 0; i < opt.length; i++) {
    const n = opt[i]
    pos[i] = n.index
    moveDown(n, list, offset)
  }

  moveDownAndUp(node, list, offset)

  for (let i = opt.length - 1; i >= 0; i--) {
    moveOptimisticUp(opt[i], list, offset)
  }

  let shared = node.index

  for (let i = 0; i < opt.length; i++) {
    const idx = pos[i]
    const n = opt[i]

    if (idx === n.index) continue

    if (idx < shared) shared = idx
    if (n.index < shared) shared = n.index
  }

  return shared
}

function addSorted(node, list, offset) {
  list.push(node)
  node.index = list.length - 1

  // "slow" path, 1% of nodes
  if (hasOptimisticDeps(node)) return addSortedOptimistic(node, list, offset)

  moveDownAndUp(node, list, offset)
  return node.index
}

function moveDown(node, list, offset) {
  while (node.index > offset) {
    const prev = list[node.index - 1]
    if (links(node, prev)) break
    list[(prev.index = node.index)] = prev
    list[--node.index] = node
  }
}

function moveOptimisticUp(node, list, offset) {
  // if optimistic move all the way up
  while (node.index < list.length - 1) {
    const next = list[node.index + 1]
    if (links(next, node)) break
    list[(next.index = node.index)] = next
    list[++node.index] = node
  }

  // stable sort in case multiple children
  while (node.index > offset) {
    const prev = list[node.index - 1]
    if (!prev.optimistic) break
    const c = cmp(prev, node)
    if (c <= 0) break
    list[(prev.index = node.index)] = prev
    list[--node.index] = node
  }
}

function moveNonOptimisticUp(node, list, offset) {
  // stable sort against next non optimistic node
  while (node.index < list.length - 1) {
    const next = list[node.index + 1]
    const c = cmpNonOptimistic(node, next, list)
    if (c <= 0) break
    list[(next.index = node.index)] = next
    list[++node.index] = node
  }
}

function moveDownAndUp(node, list, offset) {
  moveDown(node, list, offset)

  if (node.optimistic) moveOptimisticUp(node, list, offset)
  else moveNonOptimisticUp(node, list, offset)
}

function links(a, b) {
  if (b.dependents.has(a)) return true
  return a.length > 0 && b.length === a.length - 1 && a.writer === b.writer
}

function cmpNonOptimistic(a, b, list) {
  if (!b.optimistic) return cmp(a, b)

  for (let i = b.index + 1; i < list.length; i++) {
    const node = list[i]
    if (!node.optimistic) return cmp(a, node)
  }

  return -1
}

function cmp(a, b) {
  return links(b, a) ? -1 : cmpUnlinked(a, b)
}

function cmpUnlinked(a, b) {
  const c = b4a.compare(a.writer.core.key, b.writer.core.key)
  return c === 0 ? (a.length < b.length ? -1 : 1) : c
}
