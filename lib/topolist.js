const b4a = require('b4a')
const assert = require('nanoassert')

module.exports = class TopoList {
  constructor () {
    this.optimistic = []
    this.tip = []
    this.undo = 0
    this.shared = 0
  }

  static compare (a, b) {
    return cmp(a, b)
  }

  static add (node, indexed, offset) {
    addSorted(node, indexed, offset)
  }

  mark () {
    this.shared = this.tip.length + this.optimistic.length
    this.undo = 0
  }

  // todo: bump to new api that just tracks undo
  flush (indexed = []) {
    if (indexed.length) this._applyIndexed(indexed)

    const u = {
      shared: this.shared,
      undo: this.undo,
      length: indexed.length + this.tip.length + this.optimistic.length,
      indexed,
      tip: this.optimistic.length ? this.tip.concat(this.optimistic) : this.tip
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
      const s = addSorted(node, tip, 0)
      if (s === tip.length - 1) continue
      this._track(shared + s)
    }

    this.tip = tip
  }

  add (node) {
    if (node.optimistic) {
      const shared = addSorted(node, this.optimistic, 0)
      this._track(this.tip.length + shared)
      return
    }

    if (this.optimistic.length) this._promoteDependencies(node)

    const shared = addSorted(node, this.tip, 0)
    this._track(shared)
  }

  _promoteDependencies (node) {
    const stack = [node]

    while (stack.length) {
      const next = stack.pop()

      for (const node of next.dependencies) {
        if (!node.optimistic) continue

        const i = this.optimistic.indexOf(node)
        if (i === -1) continue // already promoted

        this.optimistic.splice(node, i)
        stack.push(node)

        const shared = addSorted(node, this.top, 0)
        this._track(shared)
      }
    }
  }

  _track (shared) {
    if (shared < this.shared) {
      this.undo += this.shared - shared
      this.shared = shared
    }
  }
}

function addSorted (node, list, offset) {
  list.push(node)

  let i = list.length - 1

  while (i >= offset + 1) {
    const prev = list[i - 1]
    if (links(node, prev)) break
    list[i] = prev
    list[--i] = node
  }

  while (i < list.length - 1) {
    const next = list[i + 1]
    const c = cmpUnlinked(node, next)
    if (c <= 0) break
    list[i] = next
    list[++i] = node
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
  return c === 0 ? a.length < b.length ? -1 : 1 : c
}
