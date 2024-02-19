const b4a = require('b4a')

module.exports = class TopoList {
  constructor () {
    this.tip = []
    this.undo = 0
    this.shared = 0

    this._shared = 0
  }

  mark () {
    this._shared = this.shared = this.tip.length
    this.undo = 0
  }

  // todo: bump to new api that just tracks undo
  flush (indexed = []) {
    if (indexed.length) this._applyIndexed(indexed)

    const u = {
      shared: this.shared,
      undo: this.undo,
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
    const len = Math.min(nodes.length, this.tip.length)
    let shared = 0

    for (; shared < len; shared++) {
      if (this.tip[shared] !== nodes[shared]) break
    }

    if (shared === this.tip.length) {
      this.undo = 0 // nothing reordered
      this.tip = []
      return
    }

    if (shared === nodes.length) {
      this.undo = 0 // nothing reordered
      this.tip = this.tip.slice(shared)
      return
    }

    this.undo = this.shared - shared
    this.shared = shared

    let found = 0
    for (let i = shared; i < this.tip.length; i++) {
      const node = this.tip[i]
      if (node.yielded) continue
      this.tip[found++] = node
    }

    while (this.tip.length > found) this.tip.pop()
  }

  add (node) {
    const i = addSorted(node, this.tip)
    this._track(i)
  }

  _track (changed) {
    if (changed < this.shared) {
      this.shared = changed
      this.undo = this._shared - this.shared
    }
  }
}

function addSorted (node, list) {
  list.push(node)

  let i = list.length - 1

  while (i >= 1) {
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

function links (a, b) {
  for (const node of a.dependencies) {
    if (b === node) return true
  }

  return a.length > 0 && b.length === a.length - 1 && a.writer === b.writer
}

function cmp (a, b) {
  const c = b4a.compare(a.writer.core.key, b.writer.core.key)
  return c === 0 ? a.length < b.length ? -1 : 1 : c
}
