const b4a = require('b4a')

module.exports = class TopoList {
  constructor () {
    this.ordered = []
    this.pushed = 0
    this.popped = 0
    this.shared = 0

    this._shared = 0
  }

  mark () {
    this._shared = this.shared = this.ordered.length
    this.pushed = this.popped = 0
  }

  _applyTip () {
    for (const node of this._tip) {
      const p = addSorted(node, this.tip)
    }
  }

  // todo: bump to new api that just tracks undo
  flush (indexed = []) {
    if (indexed.length) this._applyIndexed(indexed)

    const u = {
      shared: this.shared,
      pushed: this.pushed,
      popped: this.popped,
      indexed,
      tip: this.ordered
    }

    this.mark()

    return u
  }

  print () {
    return this.ordered.map(n => n.writer.core.key.toString() + n.length)
  }

  _applyIndexed (nodes) {
    const len = Math.min(nodes.length, this.ordered.length)
    let shared = 0

    for (; shared < len; shared++) {
      if (this.ordered[shared] !== nodes[shared]) break
    }

    for (const node of nodes) {
      const i = this.ordered.indexOf(node)
      if (i === -1) continue
      this.ordered[i] = null
    }

    const ordered = this.ordered
    this.ordered = new Array(ordered.length - nodes.length)

    if (shared === len) {
      this.popped = 0 // nothing reordered
    } else {
      this.popped = this.shared - shared
      this.pushed = ordered.length - shared
      this.shared = shared
    }

    let found = 0
    for (let i = 0; i < ordered.length; i++) {
      const node = ordered[i]

      if (node === null) {
        found++
        continue
      }

      this.ordered[i - found] = node
    }
  }

  add (node) {
    this.ordered.push(node)

    let i = this.ordered.length - 1

    while (i >= 1) {
      const prev = this.ordered[i - 1]
      if (links(node, prev)) break
      this.ordered[i] = prev
      this.ordered[--i] = node
    }

    while (i < this.ordered.length - 1) {
      const next = this.ordered[i + 1]
      const c = cmp(node, next)
      if (c <= 0) break
      this.ordered[i] = next
      this.ordered[++i] = node
    }

    this._track(i)

    return i
  }

  _track (changed) {
    if (changed < this.shared) {
      this.shared = changed
      this.popped = this._shared - this.shared
    }

    this.pushed = this.ordered.length - this.shared
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
