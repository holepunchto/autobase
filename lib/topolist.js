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

  _applyIndex () {
    const len = Math.min(this._indexed.length, this.tip.length)
    let same = 0

    for (; same < len; same++) {
      if (this.tip[same] !== this._indexed[same]) break
    }

    this.same = same

    if (same === this.tip.length) {
      this.undo = 0
      this.tip = []
      return
    }

    if (same === this._indexed.length) {
      this.undo = 0
      this.same = this.tip.length
      this.tip = this.tip.slice(same)
      return
    }

    this.undo = this.tip.length - same

    let p = same

    for (let i = same; i < this.tip.length; i++) {
      const node = this.tip[i]
      if (node.yielded) continue
      this.tip[p++] = node
    }

    while (p < this.tip.length) this.tip.pop()
  }

  _applyTip () {
    for (const node of this._tip) {
      const p = addSorted(node, this.tip)
    }
  }

  flush () {
    this.mark()

    if (this._indexed) this._applyIndex()

    return {
      shared,
      pushed,
      popped,
      indexed,
      tip
    }
  }

  indexed (nodes) {
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

    let j = 0
    for (let i = 0; i < ordered.length; i++) {
      const node = ordered[i]
      if (node === null) {
        if (i < shared) {
          this.shared--
          this._shared--
        }
        continue
      }

      this.ordered[j++] = node
    }

    return {
      shared,
      pushed: nodes.length > this.ordered.length ? nodes.length - this.ordered.length : 0,
      popped: 0
      indexed: nodes,
      tip: this.ordered
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
