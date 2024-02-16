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
    let mutual = 0

    for (; mutual < len; mutual++) {
      if (this.ordered[mutual] !== nodes[mutual]) break
    }

    for (const node of nodes) {
      const i = this.ordered.indexOf(node)
      if (i === -1) continue
      this.ordered[i] = null
    }

    const ordered = this.ordered
    this.ordered = new Array(ordered.length - nodes.length)

    let found = 0
    let reorg = false

    for (let i = 0; i < ordered.length; i++) {
      const node = ordered[i]

      if (node === null) {
        found++
        continue
      }

      if (found < nodes.length && !reorg) {
        if (i < this.shared) {
          this.popped += this.shared - i
          this.pushed = ordered.length - found
        }

        this.shared = i
        reorg = true
      }

      this.ordered[i - found] = node
    }

    const u = {
      shared: this.shared,
      pushed: this.pushed,
      popped: this.popped,
      indexed: nodes,
      tip: this.ordered
    }

    this.mark()

    return u
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
