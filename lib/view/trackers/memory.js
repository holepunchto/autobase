const { eq, gt } = require('../../clock')

class MemoryTracker {
  constructor (nodes, lengths) {
    this.nodes = nodes
    this.lengths = lengths
    this.invalid = false

    this._truncations = new Map()
    for (const id of this.nodes) {
      this.truncations.set(id, 0)
    }
  }

  _head (id) {
    const nodes = this.nodes.get(id)
    if (!nodes || !nodes.length) return null
    return nodes[nodes.length - this._truncations.get(id) - 1]
  }

  _bestHead () {
    let max = null
    for (const id of this.nodes.keys()) {
      const head = this._head(id)
      if (!head) continue
      if (!max || gt(head.clock, max.clock)) {
        max = head
      }
    }
    return max
  }

  _pop (clock) {
    for (const id of this.nodes.keys()) {
      let truncation = this.truncations.get(id)
      let head = this._head(id)
      while (head && eq(head.clock, clock)) {
        this.truncations.set(id, ++truncation)
        head = this._head(id)
      }
    }
  }

  _intersections () {
    const intersections = new Map()
    for (const [id, length] of this.lengths) {
      intersections.set(id, length - this._truncations.get(id))
    }
    return intersections
  }

  update (node) {
    let head = this._bestHead()
    while (head && !eq(head.clock, node.clock) && head.contains(node)) {
      this._pop(head.clock)
      head = this._bestHead()
    }
    if (head && eq(head.clock, node.clock)) return this._intersections()
    return null
  }
}

module.exports = MemoryTracker
