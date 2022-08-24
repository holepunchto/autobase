const { eq, gte } = require('../../clock')

class MemoryTracker {
  constructor (nodes, lengths) {
    this.nodes = nodes
    this.lengths = lengths
    this.invalid = false

    this._truncations = new Array(this.nodes.length)
    for (let id = 0; id < this.nodes.length; id++) {
      this._truncations[id] = 0
    }
  }

  _head (id) {
    const nodes = this.nodes[id]
    if (!nodes || !nodes.length) return null
    return nodes[nodes.length - this._truncations[id] - 1]
  }

  _bestHead () {
    let max = null
    for (let id = 0; id < this.nodes.length; id++) {
      const head = this._head(id)
      if (!head) continue
      if (!max || gte(head.clock, max.clock)) {
        max = head
      }
    }
    return max
  }

  _pop (clock) {
    for (let id = 0; id < this.nodes.length; id++) {
      let truncation = this._truncations[id]
      let head = this._head(id)
      while (head && eq(head.clock, clock)) {
        this._truncations[id] = ++truncation
        head = this._head(id)
      }
    }
  }

  _intersections () {
    const intersections = new Array(this.nodes.length)
    for (let id = 0; id < this.nodes.length; id++) {
      intersections[id] = this.lengths[id] - this._truncations[id]
    }
    return intersections
  }

  intersect (node) {
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
