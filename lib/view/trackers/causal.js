const { eq, gt } = require('../../clock')

class MemoryTracker {
  constructor (nodes, lengths) {
    this.nodes = nodes
    this.truncations = new Map()
    this.head = null
    this.invalid = false
  }

  _head (id) {
    const truncation = this.truncations.get(id) || 0
    const nodes = this.nodes.get(id)
    if (!nodes) return null
    return nodes[nodes.length - truncation - 1]
  }

  _bestMemoryHead () {
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

  async update (node) {
    if (!this.opened) await this.open()
    if (this.head && eq(this.head.clock, node.clock)) return { length: this.length, operations: this.head.operations }
    while (this.head && !eq(this.head.clock, node.clock) && this.head.contains(node)) {
      await this.pop()
    }
    if (this.head && eq(this.head.clock, node.clock)) return { length: this.length, operations: this.head.operations }
    return null
  }
}

module.exports = MemoryTracker
