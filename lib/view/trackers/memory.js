const { eq } = require('../../clock')

class AppliedBranch {
  constructor (autobase, nodes, oldLength, oldClock) {
    this.autobase = autobase
    this.nodes = nodes
    this.length = oldLength

    this.truncated = 0
    this.head = null
    this.opened = false

    this._ite = this.autobase.createCausalStream({
      clock: oldClock
    })[Symbol.asyncIterator]()
  }

  get operations () {
    if (!this.head) return 0
    return this.head.operations
  }

  async open () {
    if (this.nodes.length) {
      this.head = this.nodes[this.nodes.length - 1]
    } else {
      const next = await this._ite.next()
      this.head = next.value
    }
    this.opened = true
  }

  async pop () {
    if ((this.truncated + 1) < this.nodes.length) {
      this.head = this.nodes[this.nodes.length - this.truncated]
    } else {
      const next = await this._ite.next()
      this.head = next.value
    }
    if (this.head) {
      this.truncated++
      this.length--
    } else {
      return null
    }
    if (this.head && this.head.batch[0] === 0) return null
    return this.pop()
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

  slice () {
    if (!this.truncated) return this.nodes
    return this.nodes.slice(0, this.nodes.length - this.truncated)
  }
}

module.exports = AppliedBranch
