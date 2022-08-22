const b = require('b4a')
const { lt, lte, gte, eq } = require('../clock')

class InputNode {
  constructor ({ header, key, seq, value, clock, batch }) {
    this.header = header
    this.key = (key && !b.isBuffer(key)) ? b.from(key, 'hex') : key
    this.value = (value && !b.isBuffer(value)) ? b.from(value) : value
    this.batch = batch || [0, 0]
    this.clock = clock
    this.seq = seq
    this._id = null
  }

  get id () {
    if (this._id) return this._id
    this._id = this.key && b.toString(this.key, 'hex')
    return this._id
  }

  lt (other) {
    if (!other.clock.has(this.id)) return false
    return this.seq < other.clock.get(this.id)
  }

  lte (other) {
    if (!other.clock.has(this.id)) return false
    return this.seq <= other.clock.get(this.id)
  }

  contains (other) {
    if (!this.clock.has(other.id)) return false
    return this.clock.get(other.id) >= other.seq
  }
}

class OutputNode {
  constructor ({ id, header, change, clocks, clock, value, batch, operations }, outputId) {
    this.id = b.toString(change, 'hex')
    this.header = header
    this.change = change
    this.clocks = clocks
    this.value = value
    this.batch = batch

    if (outputId !== null) {
      // This is coming from an output core
      if (clocks) {
        this.seq = clocks.length ? clocks[outputId].clock.get(this.id) : 0
        this.clock = clocks.length ? clocks[outputId].clock : new Map()
      } else {
        this.seq = clock.get(this.id) // backwards compat
        this.clock = clock // backwards compat
      }
    } else {
      // This is coming from the causal stream
      this.seq = clock.get(this.id)
      this.clock = clock
    }
  }

  lt (other) {
    return lt(this.clock, other.clock)
  }

  lte (other) {
    return lte(this.clock, other.clock)
  }

  gte (other) {
    return gte(this.clock, other.clock)
  }

  eq (other) {
    return eq(this.clock, other.clock)
  }

  contains (other) {
    if (!this.clock.has(other.id)) return false
    return this.clock.get(other.id) >= other.seq
  }
}

module.exports = {
  InputNode,
  OutputNode
}
