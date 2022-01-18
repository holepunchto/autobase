const b = require('b4a')
const { get, lt, lte, gte, eq } = require('./clock')

class InputNode {
  constructor ({ header, key, seq, value, clock, batch }) {
    this.value = (value && !b.isBuffer(value)) ? b.from(value) : value
    this.batch = batch || [0, 0]
    this.key = key
    this.header = header
    this.clock = clock
    this.seq = seq
  }

  lt (other) {
    const o = get(other.clock, this.key)
    return o && (this.seq < o[1])
  }

  lte (other) {
    const o = get(other.clock, this.key)
    return o && (this.seq <= o[1])
  }

  contains (other) {
    const o = get(this.clock, other.key)
    return o && (o[1] > other.seq)
  }
}

class OutputNode {
  constructor ({ header, change, clock, value, batch }) {
    this.seq = get(clock, change)[1]
    this.header = header
    this.change = change
    this.clock = clock
    this.value = value
    this.batch = batch
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
    const o = get(this.clock, other.change)
    return o && (o[1] >= other.seq)
  }
}

module.exports = {
  InputNode,
  OutputNode
}
