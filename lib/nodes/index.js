const b = require('b4a')

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
    this._id = this.key && this.key.toString('hex')
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
  constructor ({ header, change, clock, value, batch }) {
    this.header = header
    this.id = b.toString(change, 'hex')
    this.seq = clock.get(this.id)
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
    if (!this.clock.has(other.id)) return false
    return this.clock.get(other.id) >= other.seq
  }
}

module.exports = {
  InputNode,
  OutputNode
}

function lt (clock1, clock2) {
  if (!clock1.size || !clock2.size || clock1 === clock2) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length >= clock2.get(key)) return false
  }
  return true
}

function lte (clock1, clock2) {
  if (!clock1.size || !clock2.size) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length > clock2.get(key)) return false
  }
  return true
}

function gte (clock1, clock2) {
  if (!clock1.size || !clock2.size) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length < clock2.get(key)) return false
  }
  return true
}

function eq (clock1, clock2) {
  return lte(clock1, clock2) && gte(clock2, clock1)
}
