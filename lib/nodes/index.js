const cenc = require('compact-encoding')
const {
  InputNode: InputNodeSchema,
  IndexNode: IndexNodeSchema
} = require('./messages')

class InputNode {
  constructor ({ header, key, seq, value, clock, batch }) {
    this.header = header
    this.key = (key && !Buffer.isBuffer(key)) ? Buffer.from(key, 'hex') : key
    this.value = (value && !Buffer.isBuffer(value)) ? Buffer.from(value) : value
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

  static encode (node) {
    if (!(node instanceof InputNode)) node = new InputNode(node)
    if (node.clock && node.clock.has(node.id)) {
      // Remove any self-links, since they are implicit.
      node.clock.delete(node.id)
    }
    return cenc.encode(InputNodeSchema, node)
  }

  static decode (raw, from = {}) {
    if (!raw) return null
    try {
      const decoded = cenc.decode(InputNodeSchema, raw)
      const node = new this({ ...decoded, key: from.key, seq: from.seq })
      // Add a self-link to the previous node in the input (if it isn't the first node).
      if (node.seq > 0) node.clock.set(node.id, node.seq - 1)
      return node
    } catch (err) {
      // Gracefully discard malformed messages.
      return null
    }
  }
}

class IndexNode {
  constructor ({ header, change, clock, value, batch }) {
    this.header = header
    this.id = change.toString('hex')
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

  static encode (indexNode) {
    return cenc.encode(IndexNodeSchema, indexNode)
  }

  static decode (raw) {
    if (!raw) return null
    return new this(cenc.decode(IndexNodeSchema, raw))
  }
}

module.exports = {
  InputNode,
  IndexNode
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
