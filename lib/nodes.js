const {
  InputNode: InputNodeSchema,
  IndexNode: IndexNodeSchema
} = require('./messages')

class InputNode {
  constructor ({ key, seq, value, links, batch }) {
    this.key = (key && !Buffer.isBuffer(key)) ? Buffer.from(key, 'hex') : key
    this.value = (value && !Buffer.isBuffer(value)) ? Buffer.from(value) : value
    this.links = links
    this.batch = batch
    this.seq = seq
    this._id = null
  }

  get id () {
    if (this._id) return this._id
    this._id = this.key && this.key.toString('hex')
    return this._id
  }

  lt (other) {
    if (!other.links.has(this.id)) return false
    return this.seq < other.links.get(this.id)
  }

  lte (other) {
    if (!other.links.has(this.id)) return false
    return this.seq <= other.links.get(this.id)
  }

  static encode (node) {
    if (!(node instanceof InputNode)) node = new InputNode(node)
    if (node.links && node.links.has(node.id)) {
      // Remove any self-links, since they are implicit.
      node.links.delete(node.id)
    }
    return InputNodeSchema.fullEncode(node)
  }

  static decode (raw, from = {}) {
    if (!raw) return null
    try {
      const decoded = InputNodeSchema.decode({ start: 0, end: raw.length, buffer: raw })
      const node = new this({ ...decoded, key: from.key, seq: from.seq })
      // Add a self-link to the previous node in the input (if it isn't the first node).
      if (node.seq > 1) node.links.set(node.id, node.seq - 1)
      return node
    } catch (err) {
      // Gracefully discard malformed messages.
      return null
    }
  }
}

class IndexNode {
  constructor ({ node, batch, value, clock }) {
    this.node = node
    this.value = value
    this.batch = batch
    this.clock = clock
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

  equals (other) {
    return this.node.id === other.node.id && this.node.seq === other.node.seq
  }

  contains (other) {
    if (!this.clock.has(other.node.id)) return false
    const seq = this.clock.get(other.node.id)
    return seq >= other.node.seq
  }

  static encode (indexNode) {
    return IndexNodeSchema.fullEncode(indexNode)
  }

  static decode (raw) {
    if (!raw) return null
    const decoded = IndexNodeSchema.decode({ start: 0, end: raw.length, buffer: raw })
    const node = new InputNode(decoded)
    return new this({
      ...decoded,
      node
    })
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
