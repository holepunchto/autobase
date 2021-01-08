const {
  InputNode: InputNodeSchema,
  OutputNode: OutputNodeSchema
} = require('./messages')

class InputNode {
  constructor({ key, seq, value, links }) {
    this.key = Buffer.isBuffer(key) ? key.toString('hex') : key
    this.links = linksToMap(links)
    this.seq = seq
    this.value = value
  }

  lt(other) {
    if (!other.links.has(this.key)) return false
    return this.seq < other.links.get(this.key)
  }

  lte(other) {
    if (!other.links.has(this.key)) return false
    return this.seq <= other.links.get(this.key)
  }

  static encode(node) {
    return InputNodeSchema.encode({
      value: node.value,
      links: intoObj(node.links)
    })
  }

  static decode(raw) {
    if (!raw) return null
    try {
      return new this(InputNodeSchema.decode(raw))
    } catch (err) {
      // Gracefully discard malformed messages.
      return null
    }
  }
}

class OutputNode {
  constructor ({ node, clock }) {
    this.node = node
    this.clock = linksToMap(clock)
  }

  lt (other) {
    return lt(this.clock, other.clock)
  }

  lte (other) {
    return lte(this.clock, other.clock)
  }

  equals (other) {
    return this.node.key === other.node.key && this.node.seq === other.node.seq
  }

  contains (other) {
    if (!this.clock.has(other.node.key)) return false
    const seq = this.clock.get(other.node.key)
    return seq >= other.node.seq
  }

  static encode (outputNode) {
    if (outputNode.node.links) outputNode.node.links = intoObj(outputNode.node.links)
    return OutputNodeSchema.encode({
      node: outputNode.node,
      key: outputNode.node.key,
      seq: outputNode.node.seq,
      clock: intoObj(outputNode.clock)
    })
  }

  static decode (raw) {
    if (!raw) return null
    const decoded = OutputNodeSchema.decode(raw)
    const node = new InputNode(decoded.node)
    node.key = decoded.key
    node.seq = decoded.seq
    return new this({
      clock: decoded.clock,
      node
    })
  }
}

module.exports = {
  InputNode,
  OutputNode
}

function linksToMap (links) {
  if (!links) return new Map()
  if (links instanceof Map) return links
  if (Array.isArray(links)) return new Map(links)
  return new Map(Object.entries(links))
}

function intoObj (links) {
  if (links instanceof Map || Array.isArray(links)) {
    const obj = {}
    for (let [key, value] of links) {
      obj[key] = value
    }
    return obj
  }
  return links
}

function lt(clock1, clock2) {
  if (!clock1.size || !clock2.size || clock1 === clock2) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length >= clock2.get(key)) return false
  }
  return true
}

function lte(clock1, clock2) {
  if (!clock1.size || !clock2.size) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length > clock2.get(key)) return false
  }
  return true
}
