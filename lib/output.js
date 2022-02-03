const c = require('compact-encoding')

const KeyCompressor = require('./compression')
const { OutputNode } = require('./nodes')
const { Node: NodeSchema } = require('./nodes/messages')

module.exports = class Output {
  constructor (output, opts = {}) {
    this.output = output
    this.compressor = opts.compressor || new KeyCompressor(this.output)
  }

  get key () {
    return this.output.key
  }

  get length () {
    return this.output.length
  }

  update () {
    return this.output.update()
  }

  snapshot () {
    return new Output(this.output.snapshot(), {
      compressor: this.compressor
    })
  }

  async get (seq, opts = {}) {
    const raw = await this.output.get(seq)
    if (!raw) return null

    const state = { start: 0, end: raw.length, buffer: raw }
    const decoded = NodeSchema.decode(state)
    const [clock, change] = await Promise.all([
      this._keyCompressor.decompress(decoded.clock, seq),
      this._keyCompressor.resolvePointer(decoded.change)
    ])
    decoded.clock = clock
    decoded.change = change

    return new OutputNode(decoded)
  }

  async append (nodes) {
    const batch = []
    for (let i = 0; i < nodes.length; i++) {
      const node = nodes[i]
      const { keys, clock } = await this.compressor.compress(node.clock, this.output.length + i)
      node.keys = keys
      node.clock = clock
      const resolvedChange = this._keyCompressor.resolveKey(node.change)
      if (resolvedChange) {
        node.change = resolvedChange
      } else {
        node.keys.push(node.change)
        node.change = { seq: this.output.length + i, offset: node.keys.length }
      }
      batch.push(c.encode(NodeSchema, node))
    }

    return this.output.append(batch)
  }
}
