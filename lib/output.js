const c = require('compact-encoding')
const b = require('b4a')

const KeyCompressor = require('./compression')
const { OutputNode } = require('./nodes')
const { Node: NodeSchema } = require('./nodes/messages')

module.exports = class Output {
  constructor (core, opts = {}) {
    this.core = core
    this.compressor = opts.compressor || new KeyCompressor(this.core)
  }

  get key () {
    return this.core.key
  }

  get length () {
    return this.core.length
  }

  get fork () {
    return this.core.fork
  }

  update () {
    return this.core.update()
  }

  truncate (length) {
    this.compressor.truncate(length)
    return this.core.truncate(length)
  }

  close () {
    return this.core.close()
  }

  snapshot () {
    return new Output(this.core.snapshot(), {
      compressor: this.compressor
    })
  }

  async get (seq, opts = {}) {
    const raw = await this.core.get(seq)
    if (!raw) return null

    const state = { start: 0, end: raw.length, buffer: raw }
    const decoded = NodeSchema.decode(state)
    if (decoded.clocks) {
      const [clocks, keys, change] = await Promise.all([
        Promise.all(decoded.clocks.map(c => this.compressor.decompress(c.clock, seq))),
        Promise.all(decoded.keys.map(k => this.compressor.resolvePointer(k))),
        this.compressor.resolvePointer(decoded.change)
      ])
      decoded.clocks = new Map()
      for (let i = 0; i < keys.length; i++) {
        decoded.clocks.set(keys[i], clocks[i])
      }
      decoded.change = change
    } else {
      const [clock, change] = await Promise.all([
        this.compressor.decompress(decoded.clock, seq),
        this.compressor.resolvePointer(decoded.change)
      ])
      decoded.change = change
      decoded.clocks = new Map([
        [b.toString(this.core.key, 'hex'), clock]
      ])
    }

    return new OutputNode(decoded)
  }

  async append (nodes) {
    const batch = []
    for (let i = 0; i < nodes.length; i++) {
      const node = nodes[i]
      const keys = []
      const clocks = []

      for (const { key, clock, operations } of node.clocks) {
        const compressed = await this.compressor.compress(clock, this.core.length + 1, { extraKeys: [key] })
        keys.push(...compressed.keys)
        clocks.push({
          key: compressed.extraKeys[0],
          clock: compressed.clock,
          operations
        })
      }
      node.keys = keys
      node.clocks = clocks

      const resolvedChange = this.compressor.resolveKey(node.change)
      if (resolvedChange) {
        node.change = resolvedChange
      } else {
        node.keys.push(node.change)
        node.change = { seq: this.core.length + i, offset: node.keys.length }
      }
      batch.push(c.encode(NodeSchema, node))
    }

    return this.core.append(batch)
  }
}
