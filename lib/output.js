const c = require('compact-encoding')
const b = require('b4a')

const KeyCompressor = require('./compression')
const { OutputNode } = require('./nodes')
const { Node: NodeSchema } = require('./nodes/messages')

module.exports = class Output {
  constructor (id, core, opts = {}) {
    this.id = id
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
    return new Output(this.id, this.core.snapshot(), {
      compressor: this.compressor
    })
  }

  async get (seq, opts = {}) {
    const raw = await this.core.get(seq)
    if (!raw) return null

    const state = { start: 0, end: raw.length, buffer: raw }
    const decoded = NodeSchema.decode(state)
    if (decoded.clocks) {
      const [clocks, change] = await Promise.all([
        Promise.all(decoded.clocks.map(c => this.compressor.decompress(c.clock, seq))),
        this.compressor.resolvePointer(decoded.change)
      ])
      const fullClocks = new Array(decoded.clocks.length)
      for (let id = 0; id < decoded.clocks.length; id++) {
        fullClocks[id] = { ...decoded.clocks[id], clock: clocks[id] }
      }
      decoded.change = change
      decoded.clocks = fullClocks
    } else {
      const [clock, change] = await Promise.all([
        this.compressor.decompress(decoded.clock, seq),
        this.compressor.resolvePointer(decoded.change)
      ])
      decoded.change = change
      decoded.clocks = [{ clock, operations: decoded.operations }]
    }

    return new OutputNode(decoded, this.id)
  }

  async append (nodes) {
    const batch = []
    for (let i = 0; i < nodes.length; i++) {
      const node = { ...nodes[i] }
      const keys = []
      const clocks = []

      if (node.clocks && node.clocks.length) {
        for (let id = 0; id < node.clocks.length; id++) {
          const clock = node.clocks[id]
          if (!clock) continue
          const compressed = await this.compressor.compress(clock.clock, this.core.length + i, { keys })
          clocks.push({
            id,
            clock: compressed.clock,
            operations: clock.operations
          })
        }
      }
      node.keys = keys
      node.clocks = clocks

      const resolvedChange = this.compressor.resolveKey(node.change)
      if (resolvedChange) {
        node.change = resolvedChange
      } else {
        node.keys.push(node.change)
        node.change = { seq: this.core.length + i, offset: node.keys.length - 1 }
      }
      batch.push(c.encode(NodeSchema, node))
    }

    return this.core.append(batch)
  }
}
