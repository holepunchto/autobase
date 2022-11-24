const { EventEmitter } = require('events')

module.exports = class LinearizedCore extends EventEmitter {
  constructor (core, indexedLength) {
    super()

    this.core = core
    this.nodes = []
    this.indexedLength = indexedLength
    this.fork = 0
  }

  get length () {
    return this.indexed + this.nodes.length
  }

  async update () {
    return true
  }

  async get (seq) {
    if (seq > this.length || seq < 0) throw new Error('Out of bounds get')
    return seq < this.indexed ? this.core.get(seq) : this.nodes[seq - this.indexed]
  }

  truncate (len) {
    const oldLength = this.length
    if (len < this.indexed) throw new Error('Cannot truncate less than the index length')
    len -= this.indexed
    while (this.nodes.length > len) this.nodes.pop()
    this.emit('truncate', oldLength, 0)
  }

  async append (buf) {
    this.nodes.push(buf)
    this.emit('append')
    return { length: this.length }
  }
}
