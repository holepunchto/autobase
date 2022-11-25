const { EventEmitter } = require('events')

module.exports = class LinearizedCore extends EventEmitter {
  constructor (core, indexedLength) {
    super()

    this.core = core
    this.tip = []
    this.indexedLength = indexedLength
    this.fork = 0
  }

  get length () {
    return this.indexedLength + this.tip.length
  }

  async update () {
    return true
  }

  async get (seq) {
    if (seq > this.length || seq < 0) throw new Error('Out of bounds get')
    return seq < this.indexedLength ? this.core.get(seq) : this.tip[seq - this.indexedLength]
  }

  truncate (len) {
    const oldLength = this.length
    if (len < this.indexedLength) throw new Error('Cannot truncate less than the index length')
    len -= this.indexedLength
    while (this.tip.length > len) this.tip.pop()
    this.emit('truncate', oldLength, 0)
  }

  async append (buf) {
    this.tip.push(buf)
    this.emit('append')
    return { length: this.length }
  }
}
