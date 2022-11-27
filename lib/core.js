const { EventEmitter } = require('events')

module.exports = class LinearizedCore extends EventEmitter {
  constructor (base, core, name, indexedLength) {
    super()

    this.base = base
    this.core = core
    this.name = name
    this.tip = []
    this.indexedLength = indexedLength
    this.fork = 0

    // maintained by base
    this.appending = 0
    this.truncating = 0
    this.indexing = 0
  }

  get length () {
    return this.indexedLength + this.tip.length
  }

  // triggered by base
  _onindex (added) {
    this.indexedLength += added
    this.tip = this.tip.slice(added)
  }

  // triggered by base
  _onundo (removed) {
    const oldLength = this.length
    while (removed-- > 0) this.tip.pop()
    this.emit('truncate', oldLength, 0)
  }

  async update () {
    return true
  }

  async get (seq) {
    if (seq > this.length || seq < 0) throw new Error('Out of bounds get')
    return seq < this.indexedLength ? this.core.get(seq) : this.tip[seq - this.indexedLength]
  }

  async truncate (len) {
    throw new Error('Cannot truncate a linearized core')
  }

  async append (buf) {
    this.base._onuserappend(this, 1)
    this.tip.push(buf)
    this.emit('append')
    return { length: this.length }
  }
}
