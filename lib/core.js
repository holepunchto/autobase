const ReadyResource = require('ready-resource')
const c = require('compact-encoding')

module.exports = class LinearizedCore extends ReadyResource {
  constructor (base, core, name, valueEncoding) {
    super()

    this.base = base
    this.core = core
    this.name = name
    this.tip = []
    this.valueEncoding = valueEncoding

    this.indexedLength = 0
    this.fork = 0

    // maintained by base
    this.appending = 0
    this.truncating = 0
    this.indexing = 0

    this.ready().catch(noop)
  }

  async _open () {
    const index = await this.base.system.getIndex(this.name)
    this.indexedLength = index ? index.length : 0
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
    if (this.opened === false) await this.ready()
    if (seq > this.length || seq < 0) throw new Error('Out of bounds get')

    return seq < this.indexedLength
      ? this.core.get(seq, { valueEncoding: this.valueEncoding })
      : c.decode(this.valueEncoding, this.tip[seq - this.indexedLength])
  }

  async truncate (len) {
    throw new Error('Cannot truncate a linearized core')
  }

  async append (buffers) {
    if (this.opened === false) await this.ready()
    if (!Array.isArray(buffers)) buffers = [buffers]

    for (let i = 0; i < buffers.length; i++) {
      const buf = c.encode(this.valueEncoding, buffers[i])
      this.tip.push(buf)
    }

    this.base._onuserappend(this, buffers.length)
    this.emit('append', this.length)

    return { length: this.length }
  }
}

function noop () {}
