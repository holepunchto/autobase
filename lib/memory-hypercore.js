const promises = Symbol.for('hypercore.promises')

module.exports = class MemoryCore {
  constructor (core = {}) {
    this[promises] = true
    this.core = core
    this.writable = true
    this.reset(core)
  }

  get peers () {
    return this.core.peers
  }

  get length () {
    return this._length + this._buf.length
  }

  get byteLength () {
    return this._byteLength + bufLength(this._buf)
  }

  ready () {
    return null
  }

  async get (idx, opts = {}) {
    let blk = null
    if (idx < this._length) {
      blk = await this.core.get(idx, opts)
    } else {
      const bufIdx = idx - this._length
      if (bufIdx >= this._buf.length) throw new Error('Block not available')
      blk = this._buf[bufIdx]
    }
    if (opts.valueEncoding) return opts.valueEncoding.decode(blk)
    return blk
  }

  append (blocks) {
    if (Array.isArray(blocks)) this._buf.push(...blocks)
    else this._buf.push(blocks)
  }

  commit () {
    const tmp = this._buf
    this._length += tmp.length
    this._byteLength += bufLength(tmp)
    this._buf = []
    return tmp
  }

  reset (opts = {}) {
    this._length = opts.length || 0
    this._byteLength = opts.byteLength || 0
    this._buf = []
  }

  truncate (length) {
    if (length >= this._length) {
      const bl = bufLength(this._buf)
      this._buf = this._buf.slice(0, length - this._length)
      this._byteLength -= (bl - bufLength(this._buf))
    } else {
      this._byteLength -= bufLength(this._buf)
      this._buf = []
    }
    this._length = length
  }

  registerExtension (name, handlers) {
    if (this.core.registerExtension) return this.core.registerExtension(name, handlers)
  }

  // TODO: Implement
  update () {}
  cancel () {}
}

function bufLength (buf) {
  return buf.reduce((acc, b) => acc + b.length, 0)
}
