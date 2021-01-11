module.exports = class MemoryCore {
  constructor (core = {}) {
    this.core = core
    this.writable = true
    this._length = core.length || 0
    this._byteLength = core.byteLength || 0
    this._buf = []

    this[Symbol.for('hypercore.promises')] = true
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
      blk = await this.core.get(idx)
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

  update () {}
  cancel () {}
  registerExtension () {}
}

function bufLength (buf) {
  return buf.reduce((acc, b) => acc + b.length, 0)
}
