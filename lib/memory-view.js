const Omega = require('omega')

const coreLength = Symbol('@memory-hypercore/length')
const coreByteLength = Symbol('@memory-hypercore/byte-length')
const viewBuf = Symbol('@memory-hypercore/buf')
const viewOpening = Symbol('@memory-hypercore/view-opening')

module.exports = class MemoryView extends Omega {
  constructor (...args) {
    super(...args)
    this[coreLength] = 0
    this[coreByteLength] = 0
    this[viewBuf] = []
    this[viewOpening] = this.ready()
  }

  async ready () {
    if (this[viewOpening]) return this[viewOpening]
    await super.ready()
    this[coreLength] = super.length
    this[coreByteLength] = super.byteLength
    this[viewOpening] = null
  }

  get length () {
    return this[coreLength] + this[viewBuf].length
  }

  get byteLength () {
    return this[coreByteLength] + bufLength(this[viewBuf])
  }

  async get (idx, opts = {}) {
    if (this[viewOpening]) await this[viewOpening]
    let blk = null
    if (idx < this[coreLength]) {
      blk = await super.get(idx, opts)
    } else {
      const bufIdx = idx - this[coreLength]
      if (bufIdx >= this[viewBuf].length) throw new Error('Block not available')
      blk = this[viewBuf][bufIdx]
    }
    if (opts.valueEncoding) return opts.valueEncoding.decode(blk)
    return blk
  }

  async append (blocks) {
    if (this[viewOpening]) await this[viewOpening]
    if (Array.isArray(blocks)) this[viewBuf].push(...blocks)
    else this[viewBuf].push(blocks)
  }

  commit () {
    const tmp = this[viewBuf]
    this[viewBuf] = []
    if (tmp) {
      this[coreLength] += tmp.length
      this[coreByteLength] += bufLength(tmp)
    }
    return tmp
  }

  truncate (length) {
    if (length >= this[coreLength]) {
      const bl = bufLength(this[viewBuf])
      this._buf = this[viewBuf].slice(0, length - this[coreLength])
      this._byteLength -= (bl - bufLength(this[viewBuf]))
    } else {
      this[coreByteLength] -= bufLength(this[viewBuf])
      this[viewBuf] = []
    }
    this[coreLength] = length
  }

  static from (core) {
    return core.session({ class: this })
  }
}

function bufLength (buf) {
  return buf.reduce((acc, b) => acc + b.length, 0)
}
