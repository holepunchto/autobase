const Hypercore = require('hypercore')

const coreLength = Symbol('length')
const coreByteLength = Symbol('byte-length')
const viewBuf = Symbol('buf')
const viewOpening = Symbol('view-opening')
const viewOpened = Symbol('view-opened')

module.exports = class MemoryView extends Hypercore {
  constructor (...args) {
    super(...args)
    this[coreLength] = 0
    this[coreByteLength] = 0
    this[viewBuf] = []
    this[viewOpening] = this.ready()
    this[viewOpened] = false
  }

  async ready () {
    if (this[viewOpening]) return this[viewOpening]
    await super.ready()
    this.reset()
    this[viewOpened] = true
  }

  get length () {
    return this[coreLength] + this[viewBuf].length
  }

  get byteLength () {
    return this[coreByteLength] + bufLength(this[viewBuf])
  }

  get changes () {
    return this[viewBuf]
  }

  reset () {
    this[viewBuf] = []
    this[coreLength] = super.length
    this[coreByteLength] = super.byteLength
  }

  async get (idx, opts = {}) {
    if (!this[viewOpened]) await this[viewOpening]
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
    if (!this[viewOpened]) await this[viewOpening]
    if (Array.isArray(blocks)) this[viewBuf].push(...blocks)
    else this[viewBuf].push(blocks)
  }

  async truncate (length) {
    if (!this[viewOpened]) await this[viewOpening]
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
