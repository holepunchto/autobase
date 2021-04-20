const { EventEmitter } = require('events') module.exports = class StableIndexView extends EventEmitter {
  constructor (core, opts = {}) {
    super()
    this.core = core
    this._viewBuf = []
    this._coreLength = this.core.length
    this._onupdate = opts.onupdate || noop
  }

  get writable () {
    return this.core.writable
  }

  get length () {
    return this._coreLength + this._viewBuf.length
  }

  get byteLength () {
    // TODO: Byte length calculations will always be incorrect due to IndexNode wrapping.
    return 0
  }

  async update (...args) {
    await this._onupdate()
    return this.core.update(...args)
  }

  async get (idx, opts = {}) {
    if (idx < this._coreLength) return this.core.get(idx, opts)
    const bufIdx = idx - this._coreLength
    if (bufIdx >= this._viewBuf.length) throw new Error('Block not available')
    const blk = this._viewBuf[bufIdx]
    const encoding = opts.valueEncoding || this.valueEncoding
    return encoding ? encoding.decode(blk) : blk
  }

  async append (blocks, opts = {}) {
    const encoding = opts.valueEncoding || this.valueEncoding
    if (Array.isArray(blocks)) {
      this._viewBuf.push(...(encoding ? blocks.map(encoding.encode) : blocks))
    } else {
      this._viewBuf.push(encoding ? encoding.encode(blocks) : blocks)
    }
    this.emit('append')
  }

  async truncate (length) {
    if (length >= this._coreLength) {
      this._viewBuf = this._viewBuf.slice(0, length - this._coreLength)
    } else {
      this._viewBuf = []
      this._coreLength = length
    }
    this.emit('truncate')
  }

  // TODO: The truncate/append here should be atomic.
  async commit (opts = {}) {
    if (this.writable) {
      if (this._coreLength < this.core.length) {
        await this.core.truncate(this._coreLength)
      }
      await this.core.append(this._viewBuf)
    }
    this._viewBuf = []
    this._coreLength = this.core.length
  }

  static async from (core, opts = {}) {
    if (core) await core.ready()
    return new this(core, opts)
  }
}

function noop () { }
