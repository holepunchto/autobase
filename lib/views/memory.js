const { EventEmitter } = require('events')
const { IndexNode } = require('../nodes')

const promises = Symbol.for('hypercore.promises')

module.exports = class MemoryView extends EventEmitter {
  constructor (base, core, opts = {}) {
    super()
    this[promises] = true
    this.base = base
    this.core = core
    this.ready = this.core.ready.bind(this.core)
    this.valueEncoding = opts.valueEncoding || IndexNode

    this._readonly = !!opts.readonly
    this._unwrap = !!opts.unwrap
    this._includeInputNodes = opts.includeInputNodes !== false
    this._viewBuf = []
    this._coreLength = this.core.length
    this._onupdate = opts.onupdate || noop
  }

  get writable () {
    return !this._readonly && this.core.writable
  }

  get length () {
    return this._coreLength + this._viewBuf.length
  }

  get byteLength () {
    // TODO: Byte length calculations will always be incorrect due to IndexNode wrapping.
    return 0
  }

  async update (...args) {
    console.log('IN UPDATE HERE')
    await this._onupdate()
    console.log('AFTER ONUPDATE')
    return this.core.update(...args)
  }

  async _get (idx, opts) {
    let block = null
    if (idx < this._coreLength) {
      block = await this.core.get(idx, opts)
    } else {
      const bufIdx = idx - this._coreLength
      if (bufIdx >= this._viewBuf.length) throw new Error('Block not available')
      block = this._viewBuf[bufIdx]
    }
    const encoding = opts.valueEncoding || this.valueEncoding
    return (encoding && Buffer.isBuffer(block)) ? encoding.decode(block) : block
  }

  async get (idx, opts = {}) {
    const block = await this._get(idx, opts)
    if (!block) return block
    if (this._includeInputNodes) {
      const inputNode = await this.base._getInputNode(block.node.id, block.node.seq)
      if (inputNode) {
        inputNode.key = block.node.key
        inputNode.seq = block.node.seq
        block.node = inputNode
      }
    }
    if (this._unwrap) {
      return block.value || block.node.value
    }
    return block
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
    if (this.writable && opts.flush !== false) {
      if (this._coreLength < this.core.length) {
        await this.core.truncate(this._coreLength)
      }
      await this.core.append(this._viewBuf)
    }
    this._viewBuf = []
    this._coreLength = this.core.length
  }

  static from (base, core, opts = {}) {
    return new this(base, core, opts)
  }
}

function noop () {}
