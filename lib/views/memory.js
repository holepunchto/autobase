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

    this._unwrap = !!opts.unwrap
    this._includeInputNodes = opts.includeInputNodes !== false
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

  async _get (idx, opts) {
    let block = null
    if (idx < this._coreLength) {
      block = await this.core.get(idx, { ...opts, valueEncoding: null })
    } else {
      const bufIdx = idx - this._coreLength
      if (bufIdx >= this._viewBuf.length) throw new Error('Block not available')
      block = this._viewBuf[bufIdx]
    }
    if (!Buffer.isBuffer(block)) return block
    return IndexNode.decode(block)
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
    if (block.value && opts.valueEncoding) {
      block.value = opts.valueEncoding.decode(block.value)
    }
    if (this._unwrap) {
      return block.value || block.node.value
    }
    return block
  }

  async append (blocks, opts = {}) {
    const encoding = opts.valueEncoding
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

  // Wrap blocks that were appended through the `apply` option with IndexNodes.
  wrap (indexNode, until) {
    const end = until - this._coreLength
    for (let i = this._viewBuf.length - 1; i >= end; i--) {
      this._viewBuf[i] = IndexNode.encode({ ...indexNode, value: this._viewBuf[i] })
    }
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
}

function noop () {}
