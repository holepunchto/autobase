const { EventEmitter } = require('events')
const { IndexNode } = require('../nodes')

const promises = Symbol.for('hypercore.promises')
const coreSymbol = Symbol('core')
const extSymbol = Symbol('ext')
const setCoreSymbol = Symbol('set-core')

class ExtensionProxy {
  constructor (name, handlers) {
    this.name = name
    this.handlers = handlers
    this[coreSymbol] = null
    this[extSymbol] = null
  }

  get encoding () {
    return this[extSymbol] && this[extSymbol].encoding
  }

  get onmessage () {
    return this[extSymbol] && this[extSymbol].onmessage
  }

  get onremotesupports () {
    return this[extSymbol] && this[extSymbol].onremotesupports
  }

  [setCoreSymbol] (core) {
    this[coreSymbol] = core
    this[extSymbol] = this[coreSymbol].registerExtension(this.name, this.handlers)
  }

  send (message, peer) {
    if (!this[extSymbol]) return
    return this[extSymbol].send(message, peer)
  }

  broadcast (message) {
    if (!this[extSymbol]) return
    return this[extSymbol].broadcast(message)
  }

  destroy () {
    if (!this[extSymbol]) return
    return this[extSymbol].destroy()
  }
}

module.exports = class MemoryView extends EventEmitter {
  constructor (base, core, opts = {}) {
    super()
    this[promises] = true
    this.base = null
    this.core = null

    this._unwrap = null
    this._includeInputNodes = null
    this._coreLength = null
    this._onupdate = null

    this._extensions = []
    this._viewBuf = []

    if (opts.open) {
      this._opening = this._open(opts.open)
      this._opening.catch(noop)
    } else {
      this._opening = null
      this._init(base, core, opts)
    }

    this._id = Math.random()

    this.ready = () => this._opening
  }

  _init (base, core, opts) {
    this.base = base
    this.core = core

    this._unwrap = !!opts.unwrap
    this._includeInputNodes = opts.includeInputNodes !== false
    this._coreLength = this.core ? this.core.length : 0
    this._onupdate = opts.onupdate || noop
  }

  // Hypercore API

  async _open (open) {
    console.log('BEFORE ASYNC OPEN')
    const { base, core, opts } = await open()
    console.log('AFTER ASYNC OPEN')
    this._init(base, core, opts)
    this._opening = null
  }

  get writable () {
    return this.core && this.core.writable
  }

  get length () {
    return this.core ? this._coreLength + this._viewBuf.length : 0
  }

  get byteLength () {
    // TODO: Byte length calculations will always be incorrect due to IndexNode wrapping.
    return 0
  }

  async update (...args) {
    console.log('in update here')
    if (this._opening) await this._opening
    console.log('update 2')
    await this._onupdate()
    console.log('update 3')
    return this.core.update(...args)
  }

  async _get (idx, opts) {
    console.log('in _get here, idx:', idx)
    if (this._opening) await this._opening
    let block = null
    if (idx < this._coreLength) {
      console.log('less than core length')
      block = await this.core.get(idx, { ...opts, valueEncoding: null })
    } else {
      console.log('viewBuf here:', this._viewBuf)
      const bufIdx = idx - this._coreLength
      if (bufIdx >= this._viewBuf.length) throw new Error('Block not available')
      block = this._viewBuf[bufIdx]
    }
    console.log('block here:', block, 'string:')
    if (!Buffer.isBuffer(block)) return block
    return IndexNode.decode(block)
  }

  async get (idx, opts = {}) {
    console.log('in get here')
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
    console.log('appending blocks:', blocks, 'into:', this)
    if (this._opening) await this._opening
    const encoding = opts.valueEncoding
    if (Array.isArray(blocks)) {
      this._viewBuf.push(...(encoding ? blocks.map(encoding.encode) : blocks))
    } else {
      this._viewBuf.push(encoding ? encoding.encode(blocks) : blocks)
    }
    this.emit('append')
  }

  async truncate (length) {
    if (this._opening) await this._opening
    if (length >= this._coreLength) {
      this._viewBuf = this._viewBuf.slice(0, length - this._coreLength)
    } else {
      this._viewBuf = []
      this._coreLength = length
    }
    this.emit('truncate')
  }

  registerExtension (name, handlers) {
    if (this.core) return this.core.registerExtension(name, handlers)

    if (!this._extensions) {
      this._extensions = []
      const ready = this._opening ? this._opening : Promise.resolve()
      ready.then(() => {
        for (const ext of this._extensions) {
          ext[setCoreSymbol](this.core)
        }
        this._extensions = null
      }, noop)
    }

    const proxy = new ExtensionProxy(name, handlers)
    this._extensions.push(proxy)

    return proxy
  }

  // MemoryView API

  get indexLength () {
    return this._coreLength
  }

  // Wrap blocks that were appended through the `apply` option with IndexNodes.
  wrap (indexNode, until) {
    console.log('wrapping buf:', this._viewBuf)
    const end = until - this._coreLength
    for (let i = this._viewBuf.length - 1; i >= end; i--) {
      this._viewBuf[i] = IndexNode.encode({ ...indexNode, value: this._viewBuf[i] })
    }
  }

  // TODO: The truncate/append here should be atomic.
  async commit (opts = {}) {
    if (this._opening) await this._opening
    if (this.writable && opts.flush !== false) {
      if (this._coreLength < this.core.length) {
        await this.core.truncate(this._coreLength)
      }
      await this.core.append(this._viewBuf)
    }
    this._viewBuf = []
    this._coreLength = this.core.length
  }

  static async (open) {
    return new this(null, null, { open })
  }
}

function noop (err) {
  console.log('NOOP ERR:', err)
}
