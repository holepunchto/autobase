const { EventEmitter } = require('events')
const { IndexNode } = require('../nodes')

const promises = Symbol.for('hypercore.promises')
const setCoreSymbol = Symbol('set-core')

class ExtensionProxy {
  constructor (name, handlers) {
    this.name = name
    this.handlers = handlers
    this.core = extensions.core
    this.extensions = extensions
    this.name = name
    this.encoding = handlers.encoding
    this.onmessage = handlers.onmessage || noop
    this.onremotesupports = handlers.onremotesupports || noop
  }

  [setCoreSymbol] (core) {
    this.core = core
    this.ext = this.core.registerExtension(this.name, this.handlers)
  }

  send (message, peer) {
    const ext = peer.extensions.get(this.name)
    if (ext) ext.send(message)
  }

  broadcast (message) {
    if (this.core.replicator === null) return
    for (const peer of this.core.replicator.peers) this.send(message, peer)
  }

  destroy () {
    this.extensions.all.delete(this.name)
    for (const peer of this.core.replicator.peers) {
      const ext = peer.extensions.get(this.name)
      if (ext) ext.destroy()
    }
  }
}

module.exports = class MemoryView extends EventEmitter {
  constructor (base, core, opts = {}) {
    super()
    this[promises] = true
    this.base = base
    this.core = core
    this.ready = () => this._ready()

    this._asyncOpen = opts.asyncOpen
    this._unwrap = !!opts.unwrap
    this._includeInputNodes = opts.includeInputNodes !== false
    this._extensions = null

    this._viewBuf = []
    this._coreLength = this.core ? this.core.length : 0
    this._onupdate = opts.onupdate || noop
  }

  // Hypercore API

  async _ready () {
    if (this._asyncOpen) {
      const { base, core } = await this._asyncOpen()
      if (base) this.base = base
      if (core) {
        this.core = core
        this._coreLength = this.core.length
      }
      this._asyncOpen = null
    }
    if (!this.core) throw new Error('MemoryView did not initialize correctly')
    return this.core.ready()
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
    if (this._asyncOpen) await this.ready()
    await this._onupdate()
    return this.core.update(...args)
  }

  async _get (idx, opts) {
    if (this._asyncOpen) await this.ready()
    let block = null
    if (idx < this._coreLength) {
      block = await this.core.get(idx, { ...opts, valueEncoding: null })
    } else {
      const bufIdx = idx - this._coreLength
      if (bufIdx >= this._viewBuf.length) throw new Error('Block not available')
      block = this._viewBuf[bufIdx]
    }
    console.log('block here:', block)
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
    if (this._asyncOpen) await this.ready()
    const encoding = opts.valueEncoding
    if (Array.isArray(blocks)) {
      this._viewBuf.push(...(encoding ? blocks.map(encoding.encode) : blocks))
    } else {
      this._viewBuf.push(encoding ? encoding.encode(blocks) : blocks)
    }
    this.emit('append')
  }

  async truncate (length) {
    if (this._asyncOpen) await this.ready()
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
      this._asyncOpen.then(({ core }) => {
        for (const ext of this._extensions) {
          ext.core = core
        }
      }, noop)
    }

    const extensionProxy = new Proxy({
      get (target, handlers) {

      }
    })
    this._extensions.push(extensionProxy)

    return extensionProxy
  }

  // MemoryView API

  get indexLength () {
    return this._coreLength
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
