const Hyperbee = require('hyperbee')
const MemCore = require('autobase/lib/memory-hypercore.js')

const { Op } = require('./messages')

class Reducer {
  constructor (opts = {}) {
    // Set in init.
    this.core = null
    this._peers = []
    this._extensions = new Map()

    this._state = null
    this._init = opts.init
    this._reduce = opts.reduce
  }

  async init (opts = {}) {
    this.core = new MemCore({
      ...opts,
      peers: this._peers,
      get: blk => hostcalls.get(blk),
      registerExtension: this._registerExtension.bind(this)
    })
    if (!this._init) return
    this._state = await this._init(this.core)
  }

  async reduce (indexState, indexNode) {
    this.core.reset(indexState)
    if (!this._reduce) return []
    await this._reduce(this._state, indexNode)
    return this.core.commit()
  }

  // TODO: What's the right way to do error handling in these extension methods?
  _registerExtension (name, handlers) {
    if (this._extensions.has(name)) throw new Error('An extension with that name already exists.')
    hostcalls.registerExtension(name).catch(() => {})
    const ext = {
      ...handlers,
      send: (msg, peer) => hostcalls.sendExtension(name, msg, peer).catch(() => {}),
      destroy: () => {
        hostcalls.destroyExtension(name).catch(() => {})
        this._extensions.delete(name)
      }
    }
    this._extensions.set(name, ext)
    return ext
  }

  onextension (name, message, peer) {
    message = Buffer.from(message.buffer, message.byteOffset, message.byteLength)
    const ext = this._extensions.get(name)
    if (!ext || !ext.onmessage) return
    ext.onmessage(message, peer)
  }

  onpeeradd (id) {
    const idx = this._peers.indexOf(id)
    if (idx !== -1) throw new Error('A peer with that ID already exists')
    this._peers.push(id)
  }

  onpeerremove (id) {
    const idx = this._peers.indexOf(id)
    if (idx === -1) return
    this._peers.splice(idx, 1)
  }

  static create (opts = {}) {
    const reducer = new this(opts)
    return {
      init: reducer.init.bind(reducer),
      reduce: reducer.reduce.bind(reducer),
      onextension: reducer.onextension.bind(reducer),
      onpeeradd: reducer.onpeeradd.bind(reducer),
      onpeerremove: reducer.onpeerremove.bind(reducer)
    }
  }
}

async function init (core) {
  return new Hyperbee(core, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
}

async function reduce (db, { node }) {
  const val = node.value
  const buf = Buffer.from(val.buffer, val.byteOffset, val.byteLength)
  const { type, key: dbKey, value: dbValue } = Op.decode(buf)

  const b = db.batch()
  switch (type) {
    case Op.Type.Put:
      await b.put(['by-writer', node.key, node.seq].join('!'), dbValue)
      await b.put(['by-key', dbKey].join('!'), dbValue)
      break
    case Op.Type.Del:
      await b.del(['by-key', dbKey].join('!'), dbValue)
      break
    default:
      // Gracefully ignore unsupported op types.
      break
  }
  return b.flush()
}

module.exports = Reducer.create({ init, reduce })
