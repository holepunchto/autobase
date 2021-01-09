const Hyperbee = require('hyperbee')

const { Op } = require('./messages')

class MachineOmega {
  constructor (opts = {}) {
    this.writable = true
    this._length = opts.length || 0
    this._byteLength = opts.byteLength || 0
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
      blk = await hostcalls.get(idx)
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

async function index (omegaOpts, indexNode) {
  const core = new MachineOmega(omegaOpts)
  const db = new Hyperbee(core, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })

  const val = indexNode.node.value
  const buf = Buffer.from(val.buffer, val.byteOffset, val.byteLength)

  const { type, key: dbKey, value: dbValue } = Op.decode(buf)

  const b = db.batch()
  switch (type) {
    case Op.Type.Put:
      await b.put(['by-writer', indexNode.node.key, indexNode.node.seq].join('!'), dbValue)
      await b.put(['by-key', dbKey].join('!'), dbValue)
      break
    case Op.Type.Del:
      await b.del(['by-key', dbKey].join('!'), dbValue)
      break
    default:
      // Gracefully ignore unsupported op types.
      break
  }

  await b.flush()

  return core.commit()
}

function bufLength (buf) {
  return buf.reduce((acc, b) => acc + b.length, 0)
}

module.exports = {
  index
}
