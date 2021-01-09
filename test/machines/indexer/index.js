const Hyperbee = require('hyperbee')

const { Op } = require('./messages')

class MachineOmega {
  constructor (opts = {}) {
    this.writable = true
    this._length = opts.length || 0
    this._byteLength = opts.byteLength || 0
    this._buf = []
  }

  get length () {
    return this._length + this._buf.length
  }

  get byteLength () {
    return this._byteLength + bufLength(this._buf)
  }

  ready (cb) {
    return cb(null)
  }

  get (idx, opts = {}, cb) {
    if (idx < this._length) hostcalls.get(idx).then(blk => onget(null, blk), err => cb(err))

    const bufIdx = idx - this._length
    if (bufIdx >= this._buf.length) return cb(new Error('Block not available'))
    return cb(null, this._buf[bufIdx])

    function onget (block) {
      if (!opts.valueEncoding) return cb(null, block)
      try {
        block = opts.valueEncoding.decode(block)
      } catch (err) {
        return cb(err)
      }
      return cb(null, block)
    }
  }

  append (blocks, cb) {
    if (Array.isArray(blocks)) this._buf.push(...blocks)
    else this._buf.push(blocks)
    return cb(null)
  }

  commit () {
    const tmp = this._buf
    this._length += tmp.length
    this._byteLength += bufLength(tmp)
    this._buf = []
    return tmp
  }

  cancel (prom) {}
}

async function index (omegaOpts, indexNode) {
  const core = new MachineOmega(omegaOpts)
  const db = new Hyperbee(core, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })

  const { type, key: dbKey, value: dbValue } = Op.decode(indexNode.node.value)

  const b = db.batch()
  switch (type) {
    case Op.Type.Put:
      await b.put(['by-writer', indexNode.key, indexNode.seq].join('!'), dbValue)
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
