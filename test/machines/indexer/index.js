const Hyperbee = require('hyperbee')
const MemCore = require('autobase/lib/memory-hypercore.js')

const { Op } = require('./messages')

async function index (omegaOpts, indexNode) {
  const core = new MemCore({
    ...omegaOpts,
    get: (blk) => hostcalls.get(blk)
  })
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

module.exports = {
  index
}
