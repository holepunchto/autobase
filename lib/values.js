const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const b4a = require('b4a')

const { OPLOG_VERSION } = require('./caps.js')
const { OplogMessage } = require('./messages.js')
const { EncryptionView } = require('./encryption.js')

module.exports = {
  encodeValue,
  decodeValue
}

function encodeValue(value, opts = {}) {
  const state = { start: 0, end: 0, buffer: null }

  const message = {
    version: opts.version || OPLOG_VERSION,
    digest: null,
    checkpoint: null,
    optimistic: !!opts.optimistic,
    node: {
      heads: opts.heads || [],
      batch: 1,
      value
    }
  }

  OplogMessage.preencode(state, message)

  if (opts.padding) {
    state.start = opts.padding
    state.end += opts.padding
  }

  state.buffer = b4a.alloc(state.end)

  OplogMessage.encode(state, message)

  if (!opts.encrypted) return state.buffer

  if (!opts.optimistic) {
    throw new Error('Encoding an encrypted value is not supported')
  }

  const padding = b4a.alloc(16) // min hash length is 16
  crypto.hash(state.buffer, padding)
  padding[0] = 0

  return b4a.concat([padding.subarray(0, 8), state.buffer])
}

async function decodeValue(value, { autobase, encryptionKey, key, manifest, index = 0 } = {}) {
  if (encryptionKey) {
    const e = new EncryptionView({ encryptionKey, bootstrap: autobase })
    const w = e.getWriterEncryption()

    await w.decrypt(index, value, { key, manifest })
    value = value.subarray(8)
  }

  const op = c.decode(OplogMessage, value)
  return op.node.value
}
