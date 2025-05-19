const { OplogMessage } = require('./messages.js')
const { EncryptionView } = require('./encryption.js')
const c = require('compact-encoding')

module.exports = decodeValue

async function decodeValue (value, { autobase, encryptionKey, key, manifest, index = 0 } = {}) {
  if (encryptionKey) {
    const e = new EncryptionView({ encryptionKey, bootstrap: autobase })
    const w = e.getWriterEncryption()

    await w.decrypt(index, value, { key, manifest })
    value = value.subarray(8)
  }

  const op = c.decode(OplogMessage, value)
  return op.node.value
}
