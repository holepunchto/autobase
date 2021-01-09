const sodium = require('sodium-native')

module.exports = class XORNative {
  constructor (nonce, key) {
    this.handle = Buffer.alloc(sodium.crypto_stream_xor_STATEBYTES)
    this.nonce = nonce
    this.key = key
    sodium.crypto_stream_xor_init(this.handle, this.nonce, this.key)
  }

  update (out, message) {
    sodium.crypto_stream_xor_update(this.handle, out, message)
  }

  final () {
    sodium.crypto_stream_xor_final(this.handle)
  }
}
