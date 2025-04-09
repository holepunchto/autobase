const sodium = require('sodium-universal')
const LegacyEncryption = require('hypercore/lib/default-encryption')
const c = require('compact-encoding')
const b4a = require('b4a')

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)

class WriterEncryption {
  static padding = 16

  constructor (base, opts) {
    this.base = base

    this.classic = false

    this.blockKey = null
    this.blindingKey = null
  }

  get paddingBytes () {
    return this.classic ? LegacyEncryption.padding : WriterEncryption.padding
  }

  padding (context) {
    return context.manifest.version <= 1 ? LegacyEncryption.padding : WriterEncryption.padding
  }

  load (context) {
    const keys = LegacyEncryption.deriveKeys(this.base.encryptionKey, context.key, false, false)

    this.blockKey = keys.blockKey
    this.blindingKey = keys.blindingKey

    this.classic = context.manifest.version <= 1
  }

  encrypt (index, block, fork, context) {
    if (!this.blockKey) this.load(context)

    if (this.classic) {
      return LegacyEncryption.encrypt(index, block, fork, this.blockKey, this.blindingKey)
    }

    return WriterEncryption.encrypt(index, block, fork, this.blockKey, this.blindingKey)
  }

  decrypt (index, block, context) {
    if (!this.blockKey) this.load(context)

    if (this.classic) {
      return LegacyEncryption.decrypt(index, block, this.blockKey)
    }

    return WriterEncryption.decrypt(index, block, this.blockKey)
  }

  static encrypt (index, block, fork, blockKey, blindingKey) {
    const padding = block.subarray(0, WriterEncryption.paddingBytes)
    block = block.subarray(WriterEncryption.padding)

    // Unkeyed hash of block as we blind it later
    sodium.crypto_generichash(padding, block)

    // Encode padding
    c.uint64.encode({ start: 8, end: 16, buffer: padding }, fork)

    setNonce(index)

    // Blind key id, fork id and block hash
    encrypt(padding, nonce, blindingKey)

    padding[0] = 1 // encrypted

    nonce.set(padding, 8)

    // The combination of index, key id, fork id and block hash is very likely
    // to be unique for a given Hypercore and therefore our nonce is suitable
    encrypt(block, nonce, blockKey)
  }

  static decrypt (index, block, key) {
    const padding = block.subarray(0, WriterEncryption.padding)
    block = block.subarray(WriterEncryption.padding)

    if (padding[0] === 0) return block // unencrypted

    setNonce(index)

    nonce.set(padding, 8)

    // Decrypt the block using the full nonce
    decrypt(block, nonce, key)
  }
}

module.exports = {
  WriterEncryption
}

function setNonce (index) {
  c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

  // Zero out any previous padding.
  nonce.fill(0, 8)
}

function encrypt (block, nonce, key) {
  sodium.crypto_stream_xor(
    block,
    block,
    nonce,
    key
  )
}

function decrypt (block, nonce, key) {
  return encrypt(block, nonce, key) // symmetric
}
