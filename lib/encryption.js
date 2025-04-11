const sodium = require('sodium-universal')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const b4a = require('b4a')

const HypercoreEncryption = Hypercore.DefaultEncryption

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)
const hash = nonce.subarray(0, sodium.crypto_generichash_BYTES_MIN)

const [NS_HASH_KEY] = crypto.namespace('autobase/encryption', 1)

class WriterEncryption {
  static PADDING = 8

  constructor (base, opts) {
    this.base = base

    this.classic = false
    this.keys = null
    this.hashKey = null
  }

  padding (context) {
    return context.manifest.version <= 1 ? HypercoreEncryption.PADDING : WriterEncryption.PADDING
  }

  _load (context) {
    this.keys = HypercoreEncryption.deriveKeys(this.base.encryptionKey, context.key)
    this.classic = context.manifest.version <= 1

    if (!this.classic) this.hashKey = crypto.hash([NS_HASH_KEY, this.keys.block])
  }

  // Padding is blockHash and key id
  _encodePadding (padding, block, keyId) {
    sodium.crypto_generichash(hash, block, this.hashKey)
    padding.set(hash.subarray(0, 8)) // copy first 8 bytes of hash

    c.uint64.encode({ start: 4, end: 8, buffer: padding }, keyId)
    hash.fill(0) // clear nonce buffer
  }

  encrypt (index, block, fork, context) {
    if (!this.keys) this._load(context)

    if (this.classic) {
      return HypercoreEncryption.encrypt(index, block, fork, this.keys.block, this.keys.blinding)
    }

    const padding = block.subarray(0, WriterEncryption.PADDING)
    block = block.subarray(WriterEncryption.PADDING)

    this._encodePadding(padding, block, 0)

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    // Blind block hash and key id
    encrypt(padding, nonce, this.keys.blinding)

    padding[0] = 1 // version in plaintext

    nonce.set(padding, 8, 16)

    // The combination of index, key id, fork id and block hash is very likely
    // to be unique for a given Hypercore and therefore our nonce is suitable
    encrypt(block, nonce, this.keys.block)
  }

  decrypt (index, block, context) {
    if (!this.keys) this._load(context)

    if (this.classic) {
      return HypercoreEncryption.decrypt(index, block, this.keys.block)
    }

    const padding = block.subarray(0, WriterEncryption.PADDING)
    block = block.subarray(WriterEncryption.PADDING)

    if (padding[0] === 0) return block // unencrypted

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.set(padding, 8, 16)

    // Decrypt the block using the full nonce
    decrypt(block, nonce, this.keys.block)
  }
}

module.exports = {
  WriterEncryption
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
