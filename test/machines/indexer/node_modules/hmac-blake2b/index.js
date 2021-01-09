/* eslint-disable camelcase */
var { sodium_malloc, sodium_memzero } = require('sodium-universal/memory')
var { crypto_generichash, crypto_generichash_batch } = require('sodium-universal/crypto_generichash')
var assert = require('nanoassert')

var HASHLEN = 64
var BLOCKLEN = 128
var scratch = sodium_malloc(BLOCKLEN * 3)
var HMACKey = scratch.subarray(BLOCKLEN * 0, BLOCKLEN * 1)
var OuterKeyPad = scratch.subarray(BLOCKLEN * 1, BLOCKLEN * 2)
var InnerKeyPad = scratch.subarray(BLOCKLEN * 2, BLOCKLEN * 3)

// Post-fill is done in the cases where someone caught an exception that
// happened before we were able to clear data at the end
module.exports = function hmac (out, data, key) {
  assert(out.byteLength === HASHLEN)
  assert(key.byteLength != null)
  assert(Array.isArray(data) ? data.every(d => d.byteLength != null) : data.byteLength != null)

  if (key.byteLength > BLOCKLEN) {
    crypto_generichash(HMACKey.subarray(0, HASHLEN), key)
    sodium_memzero(HMACKey.subarray(HASHLEN))
  } else {
    // Covers key <= BLOCKLEN
    HMACKey.set(key)
    sodium_memzero(HMACKey.subarray(key.byteLength))
  }

  for (var i = 0; i < HMACKey.byteLength; i++) {
    OuterKeyPad[i] = 0x5c ^ HMACKey[i]
    InnerKeyPad[i] = 0x36 ^ HMACKey[i]
  }
  sodium_memzero(HMACKey)

  crypto_generichash_batch(out, [InnerKeyPad].concat(data))
  sodium_memzero(InnerKeyPad)
  crypto_generichash_batch(out, [OuterKeyPad].concat(out))
  sodium_memzero(OuterKeyPad)
}

module.exports.BYTES = HASHLEN
module.exports.KEYBYTES = BLOCKLEN
