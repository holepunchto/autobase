/* eslint-disable camelcase */
const { sodium_malloc, sodium_memzero } = require('sodium-universal/memory')
const {
  crypto_aead_chacha20poly1305_ietf_KEYBYTES,
  crypto_aead_chacha20poly1305_ietf_NPUBBYTES,
  crypto_aead_chacha20poly1305_ietf_ABYTES,
  crypto_aead_chacha20poly1305_ietf_encrypt,
  crypto_aead_chacha20poly1305_ietf_decrypt
} = require('sodium-universal/crypto_aead')

var assert = require('nanoassert')

var KEYLEN = 32
var NONCELEN = 8
var MACLEN = 16

assert(crypto_aead_chacha20poly1305_ietf_KEYBYTES === KEYLEN)
// 16 bytes are cut off in the following functions
assert(crypto_aead_chacha20poly1305_ietf_NPUBBYTES === 4 + NONCELEN)
assert(crypto_aead_chacha20poly1305_ietf_ABYTES === MACLEN)

module.exports = {
  KEYLEN,
  NONCELEN,
  MACLEN,
  encrypt,
  decrypt,
  rekey
}

var ElongatedNonce = sodium_malloc(crypto_aead_chacha20poly1305_ietf_NPUBBYTES)
function encrypt (out, k, n, ad, plaintext) {
  assert(out.byteLength >= plaintext.byteLength + MACLEN, 'output buffer must be at least plaintext plus MACLEN bytes long')
  assert(k.byteLength === KEYLEN)
  assert(n.byteLength === NONCELEN)
  assert(ad == null ? true : ad.byteLength != null)
  sodium_memzero(ElongatedNonce)

  ElongatedNonce.set(n, 4)

  encrypt.bytesWritten = crypto_aead_chacha20poly1305_ietf_encrypt(out.subarray(0, plaintext.byteLength + MACLEN), plaintext, ad, null, ElongatedNonce, k)
  encrypt.bytesRead = encrypt.bytesWritten - MACLEN

  sodium_memzero(ElongatedNonce)
}
encrypt.bytesWritten = 0
encrypt.bytesRead = 0

function decrypt (out, k, n, ad, ciphertext) {
  assert(out.byteLength >= ciphertext.byteLength - MACLEN)
  assert(k.byteLength === KEYLEN)
  assert(n.byteLength === NONCELEN)
  assert(ad == null ? true : ad.byteLength != null)
  sodium_memzero(ElongatedNonce)

  ElongatedNonce.set(n, 4)

  decrypt.bytesWritten = crypto_aead_chacha20poly1305_ietf_decrypt(out.subarray(0, ciphertext.byteLength - MACLEN), null, ciphertext, ad, ElongatedNonce, k)
  decrypt.bytesRead = decrypt.bytesWritten + MACLEN

  sodium_memzero(ElongatedNonce)
}
decrypt.bytesWritten = 0
decrypt.bytesRead = 0

var maxnonce = new Uint8Array(8).fill(0xff)
var zerolen = new Uint8Array(0)
var zeros = new Uint8Array(32)

var IntermediateKey = sodium_malloc(KEYLEN + MACLEN)
sodium_memzero(IntermediateKey)
function rekey (out, k) {
  assert(out.byteLength === KEYLEN)
  assert(k.byteLength === KEYLEN)
  sodium_memzero(IntermediateKey)

  IntermediateKey.set(k)
  encrypt(IntermediateKey, k, maxnonce, zerolen, zeros)
  rekey.bytesWritten = encrypt.bytesWritten
  rekey.bytesRead = encrypt.bytesRead
  out.set(IntermediateKey.subarray(0, KEYLEN))
  sodium_memzero(IntermediateKey)
}
rekey.bytesWritten = 0
rekey.bytesRead = 0
