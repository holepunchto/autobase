/* eslint-disable camelcase */
const { sodium_memzero } = require('sodium-universal/memory')
const { sodium_increment, sodium_memcmp, sodium_is_zero } = require('sodium-universal/helpers')

var assert = require('nanoassert')
var cipher = require('./cipher')

var STATELEN = cipher.KEYLEN + cipher.NONCELEN
var NONCELEN = cipher.NONCELEN
var MACLEN = cipher.MACLEN

module.exports = {
  STATELEN,
  NONCELEN,
  MACLEN,
  initializeKey,
  hasKey,
  setNonce,
  encryptWithAd,
  decryptWithAd,
  rekey
}

var KEY_BEGIN = 0
var KEY_END = cipher.KEYLEN
var NONCE_BEGIN = KEY_END
var NONCE_END = NONCE_BEGIN + cipher.NONCELEN

function initializeKey (state, key) {
  assert(state.byteLength === STATELEN)
  assert(key == null ? true : key.byteLength === cipher.KEYLEN)

  if (key == null) {
    sodium_memzero(state.subarray(KEY_BEGIN, KEY_END))
    return
  }

  state.set(key)
  sodium_memzero(state.subarray(NONCE_BEGIN, NONCE_END))
}

function hasKey (state) {
  assert(state.byteLength === STATELEN)
  var k = state.subarray(KEY_BEGIN, KEY_END)
  return sodium_is_zero(k) === false
}

function setNonce (state, nonce) {
  assert(state.byteLength === STATELEN)
  assert(nonce.byteLength === NONCELEN)

  state.set(nonce, NONCE_BEGIN)
}

var maxnonce = new Uint8Array(8).fill(0xff)
function encryptWithAd (state, out, ad, plaintext) {
  assert(state.byteLength === STATELEN)
  assert(out.byteLength != null)
  assert(plaintext.byteLength != null)

  var n = state.subarray(NONCE_BEGIN, NONCE_END)
  if (sodium_memcmp(n, maxnonce)) throw new Error('Nonce overflow')

  if (hasKey(state) === false) {
    out.set(plaintext)
    encryptWithAd.bytesRead = plaintext.byteLength
    encryptWithAd.bytesWritten = encryptWithAd.bytesRead
    return
  }

  var k = state.subarray(KEY_BEGIN, KEY_END)

  cipher.encrypt(
    out,
    k,
    n,
    ad,
    plaintext
  )
  encryptWithAd.bytesRead = cipher.encrypt.bytesRead
  encryptWithAd.bytesWritten = cipher.encrypt.bytesWritten

  sodium_increment(n)
}
encryptWithAd.bytesRead = 0
encryptWithAd.bytesWritten = 0

function decryptWithAd (state, out, ad, ciphertext) {
  assert(state.byteLength === STATELEN)
  assert(out.byteLength != null)
  assert(ciphertext.byteLength != null)

  var n = state.subarray(NONCE_BEGIN, NONCE_END)
  if (sodium_memcmp(n, maxnonce)) throw new Error('Nonce overflow')

  if (hasKey(state) === false) {
    out.set(ciphertext)
    decryptWithAd.bytesRead = ciphertext.byteLength
    decryptWithAd.bytesWritten = decryptWithAd.bytesRead
    return
  }

  var k = state.subarray(KEY_BEGIN, KEY_END)

  cipher.decrypt(
    out,
    k,
    n,
    ad,
    ciphertext
  )
  decryptWithAd.bytesRead = cipher.decrypt.bytesRead
  decryptWithAd.bytesWritten = cipher.decrypt.bytesWritten

  sodium_increment(n)
}
decryptWithAd.bytesRead = 0
decryptWithAd.bytesWritten = 0

function rekey (state) {
  assert(state.byteLength === STATELEN)

  var k = state.subarray(KEY_BEGIN, KEY_END)
  cipher.rekey(k, k)
  rekey.bytesRead = cipher.rekey.bytesRead
  rekey.bytesWritten = cipher.rekey.bytesWritten
}
rekey.bytesRead = 0
rekey.bytesWritten = 0
