/* eslint-disable camelcase */
const { sodium_malloc, sodium_memzero } = require('sodium-universal/memory')
var assert = require('nanoassert')
var cipherState = require('./cipher-state')
var hash = require('./hash')

var STATELEN = hash.HASHLEN + hash.HASHLEN + cipherState.STATELEN
var HASHLEN = hash.HASHLEN

module.exports = {
  STATELEN,
  initializeSymmetric,
  mixKey,
  mixHash,
  mixKeyAndHash,
  getHandshakeHash,
  encryptAndHash,
  decryptAndHash,
  split,
  _hasKey
}

var CHAINING_KEY_BEGIN = 0
var CHAINING_KEY_END = hash.HASHLEN
var HASH_BEGIN = CHAINING_KEY_END
var HASH_END = HASH_BEGIN + hash.HASHLEN
var CIPHER_BEGIN = HASH_END
var CIPHER_END = CIPHER_BEGIN + cipherState.STATELEN

function initializeSymmetric (state, protocolName) {
  assert(state.byteLength === STATELEN)
  assert(protocolName.byteLength != null)

  sodium_memzero(state)
  if (protocolName.byteLength <= HASHLEN) state.set(protocolName, HASH_BEGIN)
  else hash.hash(state.subarray(HASH_BEGIN, HASH_END), [protocolName])

  state.subarray(CHAINING_KEY_BEGIN, CHAINING_KEY_END).set(state.subarray(HASH_BEGIN, HASH_END))

  cipherState.initializeKey(state.subarray(CIPHER_BEGIN, CIPHER_END), null)
}

var TempKey = sodium_malloc(HASHLEN)
function mixKey (state, inputKeyMaterial) {
  assert(state.byteLength === STATELEN)
  assert(inputKeyMaterial.byteLength != null)

  hash.hkdf(
    state.subarray(CHAINING_KEY_BEGIN, CHAINING_KEY_END),
    TempKey,
    null,
    state.subarray(CHAINING_KEY_BEGIN, CHAINING_KEY_END),
    inputKeyMaterial
  )

  // HASHLEN is always 64 here, so we truncate to 32 bytes per the spec
  cipherState.initializeKey(state.subarray(CIPHER_BEGIN, CIPHER_END), TempKey.subarray(0, 32))
  sodium_memzero(TempKey)
}

function mixHash (state, data) {
  assert(state.byteLength === STATELEN)

  var h = state.subarray(HASH_BEGIN, HASH_END)

  hash.hash(h, [h, data])
}

var TempHash = sodium_malloc(HASHLEN)
function mixKeyAndHash (state, inputKeyMaterial) {
  assert(state.byteLength === STATELEN)
  assert(inputKeyMaterial.byteLength != null)

  hash.hkdf(
    state.subarray(CHAINING_KEY_BEGIN, CHAINING_KEY_END),
    TempHash,
    TempKey,
    state.subarray(CHAINING_KEY_BEGIN, CHAINING_KEY_END),
    inputKeyMaterial
  )

  mixHash(state, TempHash)
  sodium_memzero(TempHash)

  // HASHLEN is always 64 here, so we truncate to 32 bytes per the spec
  cipherState.initializeKey(state.subarray(CIPHER_BEGIN, CIPHER_END), TempKey.subarray(0, 32))
  sodium_memzero(TempKey)
}

function getHandshakeHash (state, out) {
  assert(state.byteLength === STATELEN)
  assert(out.byteLength === HASHLEN)

  out.set(state.subarray(HASH_BEGIN, HASH_END))
}

// ciphertext is the output here
function encryptAndHash (state, ciphertext, plaintext) {
  assert(state.byteLength === STATELEN)
  assert(ciphertext.byteLength != null)
  assert(plaintext.byteLength != null)

  var cstate = state.subarray(CIPHER_BEGIN, CIPHER_END)
  var h = state.subarray(HASH_BEGIN, HASH_END)

  cipherState.encryptWithAd(cstate, ciphertext, h, plaintext)
  encryptAndHash.bytesRead = cipherState.encryptWithAd.bytesRead
  encryptAndHash.bytesWritten = cipherState.encryptWithAd.bytesWritten
  mixHash(state, ciphertext.subarray(0, encryptAndHash.bytesWritten))
}
encryptAndHash.bytesRead = 0
encryptAndHash.bytesWritten = 0

// plaintext is the output here
function decryptAndHash (state, plaintext, ciphertext) {
  assert(state.byteLength === STATELEN)
  assert(plaintext.byteLength != null)
  assert(ciphertext.byteLength != null)

  var cstate = state.subarray(CIPHER_BEGIN, CIPHER_END)
  var h = state.subarray(HASH_BEGIN, HASH_END)

  cipherState.decryptWithAd(cstate, plaintext, h, ciphertext)
  decryptAndHash.bytesRead = cipherState.decryptWithAd.bytesRead
  decryptAndHash.bytesWritten = cipherState.decryptWithAd.bytesWritten
  mixHash(state, ciphertext.subarray(0, decryptAndHash.bytesRead))
}
decryptAndHash.bytesRead = 0
decryptAndHash.bytesWritten = 0

var TempKey1 = sodium_malloc(HASHLEN)
var TempKey2 = sodium_malloc(HASHLEN)
var zerolen = new Uint8Array(0)
function split (state, cipherstate1, cipherstate2) {
  assert(state.byteLength === STATELEN)
  assert(cipherstate1.byteLength === cipherState.STATELEN)
  assert(cipherstate2.byteLength === cipherState.STATELEN)

  hash.hkdf(
    TempKey1,
    TempKey2,
    null,
    state.subarray(CHAINING_KEY_BEGIN, CHAINING_KEY_END),
    zerolen
  )

  // HASHLEN is always 64 here, so we truncate to 32 bytes per the spec
  cipherState.initializeKey(cipherstate1, TempKey1.subarray(0, 32))
  cipherState.initializeKey(cipherstate2, TempKey2.subarray(0, 32))
  sodium_memzero(TempKey1)
  sodium_memzero(TempKey2)
}

function _hasKey (state) {
  return cipherState.hasKey(state.subarray(CIPHER_BEGIN, CIPHER_END))
}
