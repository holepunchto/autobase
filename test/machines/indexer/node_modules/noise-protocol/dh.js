/* eslint-disable camelcase */
const { crypto_kx_SEEDBYTES, crypto_kx_keypair, crypto_kx_seed_keypair } = require('sodium-universal/crypto_kx')
const { crypto_scalarmult_BYTES, crypto_scalarmult_SCALARBYTES, crypto_scalarmult } = require('sodium-universal/crypto_scalarmult')

var assert = require('nanoassert')

var DHLEN = crypto_scalarmult_BYTES
var PKLEN = crypto_scalarmult_BYTES
var SKLEN = crypto_scalarmult_SCALARBYTES
var SEEDLEN = crypto_kx_SEEDBYTES

module.exports = {
  DHLEN,
  PKLEN,
  SKLEN,
  SEEDLEN,
  generateKeypair,
  generateSeedKeypair,
  dh
}

function generateKeypair (pk, sk) {
  assert(pk.byteLength === PKLEN)
  assert(sk.byteLength === SKLEN)
  crypto_kx_keypair(pk, sk)
}

function generateSeedKeypair (pk, sk, seed) {
  assert(pk.byteLength === PKLEN)
  assert(sk.byteLength === SKLEN)
  assert(seed.byteLength === SKLEN)

  crypto_kx_seed_keypair(pk, sk, seed)
}

function dh (output, lsk, pk) {
  assert(output.byteLength === DHLEN)
  assert(lsk.byteLength === SKLEN)
  assert(pk.byteLength === PKLEN)

  crypto_scalarmult(
    output,
    lsk,
    pk
  )
}
