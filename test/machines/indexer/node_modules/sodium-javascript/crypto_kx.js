/* eslint-disable camelcase */
const { crypto_scalarmult_base } = require('./crypto_scalarmult')
const { crypto_generichash } = require('./crypto_generichash')
const { randombytes_buf } = require('./randombytes')
const assert = require('nanoassert')

const crypto_kx_SEEDBYTES = 32
const crypto_kx_PUBLICKEYBYTES = 32
const crypto_kx_SECRETKEYBYTES = 32

function crypto_kx_keypair (pk, sk) {
  assert(pk.byteLength === crypto_kx_PUBLICKEYBYTES, "pk must be 'crypto_kx_PUBLICKEYBYTES' bytes")
  assert(sk.byteLength === crypto_kx_SECRETKEYBYTES, "sk must be 'crypto_kx_SECRETKEYBYTES' bytes")

  randombytes_buf(sk, crypto_kx_SECRETKEYBYTES)
  return crypto_scalarmult_base(pk, sk)
}

function crypto_kx_seed_keypair (pk, sk, seed) {
  assert(pk.byteLength === crypto_kx_PUBLICKEYBYTES, "pk must be 'crypto_kx_PUBLICKEYBYTES' bytes")
  assert(sk.byteLength === crypto_kx_SECRETKEYBYTES, "sk must be 'crypto_kx_SECRETKEYBYTES' bytes")
  assert(seed.byteLength === crypto_kx_SEEDBYTES, "seed must be 'crypto_kx_SEEDBYTES' bytes")

  crypto_generichash(sk, seed)
  return crypto_scalarmult_base(pk, sk)
}

module.exports = {
  crypto_kx_keypair,
  crypto_kx_seed_keypair,
  crypto_kx_SEEDBYTES,
  crypto_kx_SECRETKEYBYTES,
  crypto_kx_PUBLICKEYBYTES
}
