/* eslint-disable camelcase */
const sha512 = require('sha512-universal')
const assert = require('nanoassert')

if (new Uint16Array([1])[0] !== 1) throw new Error('Big endian architecture is not supported.')

const crypto_hash_sha512_BYTES = 64
const crypto_hash_BYTES = crypto_hash_sha512_BYTES

function crypto_hash_sha512 (out, m, n) {
  assert(out.byteLength === crypto_hash_sha512_BYTES, "out must be 'crypto_hash_sha512_BYTES' bytes long")

  sha512().update(m.subarray(0, n)).digest(out)
  return 0
}

function crypto_hash (out, m, n) {
  return crypto_hash_sha512(out, m, n)
}

module.exports = {
  crypto_hash,
  crypto_hash_sha512,
  crypto_hash_sha512_BYTES,
  crypto_hash_BYTES
}
