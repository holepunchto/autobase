/* eslint-disable camelcase */
const sha256 = require('sha256-universal')
const assert = require('nanoassert')

if (new Uint16Array([1])[0] !== 1) throw new Error('Big endian architecture is not supported.')

const crypto_hash_sha256_BYTES = 32

function crypto_hash_sha256 (out, m, n) {
  assert(out.byteLength === crypto_hash_sha256_BYTES, "out must be 'crypto_hash_sha256_BYTES' bytes long")

  sha256().update(m.subarray(0, n)).digest(out)
  return 0
}

module.exports = {
  crypto_hash_sha256,
  crypto_hash_sha256_BYTES
}
