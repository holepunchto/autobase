/* eslint-disable camelcase */
const xsalsa20 = require('xsalsa20')

if (new Uint16Array([1])[0] !== 1) throw new Error('Big endian architecture is not supported.')

exports.crypto_stream_KEYBYTES = 32
exports.crypto_stream_NONCEBYTES = 24
exports.crypto_stream_PRIMITIVE = 'xsalsa20'
exports.crypto_stream_xsalsa20_MESSAGEBYTES_MAX = Number.MAX_SAFE_INTEGER

exports.crypto_stream = function (c, nonce, key) {
  c.fill(0)
  exports.crypto_stream_xor(c, c, nonce, key)
}

exports.crypto_stream_xor = function (c, m, nonce, key) {
  const xor = xsalsa20(nonce, key)

  xor.update(m, c)
  xor.final()
}

exports.crypto_stream_xor_instance = function (nonce, key) {
  return new XOR(nonce, key)
}

function XOR (nonce, key) {
  this._instance = xsalsa20(nonce, key)
}

XOR.prototype.update = function (out, inp) {
  this._instance.update(inp, out)
}

XOR.prototype.final = function () {
  this._instance.finalize()
  this._instance = null
}
