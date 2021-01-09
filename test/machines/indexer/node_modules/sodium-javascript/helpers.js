/* eslint-disable camelcase */
const assert = require('nanoassert')
const { vn } = require('./crypto_verify')

function sodium_increment (n) {
  const nlen = n.byteLength
  var c = 1
  for (var i = 0; i < nlen; i++) {
    c += n[i]
    n[i] = c
    c >>= 8
  }
}

function sodium_memcmp (a, b) {
  assert(a.byteLength === b.byteLength, 'buffers must be the same size')

  return vn(a, 0, b, 0, a.byteLength) === 0
}

function sodium_is_zero (arr) {
  var d = 0
  for (let i = 0; i < arr.length; i++) d |= arr[i]
  return d === 0
}

module.exports = {
  sodium_increment,
  sodium_memcmp,
  sodium_is_zero
}
