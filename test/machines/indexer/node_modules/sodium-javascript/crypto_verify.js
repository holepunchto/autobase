/* eslint-disable camelcase */
module.exports = {
  crypto_verify_16,
  crypto_verify_32,
  crypto_verify_64
}

function vn (x, xi, y, yi, n) {
  var d = 0
  for (let i = 0; i < n; i++) d |= x[xi + i] ^ y[yi + i]
  return (1 & ((d - 1) >>> 8)) - 1
}

// Make non enumerable as this is an internal function
Object.defineProperty(module.exports, 'vn', {
  value: vn
})

function crypto_verify_16 (x, xi, y, yi) {
  return vn(x, xi, y, yi, 16) === 0
}

function crypto_verify_32 (x, xi, y, yi) {
  return vn(x, xi, y, yi, 32) === 0
}

function crypto_verify_64 (x, xi, y, yi) {
  return vn(x, xi, y, yi, 64) === 0
}
