/* eslint-disable camelcase, one-var */
const { _9, _121665, gf, inv25519, pack25519, unpack25519, sel25519, A, M, Z, S } = require('./internal/ed25519')

const crypto_scalarmult_BYTES = 32
const crypto_scalarmult_SCALARBYTES = 32

module.exports = {
  crypto_scalarmult,
  crypto_scalarmult_base,
  crypto_scalarmult_BYTES,
  crypto_scalarmult_SCALARBYTES
}

function crypto_scalarmult (q, n, p) {
  check(q, crypto_scalarmult_BYTES)
  check(n, crypto_scalarmult_SCALARBYTES)
  check(p, crypto_scalarmult_BYTES)
  var z = new Uint8Array(32)
  var x = new Float64Array(80), r, i
  var a = gf(), b = gf(), c = gf(),
    d = gf(), e = gf(), f = gf()
  for (i = 0; i < 31; i++) z[i] = n[i]
  z[31] = (n[31] & 127) | 64
  z[0] &= 248
  unpack25519(x, p)
  for (i = 0; i < 16; i++) {
    b[i] = x[i]
    d[i] = a[i] = c[i] = 0
  }
  a[0] = d[0] = 1
  for (i = 254; i >= 0; --i) {
    r = (z[i >>> 3] >>> (i & 7)) & 1
    sel25519(a, b, r)
    sel25519(c, d, r)
    A(e, a, c)
    Z(a, a, c)
    A(c, b, d)
    Z(b, b, d)
    S(d, e)
    S(f, a)
    M(a, c, a)
    M(c, b, e)
    A(e, a, c)
    Z(a, a, c)
    S(b, a)
    Z(c, d, f)
    M(a, c, _121665)
    A(a, a, d)
    M(c, c, a)
    M(a, d, f)
    M(d, b, x)
    S(b, e)
    sel25519(a, b, r)
    sel25519(c, d, r)
  }
  for (i = 0; i < 16; i++) {
    x[i + 16] = a[i]
    x[i + 32] = c[i]
    x[i + 48] = b[i]
    x[i + 64] = d[i]
  }
  var x32 = x.subarray(32)
  var x16 = x.subarray(16)
  inv25519(x32, x32)
  M(x16, x16, x32)
  pack25519(q, x16)
  return 0
}

function crypto_scalarmult_base (q, n) {
  return crypto_scalarmult(q, n, _9)
}

function check (buf, len) {
  if (!buf || (len && buf.length < len)) throw new Error('Argument must be a buffer' + (len ? ' of length ' + len : ''))
}
