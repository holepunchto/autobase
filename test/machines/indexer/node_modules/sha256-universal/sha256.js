if (btoa == null) var btoa = buf => require('buf' + 'fer')['Buf' + 'fer'].from(buf).toString('base64')
if (atob == null) var atob = buf => new Uint8Array(require('buf' + 'fer')['Buf' + 'fer'].from(buf, 'base64'))

const assert = require('nanoassert')

module.exports = Sha256
const SHA256_BYTES = module.exports.SHA256_BYTES = 32
const BLOCKSIZE = 64

const K = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
  0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
  0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
  0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
  0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
  0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
]

function expand (a, b, c, d) {
  var b_ = (((a >>> 17) | (a << 15)) ^ ((a >>> 19) | (a << 13)) ^ (a >>> 10)) + b
  var d_ = (((c >>> 7) | (c << 25)) ^ ((c >>> 18) | (c << 14)) ^ (c >>> 3)) + d

  return (b_ + d_) << 0
}

function compress (state, words) {
  // initialise registers
  var ch, maj, s0, s1, T1, T2
  var [a, b, c, d, e, f, g, h] = state

  // expand message schedule
  const w = new Uint32Array(64)
  for (let i = 0; i < 16; i++) w[i] = bswap(words[i])
  for (let i = 16; i < 64; i++) w[i] = expand(w[i - 2], w[i - 7], w[i - 15], w[i - 16])
  for (let i = 0; i < 64; i += 4) round(i)

  state[0] = state[0] + a
  state[1] = state[1] + b
  state[2] = state[2] + c
  state[3] = state[3] + d
  state[4] = state[4] + e
  state[5] = state[5] + f
  state[6] = state[6] + g
  state[7] = state[7] + h

  function round (n) {
    ch = (e & f) ^ (~e & g)
    maj = (a & b) ^ (a & c) ^ (b & c)
    s0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10))
    s1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7))
    T1 = h + ch + s1 + w[n] + K[n]
    T2 = s0 + maj
    h = d + T1
    d = T1 + T2

    ch = (h & e) ^ (~h & f)
    maj = (d & a) ^ (d & b) ^ (a & b)
    s0 = ((d >>> 2) | (d << 30)) ^ ((d >>> 13) | (d << 19)) ^ ((d >>> 22) | (d << 10))
    s1 = ((h >>> 6) | (h << 26)) ^ ((h >>> 11) | (h << 21)) ^ ((h >>> 25) | (h << 7))
    T1 = g + ch + s1 + w[n + 1] + K[n + 1]
    T2 = s0 + maj
    g = c + T1
    c = T1 + T2

    ch = (g & h) ^ (~g & e)
    maj = (c & d) ^ (c & a) ^ (d & a)
    s0 = ((c >>> 2) | (c << 30)) ^ ((c >>> 13) | (c << 19)) ^ ((c >>> 22) | (c << 10))
    s1 = ((g >>> 6) | (g << 26)) ^ ((g >>> 11) | (g << 21)) ^ ((g >>> 25) | (g << 7))
    T1 = f + ch + s1 + w[n + 2] + K[n + 2]
    T2 = s0 + maj
    f = b + T1
    b = T1 + T2

    ch = (f & g) ^ (~f & h)
    maj = (b & c) ^ (b & d) ^ (c & d)
    s0 = ((b >>> 2) | (b << 30)) ^ ((b >>> 13) | (b << 19)) ^ ((b >>> 22) | (b << 10))
    s1 = ((f >>> 6) | (f << 26)) ^ ((f >>> 11) | (f << 21)) ^ ((f >>> 25) | (f << 7))
    T1 = e + ch + s1 + w[n + 3] + K[n + 3]
    T2 = s0 + maj
    e = a + T1
    a = T1 + T2
  }
}

function Sha256 () {
  if (!(this instanceof Sha256)) return new Sha256()

  this.buffer = new ArrayBuffer(64)
  this.bytesRead = 0
  this.pos = 0
  this.digestLength = SHA256_BYTES
  this.finalised = false

  this.load = new Uint8Array(this.buffer)
  this.words = new Uint32Array(this.buffer)

  this.state = new Uint32Array([
    0x6a09e667,
    0xbb67ae85,
    0x3c6ef372,
    0xa54ff53a,
    0x510e527f,
    0x9b05688c,
    0x1f83d9ab,
    0x5be0cd19
  ])

  return this
}

Sha256.prototype.update = function (input, enc) {
  assert(this.finalised === false, 'Hash instance finalised')

  var [inputBuf, len] = formatInput(input, enc)
  var i = 0
  this.bytesRead += len

  while (len > 0) {
    this.load.set(inputBuf.subarray(i, i + BLOCKSIZE - this.pos), this.pos)
    i += BLOCKSIZE - this.pos
    len -= BLOCKSIZE - this.pos

    if (len < 0) break

    this.pos = 0
    compress(this.state, this.words)
  }

  this.pos = this.bytesRead & 0x3f
  this.load.fill(0, this.pos)

  return this
}

Sha256.prototype.digest = function (enc, offset = 0) {
  assert(this.finalised === false, 'Hash instance finalised')
  this.finalised = true

  this.load.fill(0, this.pos)
  this.load[this.pos] = 0x80

  if (this.pos > 55) {
    compress(this.state, this.words)

    this.words.fill(0)
    this.pos = 0
  }

  const view = new DataView(this.buffer)
  view.setUint32(56, this.bytesRead / 2 ** 29)
  view.setUint32(60, this.bytesRead << 3)

  compress(this.state, this.words)

  const resultBuf = new Uint8Array(this.state.map(bswap).buffer)

  if (!enc) {
    return new Uint8Array(resultBuf)
  }

  if (typeof enc === 'string') {
    if (enc === 'hex') return hexSlice(resultBuf, 0, resultBuf.length)
    if (enc === 'utf8' || enc === 'utf-8') return new TextEncoder().encode(resultBuf)
    if (enc === 'base64') return btoa(resultBuf)
    throw new Error('Encoding: ' + enc + ' not supported')
  }

  assert(enc instanceof Uint8Array, 'input must be Uint8Array or Buffer')
  assert(enc.byteLength >= this.digestLength + offset, 'input not large enough for digest')

  for (let i = 0; i < this.digestLength; i++) {
    enc[i + offset] = resultBuf[i]
  }

  return enc
}

function HMAC (key) {
  if (!(this instanceof HMAC)) return new HMAC(key)

  this.pad = Buffer.alloc(64)
  this.inner = Sha256()
  this.outer = Sha256()

  const keyhash = Buffer.alloc(32)
  if (key.byteLength > 64) {
    Sha256().update(key).digest(keyhash)
    key = keyhash
  }

  this.pad.fill(0x36)
  for (let i = 0; i < key.byteLength; i++) {
    this.pad[i] ^= key[i]
  }
  this.inner.update(this.pad)

  this.pad.fill(0x5c)
  for (let i = 0; i < key.byteLength; i++) {
    this.pad[i] ^= key[i]
  }
  this.outer.update(this.pad)

  this.pad.fill(0)
  keyhash.fill(0)
}

HMAC.prototype.update = function (input, enc) {
  this.inner.update(input, enc)
  return this
}

HMAC.prototype.digest = function (enc, offset = 0) {
  this.outer.update(this.inner.digest())
  return this.outer.digest(enc, offset)
}

Sha256.HMAC = HMAC

function hexSlice (buf, start = 0, len) {
  if (!len) len = buf.byteLength

  var str = ''
  for (var i = 0; i < len; i++) str += toHex(buf[start + i])
  return str
}

function toHex (n) {
  if (n < 16) return '0' + n.toString(16)
  return n.toString(16)
}

function formatInput (input, enc) {
  var result = input instanceof Uint8Array ? input : strToBuf(input, enc)

  return [result, result.byteLength]
}

function strToBuf (input, enc) {
  if (enc === 'hex') return hex2bin(input)
  else if (enc === 'utf8' || enc === 'utf-8') return new TextDecoder().decode(input)
  else if (enc === 'base64') return atob(input)
  else throw new Error('Encoding: ' + enc + ' not supported')
}

function hex2bin (str) {
  if (str.length % 2 !== 0) return hex2bin('0' + str)
  var ret = new Uint8Array(str.length / 2)
  for (var i = 0; i < ret.length; i++) ret[i] = Number('0x' + str.substring(2 * i, 2 * i + 2))
  return ret
}

function bswap (a) {
  var r = ((a & 0x00ff00ff) >>> 8) | ((a & 0x00ff00ff) << 24)
  var l = ((a & 0xff00ff00) << 8) | ((a & 0xff00ff00) >>> 24)

  return r | l
}
