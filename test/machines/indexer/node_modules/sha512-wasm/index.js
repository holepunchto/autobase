if (btoa == null) var btoa = buf => require('buf' + 'fer')['Buf' + 'fer'].from(buf).toString('base64')
if (atob == null) var atob = buf => new Uint8Array(require('buf' + 'fer')['Buf' + 'fer'].from(buf, 'base64'))

const assert = require('nanoassert')
const wasm = require('./sha512.js')({
  imports: {
    debug: {
      log (...args) {
        console.log(...args.map(int => (int >>> 0).toString(16).padStart(8, '0')))
      },
      log_tee (arg) {
        console.log((arg >>> 0).toString(16).padStart(8, '0'))
        return arg
      }
    }
  }
})

let head = 0
// assetrt head % 8 === 0 to guarantee alignment
const freeList = []

module.exports = Sha512
const SHA512_BYTES = module.exports.SHA512_BYTES = 64
const INPUT_OFFSET = 80
const STATEBYTES = 216
const BLOCKSIZE = 128

function Sha512 () {
  if (!(this instanceof Sha512)) return new Sha512()
  if (!(wasm && wasm.exports)) throw new Error('WASM not loaded. Wait for Sha512.ready(cb)')

  if (!freeList.length) {
    freeList.push(head)
    head += STATEBYTES
  }

  this.finalized = false
  this.digestLength = SHA512_BYTES
  this.pointer = freeList.pop()
  this.pos = 0
  this.wasm = wasm

  wasm.memory.fill(0, this.pointer, this.pointer + STATEBYTES)

  if (this.pointer + this.digestLength > wasm.memory.length) wasm.realloc(this.pointer + STATEBYTES)
}

Sha512.prototype.update = function (input, enc) {
  assert(this.finalized === false, 'Hash instance finalized')

  if (head % 8 !== 0) head += 8 - head % 8
  assert(head % 8 === 0, 'input should be aligned for int64')

  const [inputBuf, length] = formatInput(input, enc)

  assert(inputBuf instanceof Uint8Array, 'input must be Uint8Array or Buffer')

  if (head + input.length > wasm.memory.length) wasm.realloc(head + input.length)

  wasm.memory.fill(0, head, head + roundUp(length, BLOCKSIZE) - BLOCKSIZE)
  wasm.memory.set(inputBuf.subarray(0, BLOCKSIZE - this.pos), this.pointer + INPUT_OFFSET + this.pos)
  wasm.memory.set(inputBuf.subarray(BLOCKSIZE - this.pos), head)

  this.pos = (this.pos + length) & 0x7f
  wasm.exports.sha512(this.pointer, head, length, 0)

  return this
}

Sha512.prototype.digest = function (enc, offset = 0) {
  assert(this.finalized === false, 'Hash instance finalized')

  this.finalized = true
  freeList.push(this.pointer)

  const paddingStart = this.pointer + INPUT_OFFSET + this.pos
  wasm.memory.fill(0, paddingStart, this.pointer + INPUT_OFFSET + BLOCKSIZE)
  wasm.exports.sha512(this.pointer, head, 0, 1)

  const resultBuf = wasm.memory.subarray(this.pointer, this.pointer + this.digestLength)

  if (!enc) {
    return resultBuf
  }

  if (typeof enc === 'string') {
    if (enc === 'hex') return hexSlice(resultBuf, 0, resultBuf.length)
    if (enc === 'utf8' || enc === 'utf-8') return new TextEncoder().encode(resultBuf)
    if (enc === 'base64') return btoa(resultBuf)
    throw new Error('Encoding: ' + enc + ' not supported')
  }

  assert(enc instanceof Uint8Array, 'output must be Uint8Array or Buffer')
  assert(enc.byteLength >= this.digestLength + offset,
    "output must have at least 'SHA512_BYTES' bytes remaining")

  for (let i = 0; i < this.digestLength; i++) {
    enc[i + offset] = resultBuf[i]
  }

  return enc
}

Sha512.WASM = wasm && wasm.buffer
Sha512.WASM_SUPPORTED = typeof WebAssembly !== 'undefined'

Sha512.ready = function (cb) {
  if (!cb) cb = noop
  if (!wasm) return cb(new Error('WebAssembly not supported'))

  var p = new Promise(function (reject, resolve) {
    wasm.onload(function (err) {
      if (err) resolve(err)
      else reject()
      cb(err)
    })
  })

  return p
}

Sha512.prototype.ready = Sha512.ready

function HMAC (key) {
  if (!(this instanceof HMAC)) return new HMAC(key)

  this.pad = Buffer.alloc(128)
  this.inner = Sha512()
  this.outer = Sha512()

  const keyhash = Buffer.alloc(64)
  if (key.byteLength > 128) {
    Sha512().update(key).digest(keyhash)
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

Sha512.HMAC = HMAC

function noop () {}

function formatInput (input, enc) {
  var result = input instanceof Uint8Array ? input : strToBuf(input, enc)

  return [result, result.byteLength]
}

function strToBuf (input, enc) {
  if (enc === 'hex') return hex2bin(input)
  else if (enc === 'utf8' || enc === 'utf-8') return new TextEncoder().encode(input)
  else if (enc === 'base64') return atob(input)
  else throw new Error('Encoding: ' + enc + ' not supported')
}

function hex2bin (str) {
  if (str.length % 2 !== 0) return hex2bin('0' + str)
  var ret = new Uint8Array(str.length / 2)
  for (var i = 0; i < ret.length; i++) ret[i] = Number('0x' + str.substring(2 * i, 2 * i + 2))
  return ret
}

function hexSlice (buf, start, len) {
  var str = ''
  for (var i = 0; i < len; i++) str += toHex(buf[start + i])
  return str
}

function toHex (n) {
  if (n < 16) return '0' + n.toString(16)
  return n.toString(16)
}

// only works for base that is power of 2
function roundUp (n, base) {
  return (n + base - 1) & -base
}
