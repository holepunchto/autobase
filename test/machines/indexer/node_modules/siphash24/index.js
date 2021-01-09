var wasm = require('./siphash24')
var fallback = require('./fallback')
var assert = require('nanoassert')

module.exports = siphash24

var BYTES = siphash24.BYTES = 8
var KEYBYTES = siphash24.KEYBYTES = 16
var mod = wasm()

siphash24.WASM_SUPPORTED = typeof WebAssembly !== 'undefined'
siphash24.WASM_LOADED = false

if (mod) {
  mod.onload(function (err) {
    siphash24.WASM_LOADED = !err
  })
}

function siphash24 (data, key, out, noAssert) {
  if (!out) out = new Uint8Array(8)

  if (noAssert !== true) {
    assert(out.length >= BYTES, 'output must be at least ' + BYTES)
    assert(key.length >= KEYBYTES, 'key must be at least ' + KEYBYTES)
  }

  if (mod && mod.exports) {
    if (data.length + 24 > mod.memory.length) mod.realloc(data.length + 24)
    mod.memory.set(key, 8)
    mod.memory.set(data, 24)
    mod.exports.siphash(24, data.length)
    out.set(mod.memory.subarray(0, 8))
  } else {
    fallback(out, data, key)
  }

  return out
}
