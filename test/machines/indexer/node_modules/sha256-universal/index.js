const js = require('./sha256.js')
const wasm = require('sha256-wasm')

var Proto = js

module.exports = function () {
  return new Proto()
}

module.exports.ready = function (cb) {
  wasm.ready(function () { // ignore errors
    cb()
  })
}

module.exports.WASM_SUPPORTED = wasm.WASM_SUPPORTED
module.exports.WASM_LOADED = false

var SHA256_BYTES = module.exports.SHA256_BYTES = 32

wasm.ready(function (err) {
  if (!err) {
    module.exports.WASM_LOADED = true
    module.exports = Proto = wasm
  }
})
