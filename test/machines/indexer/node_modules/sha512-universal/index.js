const js = require('./sha512.js')
const wasm = require('sha512-wasm')

var Proto = js

module.exports = function () {
  return new Proto()
}

module.exports.ready = function (cb) {
  wasm.ready(function () { // ignore errors
    cb()
  })
}

module.exports.WASM_SUPPORTED = wasm.SUPPORTED
module.exports.WASM_LOADED = false

var SHA512_BYTES = module.exports.SHA512_BYTES = 64

wasm.ready(function (err) {
  if (!err) {
    module.exports.WASM_LOADED = true
    module.exports = Proto = wasm
  }
})
