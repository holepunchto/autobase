const sha256 = require('./')
const js256 = require('./sha256')
const test = require('sha-test').sha256

if (!sha256.WASM_SUPPORTED) {
  console.log('testing JavaScript implementation')
  test(sha256)
} else {
  sha256.ready(() => {
    console.log('testing WebAssembly implementation')
    test(sha256)

    console.log('testing JavaScript implementation')
    test(js256)
  })
}
