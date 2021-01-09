const sha512 = require('./')
const js512 = require('./sha512')
const test = require('sha-test').sha512

if (!sha512.WASM_SUPPORTED) {
  console.log('testing JavaScript implementation')
  test(sha512)
} else {
  sha512.ready(() => {
    console.log('testing WebAssembly implementation')
    test(sha512)

    console.log('testing JavaScript implementation')
    test(js512)
  })
}
