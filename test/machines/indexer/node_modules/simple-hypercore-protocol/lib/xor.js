const XSalsa20 = require('xsalsa20-universal')
const crypto = require('hypercore-crypto')

module.exports = class XOR {
  constructor (nonces, split) {
    this.rnonce = nonces.rnonce
    this.tnonce = nonces.tnonce
    this.rx = new XSalsa20(this.rnonce, split.rx.slice(0, 32))
    this.tx = new XSalsa20(this.tnonce, split.tx.slice(0, 32))
  }

  encrypt (data) {
    this.tx.update(data, data)
    return data
  }

  decrypt (data) {
    this.rx.update(data, data)
    return data
  }

  destroy () {
    this.tx.final()
    this.rx.final()
  }

  static nonce () {
    return crypto.randomBytes(24)
  }
}
