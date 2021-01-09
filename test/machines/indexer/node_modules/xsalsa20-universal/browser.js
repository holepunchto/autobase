const xsalsa20 = require('xsalsa20')

module.exports = class XORJS {
  constructor (nonce, key) {
    this.instance = xsalsa20(nonce, key)
  }

  update (out, message) {
    this.instance.update(message, out)
  }

  final () {
    this.instance.finalize()
  }
}
