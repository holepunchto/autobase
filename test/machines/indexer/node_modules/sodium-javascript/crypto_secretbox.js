/* eslint-disable camelcase */
const assert = require('nanoassert')
const { crypto_stream, crypto_stream_xor } = require('./crypto_stream')
const { crypto_onetimeauth, crypto_onetimeauth_verify, crypto_onetimeauth_BYTES, crypto_onetimeauth_KEYBYTES } = require('./crypto_onetimeauth')

const crypto_secretbox_KEYBYTES = 32
const crypto_secretbox_NONCEBYTES = 24
const crypto_secretbox_ZEROBYTES = 32
const crypto_secretbox_BOXZEROBYTES = 16
const crypto_secretbox_MACBYTES = 16

module.exports = {
  crypto_secretbox,
  crypto_secretbox_open,
  crypto_secretbox_detached,
  crypto_secretbox_open_detached,
  crypto_secretbox_easy,
  crypto_secretbox_open_easy,
  crypto_secretbox_KEYBYTES,
  crypto_secretbox_NONCEBYTES,
  crypto_secretbox_ZEROBYTES,
  crypto_secretbox_BOXZEROBYTES,
  crypto_secretbox_MACBYTES
}

function crypto_secretbox (c, m, n, k) {
  assert(c.byteLength === m.byteLength, "c must be 'm.byteLength' bytes")
  const mlen = m.byteLength
  assert(mlen >= crypto_secretbox_ZEROBYTES, "mlen must be at least 'crypto_secretbox_ZEROBYTES'")
  assert(n.byteLength === crypto_secretbox_NONCEBYTES, "n must be 'crypto_secretbox_NONCEBYTES' bytes")
  assert(k.byteLength === crypto_secretbox_KEYBYTES, "k must be 'crypto_secretbox_KEYBYTES' bytes")

  crypto_stream_xor(c, m, n, k)
  crypto_onetimeauth(
    c.subarray(crypto_secretbox_BOXZEROBYTES, crypto_secretbox_BOXZEROBYTES + crypto_onetimeauth_BYTES),
    c.subarray(crypto_secretbox_BOXZEROBYTES + crypto_onetimeauth_BYTES, c.byteLength),
    c.subarray(0, crypto_onetimeauth_KEYBYTES)
  )
  c.fill(0, 0, crypto_secretbox_BOXZEROBYTES)
}

function crypto_secretbox_open (m, c, n, k) {
  assert(c.byteLength === m.byteLength, "c must be 'm.byteLength' bytes")
  const mlen = m.byteLength
  assert(mlen >= crypto_secretbox_ZEROBYTES, "mlen must be at least 'crypto_secretbox_ZEROBYTES'")
  assert(n.byteLength === crypto_secretbox_NONCEBYTES, "n must be 'crypto_secretbox_NONCEBYTES' bytes")
  assert(k.byteLength === crypto_secretbox_KEYBYTES, "k must be 'crypto_secretbox_KEYBYTES' bytes")

  const x = new Uint8Array(crypto_onetimeauth_KEYBYTES)
  crypto_stream(x, n, k)
  const validMac = crypto_onetimeauth_verify(
    c.subarray(crypto_secretbox_BOXZEROBYTES, crypto_secretbox_BOXZEROBYTES + crypto_onetimeauth_BYTES),
    c.subarray(crypto_secretbox_BOXZEROBYTES + crypto_onetimeauth_BYTES, c.byteLength),
    x
  )

  if (validMac === false) return false
  crypto_stream_xor(m, c, n, k)
  m.fill(0, 0, 32)
  return true
}

function crypto_secretbox_detached (o, mac, msg, n, k) {
  assert(o.byteLength === msg.byteLength, "o must be 'msg.byteLength' bytes")
  assert(mac.byteLength === crypto_secretbox_MACBYTES, "mac must be 'crypto_secretbox_MACBYTES' bytes")
  assert(n.byteLength === crypto_secretbox_NONCEBYTES, "n must be 'crypto_secretbox_NONCEBYTES' bytes")
  assert(k.byteLength === crypto_secretbox_KEYBYTES, "k must be 'crypto_secretbox_KEYBYTES' bytes")

  const tmp = new Uint8Array(msg.byteLength + mac.byteLength)
  crypto_secretbox_easy(tmp, msg, n, k)
  mac.set(tmp.subarray(0, mac.byteLength))
  o.set(tmp.subarray(mac.byteLength))
  return true
}

function crypto_secretbox_open_detached (msg, o, mac, n, k) {
  assert(o.byteLength === msg.byteLength, "o must be 'msg.byteLength' bytes")
  assert(mac.byteLength === crypto_secretbox_MACBYTES, "mac must be 'crypto_secretbox_MACBYTES' bytes")
  assert(n.byteLength === crypto_secretbox_NONCEBYTES, "n must be 'crypto_secretbox_NONCEBYTES' bytes")
  assert(k.byteLength === crypto_secretbox_KEYBYTES, "k must be 'crypto_secretbox_KEYBYTES' bytes")

  const tmp = new Uint8Array(o.byteLength + mac.byteLength)
  tmp.set(mac)
  tmp.set(o, mac.byteLength)
  return crypto_secretbox_open_easy(msg, tmp, n, k)
}

function crypto_secretbox_easy (o, msg, n, k) {
  assert(o.byteLength === msg.byteLength + crypto_secretbox_MACBYTES, "o must be 'msg.byteLength + crypto_secretbox_MACBYTES' bytes")
  assert(n.byteLength === crypto_secretbox_NONCEBYTES, "n must be 'crypto_secretbox_NONCEBYTES' bytes")
  assert(k.byteLength === crypto_secretbox_KEYBYTES, "k must be 'crypto_secretbox_KEYBYTES' bytes")

  const m = new Uint8Array(crypto_secretbox_ZEROBYTES + msg.byteLength)
  const c = new Uint8Array(m.byteLength)
  m.set(msg, crypto_secretbox_ZEROBYTES)
  crypto_secretbox(c, m, n, k)
  o.set(c.subarray(crypto_secretbox_BOXZEROBYTES))
}

function crypto_secretbox_open_easy (msg, box, n, k) {
  assert(box.byteLength === msg.byteLength + crypto_secretbox_MACBYTES, "box must be 'msg.byteLength + crypto_secretbox_MACBYTES' bytes")
  assert(n.byteLength === crypto_secretbox_NONCEBYTES, "n must be 'crypto_secretbox_NONCEBYTES' bytes")
  assert(k.byteLength === crypto_secretbox_KEYBYTES, "k must be 'crypto_secretbox_KEYBYTES' bytes")

  const c = new Uint8Array(crypto_secretbox_BOXZEROBYTES + box.byteLength)
  const m = new Uint8Array(c.byteLength)
  c.set(box, crypto_secretbox_BOXZEROBYTES)
  if (crypto_secretbox_open(m, c, n, k) === false) return false
  msg.set(m.subarray(crypto_secretbox_ZEROBYTES))
  return true
}
