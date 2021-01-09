/* eslint-disable camelcase */
const { crypto_stream_chacha20_ietf, crypto_stream_chacha20_ietf_xor_ic } = require('./crypto_stream_chacha20')
const { crypto_verify_16 } = require('./crypto_verify')
const Poly1305 = require('./internal/poly1305')
const assert = require('nanoassert')

const crypto_aead_chacha20poly1305_ietf_KEYBYTES = 32
const crypto_aead_chacha20poly1305_ietf_NSECBYTES = 0
const crypto_aead_chacha20poly1305_ietf_NPUBBYTES = 12
const crypto_aead_chacha20poly1305_ietf_ABYTES = 16
const crypto_aead_chacha20poly1305_ietf_MESSAGEBYTES_MAX = Number.MAX_SAFE_INTEGER

const _pad0 = new Uint8Array(16)

function crypto_aead_chacha20poly1305_ietf_encrypt (c, m, ad, nsec, npub, k) {
  if (ad === null) return crypto_aead_chacha20poly1305_ietf_encrypt(c, m, new Uint8Array(0), nsec, npub, k)

  assert(c.byteLength === m.byteLength + crypto_aead_chacha20poly1305_ietf_ABYTES,
    "ciphertext should be 'crypto_aead_chacha20poly1305_ietf_ABYTES' longer than message")
  assert(npub.byteLength === crypto_aead_chacha20poly1305_ietf_NPUBBYTES,
    "npub should be 'crypto_aead_chacha20poly1305_ietf_NPUBBYTES' long")
  assert(k.byteLength === crypto_aead_chacha20poly1305_ietf_KEYBYTES,
    "k should be 'crypto_aead_chacha20poly1305_ietf_KEYBYTES' long")
  assert(m.byteLength <= crypto_aead_chacha20poly1305_ietf_MESSAGEBYTES_MAX, 'message is too large')

  const ret = crypto_aead_chacha20poly1305_ietf_encrypt_detached(c.subarray(0, m.byteLength),
    c.subarray(m.byteLength), m, ad, nsec, npub, k)

  return m.byteLength + ret
}

function crypto_aead_chacha20poly1305_ietf_encrypt_detached (c, mac, m, ad, nsec, npub, k) {
  if (ad === null) return crypto_aead_chacha20poly1305_ietf_encrypt_detached(c, mac, m, new Uint8Array(0), nsec, npub, k)

  assert(c.byteLength === m.byteLength, 'ciphertext should be same length than message')
  assert(npub.byteLength === crypto_aead_chacha20poly1305_ietf_NPUBBYTES,
    "npub should be 'crypto_aead_chacha20poly1305_ietf_NPUBBYTES' long")
  assert(k.byteLength === crypto_aead_chacha20poly1305_ietf_KEYBYTES,
    "k should be 'crypto_aead_chacha20poly1305_ietf_KEYBYTES' long")
  assert(m.byteLength <= crypto_aead_chacha20poly1305_ietf_MESSAGEBYTES_MAX, 'message is too large')
  assert(mac.byteLength <= crypto_aead_chacha20poly1305_ietf_ABYTES,
    "mac should be 'crypto_aead_chacha20poly1305_ietf_ABYTES' long")

  const block0 = new Uint8Array(64)
  var slen = new Uint8Array(8)

  crypto_stream_chacha20_ietf(block0, npub, k)
  const poly = new Poly1305(block0)
  block0.fill(0)

  poly.update(ad, 0, ad.byteLength)
  poly.update(_pad0, 0, (0x10 - ad.byteLength) & 0xf)

  crypto_stream_chacha20_ietf_xor_ic(c, m, npub, 1, k)

  poly.update(c, 0, m.byteLength)
  poly.update(_pad0, 0, (0x10 - m.byteLength) & 0xf)

  write64LE(slen, 0, ad.byteLength)
  poly.update(slen, 0, slen.byteLength)

  write64LE(slen, 0, m.byteLength)
  poly.update(slen, 0, slen.byteLength)

  poly.finish(mac, 0)
  slen.fill(0)

  return crypto_aead_chacha20poly1305_ietf_ABYTES
}

function crypto_aead_chacha20poly1305_ietf_decrypt (m, nsec, c, ad, npub, k) {
  if (ad === null) return crypto_aead_chacha20poly1305_ietf_decrypt(m, nsec, c, new Uint8Array(0), npub, k)

  assert(m.byteLength === c.byteLength - crypto_aead_chacha20poly1305_ietf_ABYTES,
    "message should be 'crypto_aead_chacha20poly1305_ietf_ABYTES' shorter than ciphertext")
  assert(npub.byteLength === crypto_aead_chacha20poly1305_ietf_NPUBBYTES,
    "npub should be 'crypto_aead_chacha20poly1305_ietf_NPUBBYTES' long")
  assert(k.byteLength === crypto_aead_chacha20poly1305_ietf_KEYBYTES,
    "k should be 'crypto_aead_chacha20poly1305_ietf_KEYBYTES' long")
  assert(m.byteLength <= crypto_aead_chacha20poly1305_ietf_MESSAGEBYTES_MAX, 'message is too large')

  if (c.byteLength < crypto_aead_chacha20poly1305_ietf_ABYTES) throw new Error('could not verify data')

  crypto_aead_chacha20poly1305_ietf_decrypt_detached(
    m, nsec,
    c.subarray(0, c.byteLength - crypto_aead_chacha20poly1305_ietf_ABYTES),
    c.subarray(c.byteLength - crypto_aead_chacha20poly1305_ietf_ABYTES),
    ad, npub, k)

  return c.byteLength - crypto_aead_chacha20poly1305_ietf_ABYTES
}

function crypto_aead_chacha20poly1305_ietf_decrypt_detached (m, nsec, c, mac, ad, npub, k) {
  if (ad === null) return crypto_aead_chacha20poly1305_ietf_decrypt_detached(m, nsec, c, mac, new Uint8Array(0), npub, k)

  assert(c.byteLength === m.byteLength, 'message should be same length than ciphertext')
  assert(npub.byteLength === crypto_aead_chacha20poly1305_ietf_NPUBBYTES,
    "npub should be 'crypto_aead_chacha20poly1305_ietf_NPUBBYTES' long")
  assert(k.byteLength === crypto_aead_chacha20poly1305_ietf_KEYBYTES,
    "k should be 'crypto_aead_chacha20poly1305_ietf_KEYBYTES' long")
  assert(m.byteLength <= crypto_aead_chacha20poly1305_ietf_MESSAGEBYTES_MAX, 'message is too large')
  assert(mac.byteLength <= crypto_aead_chacha20poly1305_ietf_ABYTES,
    "mac should be 'crypto_aead_chacha20poly1305_ietf_ABYTES' long")

  const block0 = new Uint8Array(64)
  const slen = new Uint8Array(8)
  const computed_mac = new Uint8Array(crypto_aead_chacha20poly1305_ietf_ABYTES)

  crypto_stream_chacha20_ietf(block0, npub, k)
  const poly = new Poly1305(block0)
  block0.fill(0)

  poly.update(ad, 0, ad.byteLength)
  poly.update(_pad0, 0, (0x10 - ad.byteLength) & 0xf)

  const mlen = c.byteLength
  poly.update(c, 0, mlen)
  poly.update(_pad0, 0, (0x10 - mlen) & 0xf)

  write64LE(slen, 0, ad.byteLength)
  poly.update(slen, 0, slen.byteLength)

  write64LE(slen, 0, mlen)
  poly.update(slen, 0, slen.byteLength)

  poly.finish(computed_mac, 0)

  assert(computed_mac.byteLength === 16)
  const ret = crypto_verify_16(computed_mac, 0, mac, 0)

  computed_mac.fill(0)
  slen.fill(0)

  if (!ret) {
    m.fill(0)
    throw new Error('could not verify data')
  }

  crypto_stream_chacha20_ietf_xor_ic(m, c, npub, 1, k)
}

function write64LE (buf, offset, int) {
  buf.fill(0, 0, 8)

  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  view.setUint32(offset, int & 0xffffffff, true)
  view.setUint32(offset + 4, (int / 2 ** 32) & 0xffffffff, true)
}

module.exports = {
  crypto_aead_chacha20poly1305_ietf_encrypt,
  crypto_aead_chacha20poly1305_ietf_encrypt_detached,
  crypto_aead_chacha20poly1305_ietf_decrypt,
  crypto_aead_chacha20poly1305_ietf_decrypt_detached,
  crypto_aead_chacha20poly1305_ietf_ABYTES,
  crypto_aead_chacha20poly1305_ietf_KEYBYTES,
  crypto_aead_chacha20poly1305_ietf_NPUBBYTES,
  crypto_aead_chacha20poly1305_ietf_NSECBYTES,
  crypto_aead_chacha20poly1305_ietf_MESSAGEBYTES_MAX
}
