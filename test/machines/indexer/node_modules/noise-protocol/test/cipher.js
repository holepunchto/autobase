/* eslint-disable camelcase */
const {
  crypto_aead_chacha20poly1305_ietf_KEYBYTES,
  crypto_aead_chacha20poly1305_ietf_NPUBBYTES,
  crypto_aead_chacha20poly1305_ietf_ABYTES
} = require('sodium-universal/crypto_aead')
const { randombytes_buf } = require('sodium-universal/randombytes')
var cipher = require('../cipher')
var test = require('tape')

test('constants', function (assert) {
  assert.ok(cipher.KEYLEN === 32, 'KEYLEN conforms to Noise Protocol')
  assert.ok(cipher.NONCELEN === 8, 'NONCELEN conforms to Noise Protocol')
  assert.ok(cipher.MACLEN === 16, 'MACLEN conforms to Noise Protocol')

  assert.ok(cipher.KEYLEN === crypto_aead_chacha20poly1305_ietf_KEYBYTES, 'KEYLEN')
  assert.ok(cipher.NONCELEN + 4 === crypto_aead_chacha20poly1305_ietf_NPUBBYTES, 'NONCELEN')
  assert.ok(cipher.MACLEN === crypto_aead_chacha20poly1305_ietf_ABYTES, 'MACLEN')

  assert.end()
})

test('identity', function (assert) {
  var key = Buffer.alloc(cipher.KEYLEN)
  var nonce = Buffer.alloc(cipher.NONCELEN)
  randombytes_buf(key)
  randombytes_buf(nonce)

  var key2 = Buffer.alloc(cipher.KEYLEN)
  var nonce2 = Buffer.alloc(cipher.NONCELEN)
  randombytes_buf(key2)
  randombytes_buf(nonce2)

  var plaintext = Buffer.from('Hello world')
  var ciphertext = Buffer.alloc(plaintext.byteLength + cipher.MACLEN)
  var decrypted = Buffer.alloc(plaintext.byteLength)

  cipher.encrypt(ciphertext, key, nonce, null, plaintext)

  assert.throws(_ => cipher.decrypt(decrypted, key, nonce, Buffer.alloc(1), ciphertext), 'should not have ad')
  assert.throws(_ => cipher.decrypt(decrypted, key2, nonce, null, ciphertext), 'wrong key')
  assert.throws(_ => cipher.decrypt(decrypted, key, nonce2, null, ciphertext), 'wrong nonce')

  for (var i = 0; i < ciphertext.length; i++) {
    ciphertext[i] ^= i + 1
    assert.throws(_ => cipher.decrypt(decrypted, key, nonce, null, ciphertext))
    ciphertext[i] ^= i + 1
  }

  cipher.decrypt(decrypted, key, nonce, null, ciphertext)

  assert.ok(decrypted.equals(plaintext))
  assert.end()
})

test('identity with ad', function (assert) {
  var key = Buffer.alloc(cipher.KEYLEN)
  var nonce = Buffer.alloc(cipher.NONCELEN)
  randombytes_buf(key)
  randombytes_buf(nonce)

  var ad = Buffer.from('version 0')

  var key2 = Buffer.alloc(cipher.KEYLEN)
  var nonce2 = Buffer.alloc(cipher.NONCELEN)
  randombytes_buf(key2)
  randombytes_buf(nonce2)

  var plaintext = Buffer.from('Hello world')
  var ciphertext = Buffer.alloc(plaintext.byteLength + cipher.MACLEN)
  var decrypted = Buffer.alloc(plaintext.byteLength)

  cipher.encrypt(ciphertext, key, nonce, ad, plaintext)

  assert.throws(_ => cipher.decrypt(decrypted, key, nonce, Buffer.alloc(1), ciphertext), 'should not have ad')
  assert.throws(_ => cipher.decrypt(decrypted, key2, nonce, ad, ciphertext), 'wrong key')
  assert.throws(_ => cipher.decrypt(decrypted, key, nonce2, ad, ciphertext), 'wrong nonce')

  for (var i = 0; i < ciphertext.length; i++) {
    ciphertext[i] ^= 255
    assert.throws(_ => cipher.decrypt(decrypted, key, nonce, ad, ciphertext))
    ciphertext[i] ^= 255
  }

  cipher.decrypt(decrypted, key, nonce, ad, ciphertext)

  assert.ok(decrypted.equals(plaintext))
  assert.end()
})

test('rekey', function (assert) {
  var key = Buffer.alloc(cipher.KEYLEN)
  var nonce = Buffer.alloc(cipher.NONCELEN)
  randombytes_buf(key)
  randombytes_buf(nonce)

  var keyCopy = Buffer.from(key)
  cipher.rekey(key, key)
  assert.notOk(key.equals(keyCopy))

  var plaintext = Buffer.from('Hello world')
  var ciphertext = Buffer.alloc(plaintext.byteLength + cipher.MACLEN)
  var decrypted = Buffer.alloc(plaintext.byteLength)

  cipher.encrypt(ciphertext, key, nonce, null, plaintext)

  assert.throws(_ => cipher.decrypt(decrypted, keyCopy, nonce, null, ciphertext), 'wrong key')

  cipher.rekey(keyCopy, keyCopy)
  cipher.decrypt(decrypted, keyCopy, nonce, null, ciphertext)

  assert.ok(decrypted.equals(plaintext))
  assert.end()
})
