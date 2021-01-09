const sodium = require('./')

const key = Buffer.alloc(sodium.crypto_secretbox_KEYBYTES)
const nonce = Buffer.alloc(sodium.crypto_secretbox_NONCEBYTES)

sodium.randombytes_buf(key)
sodium.randombytes_buf(nonce)

const message = Buffer.from('Hello, World!')
const cipher = Buffer.alloc(message.length + sodium.crypto_secretbox_MACBYTES)

sodium.crypto_secretbox_easy(cipher, message, nonce, key)

console.log('Encrypted:', cipher)

const plainText = Buffer.alloc(cipher.length - sodium.crypto_secretbox_MACBYTES)

sodium.crypto_secretbox_open_easy(plainText, cipher, nonce, key)

console.log('Plaintext:', plainText.toString())

if (typeof window !== 'undefined') window.close()
