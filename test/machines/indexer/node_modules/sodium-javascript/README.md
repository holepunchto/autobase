# `sodium-javascript`

[![Build Status](https://travis-ci.org/sodium-friends/sodium-javascript.svg?branch=master)](https://travis-ci.org/sodium-friends/sodium-javascript)

> WIP - a pure javascript version of [sodium-native](https://github.com/sodium-friends/sodium-native).
Based on tweetnacl

## Usage

``` js
const sodium = require('sodium-javascript')

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
```

## API

See [sodium-native](https://github.com/sodium-friends/sodium-native).
This is a work in progress so not all functions are implemented yet.

This module is organised into individual submodules which can be required
independently for smaller bundles in the browser. To leverage automatic
switching between `sodium-javascript` and `sodium-native`, see
[`sodium-universal`](https://github.com/sodium-friends/sodium-universal).

## Install

```
npm install sodium-javascript
```

## License

[MIT](LICENSE)
