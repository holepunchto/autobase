'use strict'

// Based on https://github.com/dchest/tweetnacl-js/blob/6dcbcaf5f5cbfd313f2dcfe763db35c828c8ff5b/nacl-fast.js.

// Ported in 2014 by Dmitry Chestnykh and Devi Mandiri.
// Public domain.
//
// Implementation derived from TweetNaCl version 20140427.
// See for details: http://tweetnacl.cr.yp.to/

forward(require('./randombytes'))
forward(require('./memory'))
forward(require('./helpers'))
forward(require('./crypto_verify'))
forward(require('./crypto_auth'))
forward(require('./crypto_box'))
forward(require('./crypto_generichash'))
forward(require('./crypto_hash'))
forward(require('./crypto_hash_sha256'))
forward(require('./crypto_kdf'))
forward(require('./crypto_kx'))
forward(require('./crypto_aead'))
forward(require('./crypto_onetimeauth'))
forward(require('./crypto_scalarmult'))
forward(require('./crypto_secretbox'))
forward(require('./crypto_shorthash'))
forward(require('./crypto_sign'))
forward(require('./crypto_stream'))
forward(require('./crypto_stream_chacha20'))

function forward (submodule) {
  Object.keys(submodule).forEach(function (prop) {
    module.exports[prop] = submodule[prop]
  })
}
