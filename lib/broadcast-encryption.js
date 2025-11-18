const b4a = require('b4a')
const c = require('compact-encoding')
const sodium = require('sodium-native')

const SystemView = require('./system')

// ephemeral state
const NONCE = b4a.alloc(sodium.crypto_box_NONCEBYTES)
const RECIPIENT_KEY = b4a.alloc(sodium.crypto_box_PUBLICKEYBYTES)
const SECRET_KEY = b4a.alloc(sodium.crypto_box_SECRETKEYBYTES)
const PUBLIC_KEY = b4a.alloc(sodium.crypto_box_PUBLICKEYBYTES)

const PayloadArray = c.array(c.buffer)

const EncryptionPayload = {
  preencode(state, m) {
    c.buffer.preencode(state, m.nonce)
    c.fixed32.preencode(state, m.publicKey)
    PayloadArray.preencode(state, m.payload)
  },
  encode(state, m) {
    c.buffer.encode(state, m.nonce)
    c.fixed32.encode(state, m.publicKey)
    PayloadArray.encode(state, m.payload)
  },
  decode(state) {
    return {
      nonce: c.buffer.decode(state),
      publicKey: c.fixed32.decode(state),
      payload: PayloadArray.decode(state)
    }
  }
}

module.exports = class BroadcastEncryption {
  constructor() {
    this.base = null

    this._sk = b4a.alloc(sodium.crypto_box_SECRETKEYBYTES)
  }

  load(base) {
    this.base = base
    sodium.crypto_sign_ed25519_sk_to_curve25519(this._sk, base.local.keyPair.secretKey)
  }

  async rotate() {
    const encryptionKey = b4a.alloc(32)
    sodium.randombytes_buf(encryptionKey)

    const recipients = await this.getRecipients()

    return this.pack(encryptionKey, recipients)
  }

  async getRecipients() {
    const recipients = []
    for (const { key } of await SystemView.list(this.base.core, { onlyActive: true })) {
      const core = this.base.store.get({ key, active: false })
      await core.ready()

      recipients.push(core.manifest.signers[0].publicKey)
    }

    return recipients
  }

  unpack(data) {
    const { nonce, publicKey, payload } = c.decode(EncryptionPayload, data)

    const key = b4a.alloc(32)

    for (const ciphertext of payload) {
      try {
        sodium.crypto_box_open_easy(key, ciphertext, nonce, this._sk, publicKey)
        return key
      } catch (err) {
        console.error(err) // for debugging
        continue
      }
    }
  }

  pack(key, recipients) {
    sodium.crypto_box_keypair(SECRET_KEY, PUBLIC_KEY)
    sodium.crypto_generichash_batch(NONCE, [PUBLIC_KEY, b4a.from([this.base.core.length])])

    const payload = {
      publicKey: PUBLIC_KEY,
      nonce: NONCE,
      payload: []
    }

    for (const recipient of recipients) {
      const enc = b4a.alloc(key.byteLength + sodium.crypto_box_MACBYTES)

      sodium.crypto_sign_ed25519_pk_to_curve25519(RECIPIENT_KEY, recipient)
      sodium.crypto_box_easy(enc, key, NONCE, SECRET_KEY, RECIPIENT_KEY)

      payload.payload.push(enc)
    }

    return c.encode(EncryptionPayload, payload)
  }
}
