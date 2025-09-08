const HypercoreEncryption = require('hypercore/lib/default-encryption.js')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const b4a = require('b4a')
const rrp = require('resolve-reject-promise')
const ReadyResource = require('ready-resource')

const SystemView = require('./system.js')
const { ManifestData } = require('./messages.js')
const { NS_VIEW_BLOCK_KEY, NS_HASH_KEY, NS_ENCRYPTION } = require('./caps.js')

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)
const hash = nonce.subarray(0, sodium.crypto_generichash_BYTES_MIN)

class AutobaseEncryption {
  static PADDING = 8

  constructor (encryption) {
    this.encryption = encryption

    this.compat = null
    this.keys = null
    this.keysById = new Map()
  }

  get id () {
    return this.keys ? this.keys.id : 0
  }

  padding () {
    return AutobaseEncryption.PADDING
  }

  isCompat () {
    return false
  }

  load (keys) {
    if (this.keys === null) this.keys = keys
  }

  async update (ctx) {
    if (this.id !== 0 && this.id === this.encryption.id) return
    const keys = await this.get(this.encryption.id, ctx)
    if (keys) this.keys = keys
  }

  async get (id, ctx) {
    if (this.keysById.has(id)) return this.keysById.get(id)

    const keys = await this.getKeys(id, ctx)
    this.keysById.set(id, keys)

    return keys
  }

  async getKeys (id, ctx) {
    const entropy = await this.encryption.get(id)
    if (!entropy) return null

    const block = this.blockKey(entropy, ctx)
    const hash = crypto.hash([NS_HASH_KEY, block])

    return {
      id,
      block,
      hash
    }
  }

  blockKey (entropy, ctx) {
    return this.encryption.blockKey(entropy, ctx)
  }

  async _ensureCompat (ctx) {
    if (!this.compat) this.compat = this.compatKeys(ctx)
  }

  compatKeys () {
    throw new Error('Compatability method is not specified')
  }

  async encrypt (index, block, fork, ctx) {
    if (this.isCompat(ctx, index)) {
      this._ensureCompat(ctx)
      return HypercoreEncryption.encrypt(index, block, fork, this.compat.block, this.compat.blinding)
    }

    await this.update(ctx)

    encryptBlock(index, block, this.keys.id, this.keys.block, this.keys.hash)
  }

  async decrypt (index, block, ctx) {
    if (this.isCompat(ctx, index)) {
      this._ensureCompat(ctx)
      return HypercoreEncryption.decrypt(index, block, this.compat.block)
    }

    const padding = block.subarray(0, AutobaseEncryption.PADDING)
    block = block.subarray(AutobaseEncryption.PADDING)

    const type = padding[0]
    switch (type) {
      case 0:
        return block // unencrypted

      case 1:
        break

      default:
        throw new Error('Unrecognised encryption type')
    }

    const id = c.uint32.decode({ start: 4, end: 8, buffer: padding })

    const keys = await this.get(id, ctx)

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)
    nonce.set(padding, 8, 16)

    // Decrypt the block using the full nonce
    decrypt(block, nonce, keys.block)
  }
}

class ViewEncryption extends AutobaseEncryption {
  constructor (encryption, name) {
    super(encryption)
    this.name = name
  }

  isCompat (ctx, index) {
    if (ctx.manifest.version <= 1) return true
    if (!ctx.manifest.userData) return false
    const { legacyBlocks } = c.decode(ManifestData, ctx.manifest.userData)
    return index < legacyBlocks
  }

  compatKeys () {
    const { bootstrap, encryptionKey } = this.encryption.base
    const block = getCompatBlockKey(bootstrap, encryptionKey, this.name)
    return {
      block,
      blinding: crypto.hash(block)
    }
  }

  blockKey (entropy) {
    return getCompatBlockKey(this.encryption.base.bootstrap, entropy, this.name)
  }
}

class WriterEncryption extends AutobaseEncryption {
  isCompat (ctx) {
    return ctx.manifest.version <= 1
  }

  compatKeys (ctx) {
    return HypercoreEncryption.deriveKeys(this.encryption.base.encryptionKey, ctx.key)
  }

  blockKey (entropy, ctx) {
    if (ctx.manifest.userData) {
      const userData = c.decode(ManifestData, ctx.manifest.userData)
      if (userData.namespace !== null) {
        return this.encryption.blockKey(entropy, { key: userData.namespace })
      }
    }

    return this.encryption.blockKey(entropy, ctx)
  }
}

class EncryptionView extends ReadyResource {
  constructor (base, core) {
    super()

    this.base = base
    this.core = core || null

    this.sessions = new Map()

    this._initialising = null
  }

  async _open () {
    await this.initialised()
    await this.core.ready()
  }

  initialised () {
    if (this.core !== null) return this.core.ready()
    if (this._initialising) return this._initialising

    this._initialising = rrp()

    return this._initialising.promise
  }

  _close () {
    if (this._initialising) {
      this._initialising.reject(new Error('Encryption closed'))
      this._initialising = null
    }

    if (this.core) return this.core.close()
  }

  get bootstrapped () {
    return !!(this.core && this.core.length > 0)
  }

  get id () {
    return this.core ? this.core.length : 0
  }

  async reload (core) {
    if (this.core === core) return

    if (this.core) await this.core.close()

    this.core = core
    await this.core.ready()

    if (this._initialising) {
      this._initialising.resolve()
      this._initialising = null
    }
  }

  async update (payload) {
    await this.core.append(payload)
  }

  async _preload () {
    await this.ready()
    return this.id()
  }

  getViewEncryption (name) {
    if (this.sessions.has(name)) return this.sessions.get(name)

    const encryption = new ViewEncryption(this, name)
    this.sessions.set(name, encryption)

    return encryption
  }

  getWriterEncryption () {
    return new WriterEncryption(this)
  }

  async encryptAnchor (block, namespace) {
    const entropy = await this.get(this.id)
    if (!entropy) return null

    const blockKey = this.blockKey(entropy, { key: namespace })
    const hashKey = crypto.hash([NS_HASH_KEY, block])

    encryptBlock(0, block, this.id, blockKey, hashKey)
  }

  blockKey (entropy, ctx) {
    return getBlockKey(this.base.bootstrap, this.base.encryptionKey, entropy, ctx.key)
  }

  async get (encryptionId) {
    if (encryptionId === 0) return SystemView.GENESIS_ENTROPY

    if (!this.core) await this.initialised()

    const index = encryptionId - 1
    const payload = await this.core.get(index)

    if (this.base.broadcastEncryption) {
      return this.base.broadcastEncryption.unpack(payload)
    }

    return payload
  }

  getSystemEncryption () {
    return this.getViewEncryption('_system')
  }

  static namespace (entropy) {
    return crypto.hash([NS_ENCRYPTION, entropy])
  }

  static getBlockKey (bootstrap, encryptionKey, entropy, hypercoreKey) {
    return getBlockKey(bootstrap, encryptionKey, entropy, hypercoreKey)
  }

  static getSystemEncryption (base, core) {
    const view = new EncryptionView(base, core)
    return view.getSystemEncryption()
  }

  static async setSystemEncryption (base, core, opts) {
    if (base.encryptionKey === null) return null

    await core.ready()
    if (core.manifest.version === 1) {
      const key = getCompatBlockKey(base.bootstrap, base.encryptionKey, '_system')
      return core.setEncryptionKey(key, { block: true })
    }

    if (!core.manifest.linked.length) {
      throw new Error('System manifest does not link encryption view')
    }

    const enc = base.store.get({ key: core.manifest.linked[0], active: false })
    const encryption = EncryptionView.getSystemEncryption(base, enc)

    await core.setEncryption(encryption, opts)

    return enc
  }
}

module.exports = {
  AutobaseEncryption,
  EncryptionView
}

function encrypt (block, nonce, key) {
  sodium.crypto_stream_xor(
    block,
    block,
    nonce,
    key
  )
}

function decrypt (block, nonce, key) {
  return encrypt(block, nonce, key) // symmetric
}

function getBlockKey (bootstrap, encryptionKey, entropy, hypercoreKey) {
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, entropy, hypercoreKey])
}

function getCompatBlockKey (bootstrap, encryptionKey, name) {
  if (typeof name === 'string') return getCompatBlockKey(bootstrap, encryptionKey, b4a.from(name))
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, name])
}

function blockhash (block, padding, hashKey) {
  sodium.crypto_generichash(hash, block, hashKey)
  padding.set(hash.subarray(0, 8)) // copy first 8 bytes of hash
  hash.fill(0) // clear nonce buffer
}

function encryptBlock (index, block, id, blockKey, hashKey) {
  const padding = block.subarray(0, AutobaseEncryption.PADDING)
  block = block.subarray(AutobaseEncryption.PADDING)

  blockhash(block, padding, hashKey)
  c.uint32.encode({ start: 4, end: 8, buffer: padding }, id)

  c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

  padding[0] = 1 // version in plaintext

  nonce.set(padding, 8, 16)

  // The combination of index, key id, fork id and block hash is very likely
  // to be unique for a given Hypercore and therefore our nonce is suitable
  encrypt(block, nonce, blockKey)
}
