const Hypercore = require('hypercore')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const b4a = require('b4a')
const rrp = require('resolve-reject-promise')
const ReadyResource = require('ready-resource')

const { EncryptionDescriptor, ManifestData } = require('./messages.js')

const HypercoreEncryption = Hypercore.DefaultEncryption

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)
const hash = nonce.subarray(0, sodium.crypto_generichash_BYTES_MIN)

const [, NS_VIEW_BLOCK_KEY, NS_HASH_KEY] = crypto.namespace('autobase', 3)

class AutobaseEncryption {
  static PADDING = 8

  constructor (encryption) {
    this.encryption = encryption

    this.compat = null
    this.keys = null
    this.keysById = new Map()
  }

  get id () {
    return this.keys ? this.keys.id : -1
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
    if (this.id === this.encryption.id) return
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
    return getBlockKey(this.encryption.base.bootstrap, entropy, ctx.key)
  }

  async _ensureCompat (ctx) {
    if (!this.compat) this.compat = this.compatKeys(ctx)
  }

  compatKeys () {
    throw new Error('Compatability method is not specified')
  }

  _blockhash (padding, block) {
    sodium.crypto_generichash(hash, block, this.keys.hash)
    padding.set(hash.subarray(0, 8)) // copy first 8 bytes of hash
    hash.fill(0) // clear nonce buffer
  }

  async encrypt (index, block, fork, ctx) {
    if (this.isCompat(ctx, index)) {
      this._ensureCompat(ctx)
      return HypercoreEncryption.encrypt(index, block, fork, this.compat.block, this.compat.blinding)
    }

    await this.update(ctx)

    const padding = block.subarray(0, AutobaseEncryption.PADDING)
    block = block.subarray(AutobaseEncryption.PADDING)

    this._blockhash(padding, block)
    c.uint32.encode({ start: 4, end: 8, buffer: padding }, this.keys.id)

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    padding[0] = 1 // version in plaintext

    nonce.set(padding, 8, 16)

    // The combination of index, key id, fork id and block hash is very likely
    // to be unique for a given Hypercore and therefore our nonce is suitable
    encrypt(block, nonce, this.keys.block)
  }

  async decrypt (index, block, ctx) {
    if (this.isCompat(ctx, index)) {
      this._ensureCompat(ctx)
      return HypercoreEncryption.decrypt(index, block, this.compat.block)
    }

    const padding = block.subarray(0, AutobaseEncryption.PADDING)
    block = block.subarray(AutobaseEncryption.PADDING)

    if (padding[0] === 0) return block // unencrypted

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
    const { encryption } = c.decode(ManifestData, ctx.userData)
    return !!encryption && index < encryption.legacyBlocks
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
    return this.core ? this.core.length : -1
  }

  _createPayload (key) {
    return key
  }

  async reload (core) {
    if (this.core) await this.core.close()

    this.core = core
    await this.core.ready()

    if (this._initialising) {
      this._initialising.resolve()
      this._initialising = null
    }
  }

  unpack (type, payload) {
    if (type > 0) throw new Error('Unsupported version')
    return payload
  }

  async update (key) {
    const payload = await this._createPayload(key)
    const desc = { type: 0, version: 0, payload }

    await this.core.append(c.encode(EncryptionDescriptor, desc))
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

  blockKey (entropy, ctx) {
    return getBlockKey(this.base.bootstrap, entropy, ctx.key)
  }

  async get (encryptionId) {
    if (!this.core) await this.initialised()

    if (encryptionId === -1) return null

    if (encryptionId === 0) return this.base.encryptionKey

    const index = encryptionId - 1
    const desc = await this.core.get(index)
    const { type, payload } = c.decode(EncryptionDescriptor, desc)

    const key = this.unpack(type, payload)
    return key
  }

  static getBlockKey (bootstrap, encryptionKey, hypercoreKey) {
    return getBlockKey(bootstrap, encryptionKey, hypercoreKey)
  }

  static getSystemEncryption (base, core) {
    const view = new EncryptionView(base, core)
    return view.getViewEncryption('_system')
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

function getBlockKey (bootstrap, encryptionKey, hypercoreKey) {
  if (typeof name === 'string') return getBlockKey(bootstrap, encryptionKey, hypercoreKey)
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, hypercoreKey])
}

function getCompatBlockKey (bootstrap, encryptionKey, name) {
  if (typeof name === 'string') return getBlockKey(bootstrap, encryptionKey, b4a.from(name))
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, name])
}
