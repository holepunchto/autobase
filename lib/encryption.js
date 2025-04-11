const Hypercore = require('hypercore')
const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const c = require('compact-encoding')
const b4a = require('b4a')
const rrp = require('resolve-reject-promise')
const ReadyResource = require('ready-resource')

const { EncryptionDescriptor } = require('./messages.js')

const HypercoreEncryption = Hypercore.DefaultEncryption

const nonce = b4a.alloc(sodium.crypto_stream_NONCEBYTES)
const hash = nonce.subarray(0, sodium.crypto_generichash_BYTES_MIN)

const [NS_HASH_KEY] = crypto.namespace('autobase/encryption', 1)
const [, NS_VIEW_BLOCK_KEY] = crypto.namespace('autobase', 2)

class WriterEncryption {
  static PADDING = 8

  constructor (base, opts) {
    this.base = base

    this.classic = false
    this.keys = null
    this.hashKey = null
  }

  padding (context) {
    return context.manifest.version <= 1 ? HypercoreEncryption.PADDING : WriterEncryption.PADDING
  }

  _load (context) {
    this.keys = HypercoreEncryption.deriveKeys(this.base.encryptionKey, context.key)
    this.classic = context.manifest.version <= 1

    if (!this.classic) this.hashKey = crypto.hash([NS_HASH_KEY, this.keys.block])
  }

  // Padding is blockHash and key id
  _encodePadding (padding, block, keyId) {
    sodium.crypto_generichash(hash, block, this.hashKey)
    padding.set(hash.subarray(0, 8)) // copy first 8 bytes of hash

    c.uint64.encode({ start: 4, end: 8, buffer: padding }, keyId)
    hash.fill(0) // clear nonce buffer
  }

  encrypt (index, block, fork, context) {
    if (!this.keys) this._load(context)

    if (this.classic) {
      return HypercoreEncryption.encrypt(index, block, fork, this.keys.block, this.keys.blinding)
    }

    const padding = block.subarray(0, WriterEncryption.PADDING)
    block = block.subarray(WriterEncryption.PADDING)

    this._encodePadding(padding, block, 0)

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    // Blind block hash and key id
    encrypt(padding, nonce, this.keys.blinding)

    padding[0] = 1 // version in plaintext

    nonce.set(padding, 8, 16)

    // The combination of index, key id, fork id and block hash is very likely
    // to be unique for a given Hypercore and therefore our nonce is suitable
    encrypt(block, nonce, this.keys.block)
  }

  decrypt (index, block, context) {
    if (!this.keys) this._load(context)

    if (this.classic) {
      return HypercoreEncryption.decrypt(index, block, this.keys.block)
    }

    const padding = block.subarray(0, WriterEncryption.PADDING)
    block = block.subarray(WriterEncryption.PADDING)

    if (padding[0] === 0) return block // unencrypted

    c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

    nonce.set(padding, 8, 16)

    // Decrypt the block using the full nonce
    decrypt(block, nonce, this.keys.block)
  }
}

class AutobaseEncryption extends ReadyResource {
  constructor (base, core) {
    super()

    this.base = base
    this.core = core || null

    this.sessions = new Map()
    this.keys = new Map()

    this._initialising = null
  }

  async _open () {
    await this.initialised()
    await this.core.ready()
  }

  initialised () {
    if (this.core !== null) return Promise.resolve()
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

  _createPayload (key) {
    return key
  }

  async refresh () {
    const id = this.id()
    if (id === -1) return

    const promises = []
    for (const enc of this.sessions.values()) {
      promises.push(enc.load(id))
    }

    await Promise.all(promises)
  }

  async reload (core) {
    if (this.core) await this.core.close()

    this.core = core
    await this.core.ready()

    if (this._initialising) {
      this._initialising.resolve()
      this._initialising = null
    }

    await this.refresh()
  }

  id () {
    return this.core ? this.core.length : -1
  }

  unpack (type, payload) {
    if (type > 0) throw new Error('Unsupported version')
    return payload
  }

  async update (key) {
    const payload = await this._createPayload(key)
    const desc = { type: 0, version: 0, payload }

    await this.core.append(c.encode(EncryptionDescriptor, desc))

    this.keys.set(this.id(), key)

    await this.refresh()

    return key
  }

  async _preload () {
    await this.ready()
    return this.id()
  }

  get (name) {
    if (this.sessions.has(name)) return this.sessions.get(name)

    // todo: derive this properly
    const blindingKey = this._blindingKey(name)
    const get = this._getKey.bind(this, name)

    const encryption = new HypercoreEncryption(blindingKey, get, {
      preopen: this._preload()
    })

    this.sessions.set(name, encryption)
    return encryption
  }

  _blindingKey (name) {
    return crypto.hash(getBlockKey(this.base.bootstrap, this.base.encryptionKey, name))
  }

  async _getKey (name, encryptionId) {
    if (encryptionId === -1) return null

    // console.log(encryptionId, this.id())
    if (encryptionId === 0) return this.getLegacyEncryption(name)

    if (!this.keys.has(encryptionId)) {
      const index = encryptionId - 1
      const desc = await this.core.get(index)
      const { type, payload } = c.decode(EncryptionDescriptor, desc)

      const key = this.unpack(type, payload)
      this.keys.set(encryptionId, key)
    }

    const key = this.keys.get(encryptionId)

    return {
      version: 1,
      padding: 16,
      key: getBlockKey(this.base.bootstrap, key, name)
    }
  }

  getLegacyEncryption (name) {
    return {
      version: 0,
      padding: 8,
      key: getBlockKey(this.base.bootstrap, this.base.encryptionKey, name)
    }
  }

  static getBlockKey (bootstrap, encryptionKey, name) {
    return getBlockKey(bootstrap, encryptionKey, name)
  }

  static getSystemEncryption (base, core) {
    const view = new AutobaseEncryption(base, core)
    return view.get('_system')
  }

  static async setSystemEncryption (base, core, opts) {
    if (base.encryptionKey === null) return

    await core.ready()
    if (core.manifest.version === 1) {
      const key = getBlockKey(base.bootstrap, base.encryptionKey, '_system')
      return core.setEncryptionKey(key, { block: true })
    }

    if (!core.manifest.linked.length) {
      throw new Error('System manifest does not link encryption view')
    }

    const enc = base.store.get({ key: core.manifest.linked[0], active: false })
    const encryption = AutobaseEncryption.getSystemEncryption(base, enc)

    await core.setEncryption(encryption, opts)

    return enc
  }
}

module.exports = {
  WriterEncryption,
  AutobaseEncryption
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

function getBlockKey (bootstrap, encryptionKey, name) {
  if (typeof name === 'string') return getBlockKey(bootstrap, encryptionKey, b4a.from(name))
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, name])
}
