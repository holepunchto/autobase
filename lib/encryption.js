const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const HypercoreEncryption = require('hypercore-encryption')
const ReadyResource = require('ready-resource')

const [, NS_VIEW_BLOCK_KEY, NS_DERIVED_KEY, NS_BLINDING_KEY] = crypto.namespace('autobase', 4)

module.exports = class ViewEncryption extends ReadyResource {
  constructor (base, core) {
    super()

    this.base = base
    this.core = core || null

    this.encryptionKey = base.encryptionKey

    this.id = 0

    this.sessions = new Map()
    this.keys = new Map()
  }

  async _open () {
    if (this.core) await this.core.ready()
  }

  _createPayload (key) {
    return key
  }

  async reload (core) {
    await core.ready()
    this.core = core
  }

  async _refresh () {
    for (const enc of this.sessions.values()) {
      await enc.load(this.id)
    }
  }

  async update (id) {
    this.id = id
    await this._refresh()
  }

  get (name) {
    if (this.sessions.has(name)) return this.sessions.get(name)

    const blindingKey = this._blindingKey(name)
    const get = this._getKey.bind(this, name)

    const encryption = new HypercoreEncryption(blindingKey, get, { id: this.id })

    this.sessions.set(name, encryption)

    return encryption
  }

  async _getKey (name, encryptionId) {
    if (encryptionId === 0) {
      return this._defaultEncryption(name)
    }

    if (this.keys.has(encryptionId)) {
      const key = this.keys.has(encryptionId)
      return this._keyInfo(key, name)
    }

    const entropy = await this._getEntropy(encryptionId)
    const key = this._generateKey(entropy)

    this.keys.set(encryptionId, key)

    return this._keyInfo(key, name)
  }

  // TODO: replace with bespoke key rotation scheme
  async _getEntropy (encryptionId) {
    return this.core.treeHash(encryptionId)
  }

  _generateEncryptionKey (entropy) {
    return crypto.hash([NS_DERIVED_KEY, this.encryptionKey, entropy])
  }

  _defaultEncryption (name) {
    return this._keyInfo(this.encryptionKey, name)
  }

  _keyInfo (encryptionKey, name) {
    return {
      version: 1,
      padding: 16,
      key: getBlockKey(this.base.bootstrap, this.encryptionKey, name)
    }
  }

  _blindingKey (name) {
    return crypto.hash([NS_BLINDING_KEY, this.base.bootstrap, this.encryptionKey, b4a.from(name)])
  }

  static getBlockKey (bootstrap, encryptionKey, name) {
    return getBlockKey(bootstrap, encryptionKey, name)
  }
}

function getBlockKey (bootstrap, encryptionKey, name) {
  if (typeof name === 'string') return getBlockKey(bootstrap, encryptionKey, b4a.from(name))
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, name])
}
