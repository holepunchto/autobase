const c = require('compact-encoding')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const HypercoreEncryption = require('hypercore-block-encryption')
const ReadyResource = require('ready-resource')

const { EncryptionDescriptor } = require('./messages.js')

const [, NS_VIEW_BLOCK_KEY] = crypto.namespace('autobase', 2)

module.exports = class AutobaseEncryption extends ReadyResource {
  constructor (base, core) {
    super()

    this.base = base
    this.core = core

    this.sessions = new Map()
    this.keys = new Map()
  }

  async _open () {
    if (this.core) await this.core.ready()
  }

  _close () {
    if (this.core) return this.core.close()
  }

  get bootstrapped () {
    return !!(this.core && this.core.length > 0)
  }

  _createPayload (key) {
    return key
  }

  async _refresh () {
    const id = this.id()
    if (id === 0) return

    for (const enc of this.sessions.values()) {
      await enc.load(id)
    }
  }

  async reload (core) {
    if (this.core) await this.core.close()

    this.core = core
    await this.core.ready()

    await this._refresh()
  }

  id () {
    return this.core ? this.core.length : 0
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

    await this._refresh()

    return key
  }

  get (name) {
    if (this.sessions.has(name)) return this.sessions.get(name)

    const encryption = new HypercoreEncryption({
      id: this.id(),
      get: this._getKey.bind(this, name)
    })

    this.sessions.set(name, encryption)
    return encryption
  }

  async _getKey (name, encryptionId) {
    if (encryptionId === 0) encryptionId = 1 // hack for now (first key is same as static key)

    if (!this.keys.has(encryptionId)) {
      const index = encryptionId - 1
      const desc = await this.core.get(index)
      const { type, payload } = c.decode(EncryptionDescriptor, desc)

      const key = this.unpack(type, payload)
      this.keys.set(encryptionId, key)
    }

    const key = this.keys.get(encryptionId)
    return getBlockKey(this.base.bootstrap, key, name)
  }
}

function getBlockKey (bootstrap, encryptionKey, name) {
  if (typeof name === 'string') return getBlockKey(bootstrap, encryptionKey, b4a.from(name))
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, name])
}
