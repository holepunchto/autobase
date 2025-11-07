const DefaultEncryption = require('hypercore/lib/default-encryption.js')
const HypercoreEncryption = require('hypercore-encryption')
const BroadcastEncryption = require('@holepunchto/broadcast-encryption')
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

module.exports = class EncryptionView extends ReadyResource {
  constructor(base, core, { bootstrap = null } = {}) {
    super()

    this.base = base
    this.core = core || null

    this.broadcast = null
    this.encryption = new HypercoreEncryption(this.get.bind(this))

    this._bootstrap = bootstrap
    this._initialising = null
  }

  async _open() {
    await this.initialised()
    await this.load(this.core, this._bootstrap)
  }

  // autobase opens the view before the core
  initialised() {
    if (this.core !== null) return this.core.ready()
    if (this._initialising) return this._initialising

    this._initialising = rrp()

    return this._initialising.promise
  }

  _close() {
    if (this._initialising) {
      this._initialising.reject(new Error('Encryption closed'))
      this._initialising = null
    }

    if (this.core) return this.core.close()
  }

  get id() {
    return this.broadcast ? this.broadcast.id : -1
  }

  async load(core, bootstrap = this._bootstrap) {
    if (this.core === core && this.broadcast) return

    if (this.core !== core) {
      if (this.broadcast) await this.broadcast.close()
      if (this.core) await this.core.close()

      this.core = core
    }

    this.broadcast = new BroadcastEncryption(this.core, {
      keyPair: this.base.local.keyPair,
      bootstrap
    })

    await this.broadcast.ready()

    if (this._initialising) {
      this._initialising.resolve()
      this._initialising = null
    }
  }

  async get(id, opts) {
    await this.ready()
    const desc = await this.broadcast.get(id, opts)

    if (desc.id) return desc

    return {
      id: 0,
      encryptionKey: SystemView.GENESIS_ENTROPY
    }
  }

  bootstrap(info) {
    this.broadcast.bootstrap(info)
  }

  getBootstrap() {
    return this.broadcast.getBootstrap()
  }

  async update(payload) {
    await this.broadcast.append(payload)
  }

  getViewEncryption(name) {
    return this.encryption.createEncryptionProvider({
      transform: generateViewEncryptionKey.bind(this, name),
      compat: isViewCompat
    })
  }

  getWriterEncryption() {
    return this.encryption.createEncryptionProvider({
      transform: generateWriterEncryptionKey.bind(this),
      compat: isWriterCompat
    })
  }

  async encryptAnchor(block, namespace) {
    if (!this.base.isEncrypted) return null

    const { id, entropy } = await this.get(-1)
    if (!entropy) return null

    const blockKey = this.blockKey(entropy, { key: namespace })
    const hashKey = crypto.hash([NS_HASH_KEY, block])

    encryptBlock(0, block, id, blockKey, hashKey)
  }

  blockKey(entropy, ctx) {
    if (entropy === null) entropy = SystemView.GENESIS_ENTROPY
    return getBlockKey(this.base.bootstrap, this.base.encryptionKey, entropy, ctx.key)
  }

  getSystemEncryption() {
    return this.getViewEncryption('_system')
  }

  static namespace(entropy) {
    return crypto.hash([NS_ENCRYPTION, entropy])
  }

  static getBlockKey(bootstrap, encryptionKey, entropy, hypercoreKey) {
    return getBlockKey(bootstrap, encryptionKey, entropy, hypercoreKey)
  }

  static getSystemEncryption(base, core) {
    const view = new EncryptionView(base, core)
    return view.getSystemEncryption()
  }

  static async setSystemEncryption(base, core, opts) {
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

function encrypt(block, nonce, key) {
  sodium.crypto_stream_xor(block, block, nonce, key)
}

function getBlockKey(bootstrap, encryptionKey, entropy, hypercoreKey) {
  return (
    encryptionKey &&
    crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, entropy, hypercoreKey])
  )
}

function getCompatBlockKey(bootstrap, encryptionKey, name) {
  if (typeof name === 'string') return getCompatBlockKey(bootstrap, encryptionKey, b4a.from(name))
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, name])
}

function blockhash(block, padding, hashKey) {
  sodium.crypto_generichash(hash, block, hashKey)
  padding.set(hash.subarray(0, 8)) // copy first 8 bytes of hash
  hash.fill(0) // clear nonce buffer
}

function encryptBlock(index, block, id, blockKey, hashKey) {
  const padding = block.subarray(0, HypercoreEncryption.PADDING)
  block = block.subarray(HypercoreEncryption.PADDING)

  blockhash(block, padding, hashKey)
  c.uint32.encode({ start: 4, end: 8, buffer: padding }, id)

  c.uint64.encode({ start: 0, end: 8, buffer: nonce }, index)

  padding[0] = 1 // version in plaintext

  nonce.set(padding, 8, 16)

  // The combination of index, key id, fork id and block hash is very likely
  // to be unique for a given Hypercore and therefore our nonce is suitable
  encrypt(block, nonce, blockKey)
}

function generateViewEncryptionKey(name, ctx, entropy, compat) {
  // compat key
  if (compat) {
    const block = getCompatBlockKey(this.base.bootstrap, this.base.encryptionKey, name)
    return {
      block,
      blinding: crypto.hash(block)
    }
  }

  const block = getCompatBlockKey(entropy, this.base.encryptionKey, name)
  const hash = crypto.hash([NS_HASH_KEY, block])

  return {
    block,
    hash
  }
}

function generateWriterEncryptionKey(ctx, entropy, compat) {
  // compat key
  if (compat) {
    const compat = DefaultEncryption.deriveKeys(this.base.encryptionKey, ctx.key)
    return {
      block: compat.block,
      blinding: compat.blinding
    }
  }

  if (ctx.manifest.userData) {
    const userData = c.decode(ManifestData, ctx.manifest.userData)
    if (userData.namespace !== null) {
      const block = this.blockKey(entropy, { key: userData.namespace })
      const hash = crypto.hash([NS_HASH_KEY, block])

      return {
        block,
        hash
      }
    }
  }

  const block = this.blockKey(entropy, ctx)
  const hash = crypto.hash([NS_HASH_KEY, block])

  return {
    block,
    hash
  }
}

function isViewCompat(ctx, index) {
  if (ctx.manifest.version <= 1) return true
  if (!ctx.manifest.userData) return false
  const { legacyBlocks } = c.decode(ManifestData, ctx.manifest.userData)
  return index < legacyBlocks
}

function isWriterCompat(ctx, index) {
  return ctx.manifest.version <= 1
}
