const b4a = require('b4a')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')

const Autocore = require('./core')
const Signer = require('./signer')

const EMPTY = b4a.alloc(0)
const [NS_SIGNER_NAMESPACE, NS_VIEW_BLOCK_KEY] = crypto.namespace('autobase', 2)

module.exports = class AutoStore {
  constructor (base, indexers) {
    this.base = base
    this.opened = new Map()
    this.coresByKey = new Map()
    this.coresByIndex = new Map()
    this.waiting = []

    this._queueFastForward = this.base.queueFastForward.bind(this.base)
  }

  get (opts, moreOpts) {
    if (typeof opts === 'string') opts = { name: opts }
    if (moreOpts) opts = { ...opts, ...moreOpts, compat: false }

    const name = opts.name
    const valueEncoding = opts.valueEncoding || null

    const indexers = this.base.linearizer ? this.base.linearizer.indexers.slice() : []

    const core = indexers.length
      ? this.base.store.get(this._corePreload(indexers, name, opts.cache))
      : this.base.store.get({
        preload: async () => {
          await Promise.resolve()
          if (!this.base.opening) throw new Error('Autobase failed to open')

          await this.base._presystem

          ac._indexers = this.base.linearizer.indexers.length
          if (ac.indexers.length === 1 && !ac.indexers[0].core.manifest) {
            ac._indexers = 0
            ac.queued = 0
          }

          return this._corePreload(ac.indexers, name, opts.cache)
        }
      })

    if (name === '_system') core.on('append', this._queueFastForward)

    const ac = new Autocore(this.base, core, name, indexers, opts)

    ac.signer = new Signer(this.base, ac)

    this.opened.set(name, ac)

    this.waiting.push(ac)

    return ac.createSession(valueEncoding)
  }

  _corePreload (indexers, name, cache) {
    return {
      manifest: this._createManifest(indexers, name),
      cache,
      exclusive: true,
      compat: false,
      encryptionKey: this.getBlockKey(name),
      isBlockKey: true
    }
  }

  static getBlockKey (bootstrap, encryptionKey, name) {
    return getBlockKey(bootstrap, encryptionKey, name)
  }

  getBlockKey (name) {
    return getBlockKey(this.base.bootstrap, this.base.encryptionKey, name)
  }

  getCore (ac, indexers, opts = {}) {
    const core = this.base.store.get(this._corePreload(indexers, ac.name, opts.cache))
    if (ac.name === '_system') core.on('append', this._queueFastForward)
    return core
  }

  migrate (indexers) {
    for (const ac of this.opened.values()) {
      const key = this.deriveKey(ac.name, indexers)

      if (key) {
        if (this.coresByKey.has(b4a.toString(key, 'hex'))) continue
        this.coresByKey.set(b4a.toString(key, 'hex'), ac)
      }

      if (ac.queued === -1) ac.queued = ac.indexedLength
    }
  }

  getIndexedCores () {
    const cores = []

    for (let i = 0; i < this.base.system.views.length; i++) {
      const v = this.base.system.views[i]
      const core = this.getByKey(v.key)
      if (!core || !core.pendingIndexedLength) break
      core.likelyIndex = i // just in case its out of date...
      cores.push(core)
    }

    return cores
  }

  async flush () {
    while (this.waiting.length) {
      const core = this.waiting.pop()
      await core.ready()
    }
  }

  deriveNamespace (name, entropy) {
    const encryptionId = crypto.hash(this.base.encryptionKey || EMPTY)
    return crypto.hash([NS_SIGNER_NAMESPACE, this.base.bootstrap, encryptionId, entropy, b4a.from(name)])
  }

  getByKey (key, indexers = this.base.linearizer.indexers) {
    const hex = b4a.toString(key, 'hex')
    if (this.coresByKey.has(hex)) return this.coresByKey.get(hex)

    for (let i = indexers.length; i >= 0; i--) {
      for (const [name, core] of this.opened) {
        if (!b4a.equals(key, this.deriveKey(name, indexers.slice(0, i)))) continue

        this.coresByKey.set(hex, core)
        return core
      }
    }

    return null
  }

  getByIndex (index) {
    if (this.coresByIndex.has(index)) return this.coresByIndex.get(index)

    for (const core of this.opened.values()) {
      if (!core.indexedLength || core.likelyIndex !== index) continue
      this.coresByIndex.set(index, core)
      return core
    }

    return null
  }

  deriveKey (name, indexers = this.base.linearizer.indexers) {
    const manifest = this._createManifest(indexers, name)
    return manifest && Hypercore.key(manifest)
  }

  _deriveStaticHash (name) {
    // key doesnt matter...
    return crypto.hash([this.base.bootstrap, b4a.from(name)])
  }

  _createManifest (indexers, name) {
    if (!indexers.length) return staticManifest(this._deriveStaticHash(name))

    for (const idx of indexers) {
      if (!idx.core.manifest) return null
    }

    const signers = indexers.map(idx => ({
      namespace: this.deriveNamespace(name, idx.core.manifest.signers[0].namespace),
      signature: 'ed25519',
      publicKey: idx.core.manifest.signers[0].publicKey
    }))

    return {
      version: 0,
      hash: 'blake2b',
      allowPatch: true,
      quorum: (signers.length >> 1) + 1,
      signers
    }
  }
}

function staticManifest (hash) {
  return {
    version: 0,
    hash: 'blake2b',
    signers: [],
    prologue: {
      hash,
      length: 0
    }
  }
}

function getBlockKey (bootstrap, encryptionKey, name) {
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, b4a.from(name)])
}
