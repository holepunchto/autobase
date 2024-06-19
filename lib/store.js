const b4a = require('b4a')
const c = require('compact-encoding')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')

const Autocore = require('./core')

const MANIFEST_VERSION = 1
const INDEX_VERSION = 1

const EMPTY = b4a.alloc(0)

const [NS_SIGNER_NAMESPACE, NS_VIEW_BLOCK_KEY] = crypto.namespace('autobase', 2)

module.exports = class AutoStore {
  constructor (base) {
    this.base = base
    this.opened = new Map()
    this.coresByKey = new Map()
    this.coresByIndex = new Map()
    this.waiting = []
  }

  async ready () {
    await this.base._presystem
  }

  get _indexers () {
    return this.base.linearizer && indexersWithManifest(this.base.linearizer.indexers.slice(0))
  }

  async migrate () {
    const sys = this.getSystemCore()
    const { indexers, views } = await this.base.system.getIndexedInfo()
    const nextIndexers = await this.getWriters(indexers)

    for (const ac of this.opened.values()) {
      const view = views[ac.systemIndex]
      const length = view ? view.length : ac._isSystem() ? sys.indexedLength : 0
      await ac.migrateTo(nextIndexers, length)
    }

    this.base.recouple()
  }

  get (opts, moreOpts) {
    if (typeof opts === 'string') opts = { name: opts }
    if (moreOpts) opts = { ...opts, ...moreOpts, compat: false }

    const name = opts.name
    const valueEncoding = opts.valueEncoding || null

    const core = this._indexers
      ? this.base.store.get(this._corePreload(this._indexers, name, null, opts.cache))
      : this.base.store.get({
        preload: async () => {
          await Promise.resolve()
          if (!this.base.opening) throw new Error('Autobase failed to open')

          await this.ready()
          return this._corePreload(this._indexers, name, null, opts.cache)
        }
      })

    const ac = new Autocore(this.base, core, name, opts)

    this.opened.set(name, ac)

    this.waiting.push(ac)

    return ac.createSession(valueEncoding)
  }

  _corePreload (indexers, name, prologue, cache) {
    return {
      manifest: this._createManifest(indexers, name, prologue),
      cache,
      exclusive: true,
      compat: false,
      encryptionKey: this.getBlockKey(name),
      isBlockKey: true
    }
  }

  close () {
    const closing = []
    for (const core of this.opened.values()) {
      closing.push(core.close())
    }

    return Promise.all(closing)
  }

  static getBlockKey (bootstrap, encryptionKey, name) {
    return getBlockKey(bootstrap, encryptionKey, name)
  }

  getBlockKey (name) {
    return getBlockKey(this.base.bootstrap, this.base.encryptionKey, name)
  }

  getSystemCore () {
    return this.base.system.core._source
  }

  async getWriters (keys) {
    // todo: should initial system entry have bootstrap as indexer?
    if (!keys.length) return this.base.linearizer.indexers.slice(0)

    return Promise.all(keys.map(({ key }) => this.base._getWriterByKey(key, -1, 0, false, false, null)))
  }

  async getCore (ac, indexers, length, opts = {}) {
    const prologue = await ac.getPrologue(length)
    return this.base.store.get(this._corePreload(indexers, ac.name, prologue, opts.cache))
  }

  getIndexedCores () {
    const cores = [this.getSystemCore()]

    for (let i = 0; i < this.base.system.views.length; i++) {
      const core = this.getByIndex(i)
      if (!core || !core.pendingIndexedLength) break
      cores.push(core)
    }

    return cores
  }

  indexedViewsByName () {
    const views = []

    for (let i = 0; i < this.base.system.views.length; i++) {
      const core = this.getByIndex(i)
      if (!core || !core.pendingIndexedLength) break
      views.push(core.name)
    }

    return views
  }

  async flush () {
    while (this.waiting.length) {
      const core = this.waiting.pop()
      await core.ready()
    }

    for (const view of this.opened.values()) {
      if (!view.isBootstrapped() && !(await view.bootstrap())) return false
    }

    return true
  }

  // called by autocore on truncate
  _unindex (ac) {
    this.coresByIndex.delete(ac.systemIndex)
    ac.systemIndex = -1
  }

  deriveNamespace (name, entropy) {
    const encryptionId = crypto.hash(this.base.encryptionKey || EMPTY)
    const version = c.encode(c.uint, INDEX_VERSION)
    const bootstrap = this.base.bootstrap

    return crypto.hash([
      NS_SIGNER_NAMESPACE,
      version,
      bootstrap,
      encryptionId,
      entropy,
      b4a.from(name)
    ])
  }

  getByKey (key, indexers = this.base.linearizer.indexers) {
    const hex = b4a.toString(key, 'hex')
    if (this.coresByKey.has(hex)) return this.coresByKey.get(hex)

    for (const core of this.opened.values()) {
      if (b4a.equals(key, core.key)) {
        this.coresByKey.set(hex, core)
        return core
      }
    }

    return null
  }

  getByIndex (index) {
    if (this.coresByIndex.has(index)) return this.coresByIndex.get(index)

    for (const core of this.opened.values()) {
      if (core.systemIndex !== index) continue
      this.coresByIndex.set(index, core)
      return core
    }

    return null
  }

  deriveKey (name, indexers = this.base.linearizer.indexers, prologue = null) {
    const pl = prologue && prologue.length ? prologue : null
    const manifest = this._createManifest(indexers, name, pl)
    return manifest && Hypercore.key(manifest)
  }

  _deriveStaticHash (name) {
    // key doesnt matter...
    return crypto.hash([this.base.bootstrap, b4a.from(name)])
  }

  _createManifest (indexers, name, prologue) {
    if (!indexers.length && !(prologue && prologue.length > 0)) {
      prologue = {
        hash: this._deriveStaticHash(name),
        length: 0
      }
    }

    for (const idx of indexers) {
      if (!idx.core.manifest) return null
    }

    const signers = indexers.map(idx => ({
      namespace: this.deriveNamespace(name, idx.core.manifest.signers[0].namespace),
      signature: 'ed25519',
      publicKey: idx.core.manifest.signers[0].publicKey
    }))

    return {
      version: MANIFEST_VERSION,
      hash: 'blake2b',
      prologue,
      allowPatch: true,
      quorum: Math.min(signers.length, (signers.length >> 1) + 1),
      signers
    }
  }
}

function getBlockKey (bootstrap, encryptionKey, name) {
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, b4a.from(name)])
}

function indexersWithManifest (indexers) {
  if (indexers.length === 1 && !indexers[0].core.manifest) return []
  return indexers
}
