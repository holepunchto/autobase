const b4a = require('b4a')
const c = require('compact-encoding')
const Hypercore = require('hypercore')
const crypto = require('hypercore-crypto')

const Autocore = require('./core')

const MANIFEST_VERSION = 1
const INDEX_VERSION = 1

const EMPTY = b4a.alloc(0)

const [NS_SIGNER_NAMESPACE, NS_VIEW_BLOCK_KEY] = crypto.namespace('autobase', 2)

class MemoryStore {
  constructor (viewStore, atom) {
    this.store = viewStore
    this.atom = atom

    this.closing = null
    this.index = this.store.sessions.push(this) - 1
    this.active = new Map()
  }

  * [Symbol.iterator] () {
    yield * this.active.values()
  }

  async ready () {
    await this.store.ready()
  }

  get (opts, moreOpts) {
    if (typeof opts === 'string') opts = { name: opts }
    if (moreOpts) opts = { ...opts, ...moreOpts, compat: false }

    const valueEncoding = opts.valueEncoding || null

    const ac = this.store.get(opts, { ...moreOpts, session: false })

    if (this.active.has(ac)) return this.active.get(ac)

    const overlay = ac.createWritable(valueEncoding, this.atom)

    this.active.set(ac, overlay)

    return overlay
  }

  opened () {
    const opening = []
    for (const core of this.active.values()) opening.push(core.ready())
    return Promise.all(opening)
  }

  flush () {
    return this.atom.flush()
  }

  async truncate (systemLength, views) {
    for (const [ac, core] of this.active) {
      if (core.length === 0) continue

      const i = ac.systemIndex
      const length = i === -1 ? -1 : i < views.length ? views[i].length : 0

      await core.truncate(length === -1 ? systemLength : length)
      if (length === 0) this.store._unindex(ac)
    }
  }

  close () {
    if (!this.closing) this.closing = this._close()
    return this.closing
  }

  _close () {
    const top = this.store.sessions.pop()
    if (top !== this) this.store.sessions[top.index = this.index] = top
    this.index = -1

    const closing = []
    for (const core of this.active.values()) {
      closing.push(core.close())
    }

    return Promise.all(closing)
  }
}

module.exports = class AutoStore {
  constructor (base) {
    this.base = base
    this.opened = new Map()
    this.coresByKey = new Map()
    this.coresByIndex = new Map()
    this.waiting = []
    this.sessions = []
  }

  async ready () {
    await this.base._presystem
  }

  get _indexers () {
    return this.base.linearizer && indexersWithManifest(this.base.linearizer.indexers.slice(0))
  }

  memorySession (atom) {
    return new MemoryStore(this, atom)
  }

  async migrate (sys, atom) {
    const nextIndexers = await this.getWriters(sys.indexers)
    const systemLength = this.base._indexedLength

    for (const ac of this.opened.values()) {
      const view = sys.views[ac.systemIndex]
      const length = view ? view.length : ac._isSystem() ? systemLength : 0
      if (length === 0) this._unindex(ac)
      await ac.migrateTo(nextIndexers, length, atom)
    }
  }

  async _preload (name, opts) {
    await Promise.resolve()
    if (!this.base.opening) throw new Error('Autobase failed to open')

    await this.ready()

    const inf = await this.base._getIndexedInfo()
    if (inf.system === null) return this._corePreload(this._indexers, name, null, opts.cache)

    if (name === '_system') {
      return {
        manifest: inf.system,
        cache: opts.cache,
        exclusive: false,
        compat: false,
        encryption: this.getBlockEncryption(name)
      }
    }

    const i = inf.views.indexOf(name)
    const view = inf.info.views[i].key

    return {
      key: view,
      cache: opts.cache,
      exclusive: false,
      compat: false,
      encryption: this.getBlockEncryption(name)
    }
  }

  get (opts, moreOpts) {
    if (typeof opts === 'string') opts = { name: opts }
    if (moreOpts) opts = { ...opts, ...moreOpts, compat: false }

    const name = opts.name
    const valueEncoding = opts.valueEncoding || null

    let ac = this.opened.get(name)

    if (!ac) {
      const core = this.base.store.get({ preload: this._preload(name, opts) })
      ac = new Autocore(this.base, core, name, opts)

      this.opened.set(name, ac)
      this.waiting.push(ac)
    }

    if (opts.session === false) return ac

    return ac.createSession(valueEncoding)
  }

  _corePreload (indexers, name, prologue, cache) {
    return {
      manifest: this._createManifest(indexers, name, prologue),
      cache,
      exclusive: true,
      compat: false,
      encryption: this.getBlockEncryption(name)
    }
  }

  async close () {
    const sessions = []
    for (const session of this.sessions) {
      sessions.push(session.close())
    }
    await Promise.all(sessions)

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

  getBlockEncryption (name) {
    const key = getBlockKey(this.base.bootstrap, this.base.encryptionKey, name)
    return key ? { key, block: true } : null
  }

  getSystemCore () {
    for (const core of this.opened.values()) {
      if (core.name === '_system') return core
    }
    return null
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

  getIndexedCores (info) {
    const cores = [this.getSystemCore()]

    for (let i = 0; i < info.views.length; i++) {
      cores.push(this.getByIndex(i))
    }

    return cores
  }

  indexedViewsByName (info) {
    const views = []

    for (let i = 0; i < info.views.length; i++) {
      const core = this.getByIndex(i)
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
      this.coresByKey.set(b4a.toString(core.key, 'hex'), core)
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
