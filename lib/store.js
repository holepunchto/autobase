const b4a = require('b4a')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const Hypercore = require('hypercore')
const WakeupExtension = require('./extension.js')
const messages = require('./messages.js')

const MANIFEST_VERSION = 1
const INDEX_VERSION = 1

const EMPTY = b4a.alloc(0)

const [NS_SIGNER_NAMESPACE, NS_VIEW_BLOCK_KEY] = crypto.namespace('autobase', 2)

class ViewCore {
  constructor (name, core, base) {
    this.name = name
    this.core = null
    this.batch = null
    this.atomicBatch = null
    this.extension = null

    this.migrated(base, core)
  }

  migrated (base, core) {
    // TODO: close old core if present, for now we just close when the autobase is closed indirectly
    // atm its unsafe to do as moveTo has a bug due to a missing read lock in hc
    this.core = core

    if (!this.extension || this.extension.core !== this.core) {
      this.extension = new WakeupExtension(base, this.core, false)
    }

    if (this.name === '_system') {
      const ff = base._queueFastForward.bind(base)
      this.core.on('append', ff)
      this.core.ready().then(ff, ff)
    }
  }

  async matchesKey (key) {
    if (!this.core.opened) await this.core.ready()
    return b4a.equals(this.core.key, key)
  }

  async matchesNamespace (target) {
    if (!this.core.opened) await this.core.ready()

    if (this.core.manifest && this.core.manifest.signers.length > 0) {
      const ns = this.core.manifest.signers[0].namespace
      if (b4a.equals(ns, target)) return true
    }

    return false
  }

  async commit (atom, length, signature) {
    // TODO: is this how its supposed be done atomic wise?
    await this.core.commit(this._getAtomicBatch(atom), { length, signature })
  }

  async release () {
    if (this.atomicBatch) {
      await this.atomicBatch.close()
      this.atomicBatch = null
    }
  }

  _getAtomicBatch (atom) {
    if (this.atomicBatch === null) {
      this.atomicBatch = this.batch.session({ atom, writable: true })
    }

    return this.atomicBatch
  }

  async catchup (atom, length) {
    await this.release()
    const batch = this._getAtomicBatch(atom)
    await batch.ready()
    await batch.state.catchup(length)
  }

  createSession (atom, valueEncoding) {
    if (this.batch === null) {
      this.batch = this.core.session({ name: 'batch', writable: true })
    }

    const s = atom
      ? this._getAtomicBatch(atom).session({ valueEncoding, writable: true })
      : this.batch.session({ valueEncoding, writable: false })

    return s
  }
}

class AutoStore {
  constructor (base, byName) {
    this.base = base
    this.store = base.store
    this.byName = byName || new Map()
    this.opened = []
    this.atom = null
    this.local = null
  }

  async close () {
    if (this.local) await this.local.close()
    for (const v of this.byName.values()) {
      await v.release()
    }
    if (this.atom) return
    for (const v of this.byName.values()) {
      if (v.core) await v.core.close()
      if (v.batch) await v.batch.close()
    }
  }

  atomize () {
    const store = new AutoStore(this.base, this.byName)
    store.atom = this.store.storage.createAtom()
    return store
  }

  flush () {
    return this.atom ? this.atom.flush() : Promise.resolve()
  }

  get (opts, moreOpts) {
    if (typeof opts === 'string') opts = { name: opts }
    if (moreOpts) opts = { ...opts, ...moreOpts }

    const {
      name,
      valueEncoding = null
    } = opts

    if (!name) throw new Error('name is required')
    return this.getViewByName(name).createSession(this.atom, valueEncoding)
  }

  getViewByName (name) {
    let view = this.byName.get(name)

    if (!view) {
      const preload = this._preload(name)
      const core = this.base.store.get({ preload })
      view = new ViewCore(name, core, this.base)
      this.byName.set(name, view)
    }

    if (this.opened.indexOf(view) === -1) {
      this.opened.push(view)
    }

    return view
  }

  getLocal () {
    if (this.local !== null) return this.local

    this.local = this.base.local.session({
      atom: this.atom,
      valueEncoding: messages.OplogMessage,
      encryption: this.base.encryption,
      active: false
    })

    return this.local
  }

  getViews () {
    return [...this.byName.values()]
  }

  getSystemView () {
    return this.getViewByName('_system')
  }

  getSystemCore () {
    return this.getSystemView().core
  }

  getSystemEncryption () {
    return this.base.encryptionKey ? this.getBlockEncryption('_system') : null
  }

  async getIndexerManifests (entries) {
    const manifests = []
    for (const { key } of entries) {
      const core = this.store.get(key)
      await core.ready()
      if (!core.manifest) continue // danger but only for bootstrapping...
      manifests.push(core.manifest)
      await core.close()
    }
    return manifests
  }

  getViewCore (indexerManifests, name, prologue) {
    const manifest = this._createManifest(indexerManifests, name, prologue)

    return this.base.store.get({
      manifest,
      exclusive: false,
      encryption: this.getBlockEncryption(name)
    })
  }

  async createView (indexerManifests, name, prologue) {
    const manifest = this._createManifest(indexerManifests, name, prologue)

    const core = this.store.get({
      key: Hypercore.key(manifest),
      manifest
    })

    await core.ready()
    const key = core.key

    await core.close()
    return key
  }

  async _preload (name) {
    await Promise.resolve()

    if (!this.base.opening) throw new Error('Autobase failed to open')
    await this.base._preopen

    const boot = (await this.base._getSystemInfo()) || { key: this.getBootstrapSystemKey(), indexers: [], views: [] }
    const indexerManifests = await this.getIndexerManifests(boot.indexers)

    // no system, everything is fresh
    if (boot.key === null) {
      return this._freshCorePreload(indexerManifests, name)
    }

    // asking for the system, just return it, easy
    if (name === '_system') {
      return {
        key: boot.key,
        exclusive: false,
        encryption: this.getBlockEncryption(name)
      }
    }

    // infer which view
    const v = await this.findViewByName(indexerManifests, boot.views, name)

    // new view, fresh
    if (v === null) {
      return this._freshCorePreload(indexerManifests, name)
    }

    return {
      key: v.key,
      exclusive: false,
      encryption: this.getBlockEncryption(name)
    }
  }

  _freshCorePreload (indexerManifests, name) {
    return {
      manifest: this._createManifest(indexerManifests, name, null),
      exclusive: true,
      encryption: this.getBlockEncryption(name)
    }
  }

  _deriveStaticHash (name) {
    // key doesnt matter...
    return crypto.hash([this.base.bootstrap, b4a.from(name)])
  }

  _deriveNamespace (name, entropy) {
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

  async _getCoreManifest (key) {
    const core = this.store.get(key)
    await core.ready()
    const manifest = core.manifest
    await core.close()
    return manifest
  }

  getBootstrapSystemKey () {
    return Hypercore.key(this._createManifest([], '_system', null))
  }

  async findViewByKey (key, indexers) {
    for (const v of this.byName.values()) {
      if (await v.matchesKey(key)) return v
    }

    const manifest = await this._getCoreManifest(key)
    const target = (manifest && manifest.signers.length) ? manifest.signers[0].namespace : null

    if (target) {
      for (const v of this.byName.values()) {
        if (await v.matchesNamespace(target)) return v
      }
    }

    const indexerManifests = await this.getIndexerManifests(indexers)

    if (target) {
      const ns = indexerManifests[0].signers[0].namespace

      for (const v of this.byName.values()) {
        const namespace = this._deriveNamespace(v.name, ns)
        if (b4a.equals(namespace, target)) return v
      }
    }

    // prop the empty prologue, we dont have manifest for those in FF if len=0
    for (const v of this.byName.values()) {
      const manifest = this._createManifest(indexerManifests, v.name, null)
      const manifestKey = Hypercore.key(manifest)
      if (b4a.equals(manifestKey, key)) return v
    }

    return null
  }

  async findViewByName (indexerManifests, views, name) {
    if (indexerManifests.length === 0) return null

    const namespace = this._deriveNamespace(name, indexerManifests[0].signers[0].namespace)

    for (const v of views) {
      const manifest = await this._getCoreManifest(v.key)
      if (manifest.signers.length === 0) continue

      const signer = manifest.signers[0]

      if (b4a.equals(signer.namespace, namespace)) return v
    }

    return null
  }

  _createManifest (indexerManifests, name, prologue) {
    if (!indexerManifests.length) {
      prologue = {
        hash: this._deriveStaticHash(name),
        length: 0
      }
    } else if (prologue && prologue.length === 0) {
      // just in case
      prologue = null
    }

    const signers = []

    for (const manifest of indexerManifests) {
      const signer = manifest.signers[0]

      signers.push({
        namespace: this._deriveNamespace(name, signer.namespace),
        signature: 'ed25519',
        publicKey: signer.publicKey
      })
    }

    return {
      version: MANIFEST_VERSION,
      hash: 'blake2b',
      prologue,
      allowPatch: true,
      quorum: Math.min(signers.length, (signers.length >> 1) + 1),
      signers
    }
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
}

module.exports = AutoStore

function getBlockKey (bootstrap, encryptionKey, name) {
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, b4a.from(name)])
}
