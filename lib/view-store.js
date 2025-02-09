const b4a = require('b4a')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const Hypercore = require('hypercore')
const messages = require('./messages')

const MANIFEST_VERSION = 1
const INDEX_VERSION = 1

const EMPTY = b4a.alloc(0)

const [NS_SIGNER_NAMESPACE, NS_VIEW_BLOCK_KEY] = crypto.namespace('autobase', 2)

class ViewCore {
  constructor (name, core) {
    this.name = name
    this.core = core
    this.batch = null
    this.atomicCore = null
    this.atomicBatch = null
  }

  async commit (atom, length, signature) {
    // if (this.atomicCore === null) {
    //   this.atomicCore = this.core.session({ atom, writable: true })
    // }

    // await this.atomicCore.commit(this.atomicBatch, { length, signature })

    // TODO: some hypercore bug here, only works without the atomic core (see basic.js tests)
    await this.core.commit(this.atomicBatch, { length, signature })
  }

  async release () {
    if (this.atomicBatch) {
      await this.atomicBatch.close()
      this.atomicBatch = null
    }

    if (this.atomicCore) {
      await this.atomicCore.close()
      this.atomicCore = null
    }
  }

  createSession (atom, valueEncoding) {
    if (this.batch === null) {
      this.batch = this.core.session({ name: 'batch', writable: true })
    }

    if (this.atomicBatch === null && atom) {
      this.atomicBatch = this.batch.session({ atom, writable: true })
    }

    const s = atom
      ? this.atomicBatch.session({ valueEncoding, writable: true })
      : this.batch.session({ valueEncoding, writable: true })

    return s
  }
}

class ViewStore {
  constructor (base, byName) {
    this.base = base
    this.store = base.store
    this.byName = byName || new Map()
    this.opened = []
    this.atom = null
    this.local = null
  }

  close () {
    // noop for now, should teardown active sessions
    return Promise.resolve()
  }

  atomize () {
    const store = new ViewStore(this.base, this.byName)
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
      valueEncoding = this.base.valueEncoding
    } = opts

    if (!name) throw new Error('name is required')
    return this.getViewByName(name).createSession(this.atom, valueEncoding)
  }

  getViewByName (name) {
    let view = this.byName.get(name)

    if (!view) {
      const preload = this._preload(name)
      const core = this.base.store.get({ preload })
      view = new ViewCore(name, core)
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
      // Hypercore bug here, doesnt flush
      // atom: this.atom,
      valueEncoding: messages.OplogMessage,
      encryption: this.base.encryption,
      active: false
    })

    return this.local
  }

  getSystemCore () {
    return this.getViewByName('_system').core
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
      compat: false,
      encryption: this.getBlockEncryption(name)
    })
  }

  async _preload (name) {
    await Promise.resolve()

    if (!this.base.opening) throw new Error('Autobase failed to open')
    await this.base._presystem

    const { system, indexers, views } = await this.base._getBootInfo()

    const indexerManifests = await this.getIndexerManifests(indexers)

    // no system, everything is fresh
    if (system === null) {
      return this._freshCorePreload(indexerManifests, name)
    }

    // asking for the system, just return it, easy
    if (name === '_system') {
      return {
        key: system,
        exclusive: false,
        compat: false,
        encryption: this.getBlockEncryption(name)
      }
    }

    // infer which view
    const v = await this.findViewByName(indexerManifests, views, name)

    // new view, fresh
    if (v === null) {
      return this._freshCorePreload(indexerManifests, name)
    }

    return {
      key: v.key,
      exclusive: false,
      compat: false,
      encryption: this.getBlockEncryption(name)
    }
  }

  _freshCorePreload (indexerManifests, name) {
    return {
      manifest: this._createManifest(indexerManifests, name, null),
      exclusive: true,
      compat: false,
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

  async createView (indexerManifests, prologue, name) {
    const pl = (prologue && prologue.length) ? prologue : null
    const manifest = this._createManifest(indexerManifests, name, pl)

    const core = this.store.get({
      key: Hypercore.key(manifest),
      manifest
    })

    await core.ready()
    await core.close()

    return core.key
  }

  getBootstrapSystemKey () {
    return Hypercore.key(this._createManifest([], '_system', null))
  }

  async findViewByKey (key) {
    const manifest = await this._getCoreManifest(key)
    if (!manifest) return null

    const target = manifest.signers[0].namespace

    for (const v of this.byName.values()) {
      const ns = v.core.manifest.signers[0].namespace
      if (b4a.equals(ns, target)) return v
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

module.exports = ViewStore

function getBlockKey (bootstrap, encryptionKey, name) {
  return encryptionKey && crypto.hash([NS_VIEW_BLOCK_KEY, bootstrap, encryptionKey, b4a.from(name)])
}
