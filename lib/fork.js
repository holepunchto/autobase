const c = require('compact-encoding')

const SystemView = require('./system.js')
const { EncryptionView } = require('./encryption.js')
const { ManifestData } = require('./messages.js')

module.exports = class Fork {
  constructor (base, store, state, fork) {
    this.base = base
    this.store = store
    this.state = state
    this.indexers = fork.indexers
    this.length = fork.length
    this.system = null
    this.encryption = null
    this.manifests = null
    this.cores = []
  }

  async upgrade () {
    try {
      await this._upgrade()
      return true
    } catch {
      return false
    } finally {
      await this.close()
    }
  }

  async _upgrade () {
    this.manifests = await this.store.getIndexerManifests(this.indexers)

    this.system = new SystemView(await this._get('_system', this.length))
    this.encryption = new EncryptionView(this, await this._get('_encryption', -1))

    await this.system.ready()
    await this.encryption.ready()

    this.entropy = this.system.getEntropy(this.indexers, this.length)
    this.manifestVersion = Math.max(this.system.core.manifest.version, 2) // >= 2 to signal new encryption

    const views = []
    for (const view of this.state.views) {
      const v = this.getViewFromSystem(view, this.system)
      const indexedLength = v ? v.length : 0

      const session = await this._get(view.name, indexedLength)
      await session.ready()

      const next = await this.createView(view.name, session, null)
      await next.ready()

      await this._migrate(view.ref, next, view.core, indexedLength)

      // update key
      views.push({ key: next.key, length: indexedLength })
    }

    // internal views are explicitly migrated

    const entropy = EncryptionView.namespace(this.entropy)
    await this.encryption.update(entropy)

    const system = this.store.getViewByName('_system')
    const encryption = this.store.getViewByName('_encryption')

    const enc = await this.createView('_encryption', this.encryption.core, null)
    await enc.ready()

    await this._migrate(encryption, enc, this.encryption.core, this.encryption.core.length)

    await this.system.fork(this.indexers, this.manifests, this.encryption.core.length, views)

    const sys = await this.createView('_system', this.system.core, [enc.key])
    await sys.ready()

    await this._migrate(system, sys, this.system.core, this.system.core.length)
  }

  async createView (name, core, linked) {
    const prologue = await getPrologue(core)
    const manifestData = this._manifestData(core)

    return this.store.getViewCore(this.manifests, name, prologue, this.manifestVersion, this.entropy, linked, manifestData)
  }

  async _get (name, length) {
    const core = this.store.get({ name })
    this.cores.push(core)

    if (length !== -1) await core.truncate(length)

    return core
  }

  getViewFromSystem (view) {
    if (view.mappedIndex === -1 || view.mappedIndex >= this.system.views.length) return null
    return this.system.views[view.mappedIndex]
  }

  _manifestData (session) {
    if (session.manifest.version > 1) return session.manifest.userData
    return c.encode(ManifestData, { legacyBlocks: session.length })
  }

  async _migrate (ref, next, source, length) {
    if (length > 0) {
      await next.core.copyPrologue(source.state)
    }

    // remake the batch, reset from our prologue in case it replicated inbetween
    // TODO: we should really have an HC function for this
    const batch = next.session({ name: 'batch', overwrite: true, checkout: length })
    await batch.ready()

    if (source !== null) {
      while (batch.length < source.length) {
        await batch.append(await source.get(batch.length))
      }
    }

    await ref.batch.state.moveTo(batch, batch.length)
    await batch.close()

    ref.migrated(this.base, next)
  }

  async _ensureEncryption () {
    // only need encryption if we migrated
    if (this.core.encryption !== null) return true

    const encCore = this.core.manifest.version <= 1 ? null : this.cores[1]

    // expects internal views to be loaded in order
    this.encryption = new EncryptionView(this.base, encCore)
    const encryption = this.encryption.getViewEncryption('_system')

    await this.core.setEncryption(encryption)

    return true
  }

  async close () {
    this.destroyed = true
    if (this.system) await this.system.close()
    if (this.encryption) await this.encryption.close()
    for (const core of this.cores) await core.close()
  }
}

async function getPrologue (core) {
  if (!core.length) return null

  return {
    hash: await core.treeHash(),
    length: core.length
  }
}
