const c = require('compact-encoding')

const SystemView = require('./system.js')
const { EncryptionView } = require('./encryption.js')
const { ManifestData } = require('./messages.js')

class Migration {
  constructor (base, store, system, indexedLength) {
    this.base = base
    this.store = store
    this.indexedSystem = system
    this.length = indexedLength
    this.key = null
    this.manifestVersion = -1
    this.manifests = null
    this.views = []
  }

  async upgrade () {
    try {
      const key = await this._premigrate()
      await this._upgrade()
      return key
    } catch (err) {
      return null
    }
  }

  async _premigrate () {
    const info = this.indexedSystem

    const system = this.store.getViewByName('_system')
    const encryption = this.store.getViewByName('_encryption')

    this.manifests = await this.store.getIndexerManifests(info.indexers)
    this.manifestVersion = system.core.manifest.version
    this.entropy = info.entropy

    const processed = new Set()
    for (const view of info.views) {
      const ref = await this.store.findViewByKey(view.key, info.indexers, this.manifestVersion, this.entropy)
      processed.add(ref)

      const session = ref.getCore()
      await session.ready()

      const { core, batch } = await this.createView(ref, null, view.length)

      this.views.push({ ref, core, batch, length: view.length })
    }

    const enc = await this.createView(encryption, null, info.encryptionLength)

    this.views.push({ ref: encryption, core: enc.core, batch: enc.batch, length: info.encryptionLength })
    processed.add(encryption)

    const sys = await this.createView(system, [enc.core.key], this.length)

    this.views.push({ ref: system, core: sys.core, batch: sys.batch, length: this.length })
    processed.add(system)

    for (const ref of this.store.byName.values()) {
      if (processed.has(ref)) continue

      const session = ref.getCore()
      await session.ready()

      const { core, batch } = await this.createView(ref, null, 0)

      this.views.push({ ref, core, batch, length: 0 })
    }

    return sys.core.key
  }

  async _upgrade () {
    for (const { ref, core, batch, length } of this.views) {
      const session = ref.getCore()
      await session.ready()

      await this._migrate(ref, core, batch, length)
    }
  }

  async _migrate (ref, core, batch, length) {
    await ref.batch.state.moveTo(batch, batch.length)
    ref.migrated(this.base, core)
  }

  _manifestData (core) {
    if (core.manifest.version > 1) return core.manifest.userData
    return null
  }

  async createView (ref, linked, length) {
    const source = ref.getCore()
    await source.ready()

    const prologue = await getPrologue(source, length)
    const manifestData = this._manifestData(source)

    const core = this.store.getViewCore(this.manifests, ref.name, prologue, this.manifestVersion, this.entropy, linked, manifestData)
    await core.ready()

    if (length > 0) {
      await core.core.copyPrologue(source.state)
    }

    // remake the batch, reset from our prologue in case it replicated inbetween
    // TODO: we should really have an HC function for this
    const batch = core.session({ name: 'batch', overwrite: true, checkout: length })
    await batch.ready()

    if (source !== null) {
      while (batch.length < source.length) {
        await batch.append(await source.get(batch.length))
      }
    }

    return { core, batch }
  }
}

// TODO: not atomic in regards to the ff, fix that
class FastForwardMigration {
  constructor (base, store, ff) {
    this.base = base
    this.store = store
    this.fastForward = ff
    this.length = ff.length
    this.key = null
    this.manifestVersion = -1
    this.manifests = null
    this.destroyed = false
    this.processed = new Set()
    this.cores = []
    this.views = []
  }

  async finalize (store) {
    if (this.destroyed) throw new Error('Migration is destroyed')

    try {
      await this._finalize(store)
      return this.key
    } catch (err) {
      return null
    } finally {
      await this.close()
    }
  }

  async premigrate () {
    const info = this.fastForward

    const system = this.store.getViewByName('_system')

    this.manifests = await this.store.getIndexerManifests(info.indexers)
    this.manifestVersion = system.core.manifest.version
    this.entropy = info.entropy

    const promises = []
    const processed = new Set()

    const sysCore = this.store.store.get(info.key)
    const pending = { ref: system, core: sysCore, batch: null, length: this.length }

    processed.add(system)

    this.views.push(pending)
    promises.push(this._premigrate(pending))

    for (const view of info.views) {
      const ref = await this.store.findViewByKey(view.key, info.indexers, this.manifestVersion, this.entropy)
      processed.add(ref)

      const core = await this.store.store.get(view.key)
      const pending = { ref, core, batch: null, length: view.length }

      this.views.push(pending)
      promises.push(this._premigrate(pending))
    }

    for (const ref of this.store.byName.values()) {
      if (processed.has(ref)) continue

      const core = await this.createView(ref)
      const pending = { ref, core, batch: null, length: 0 }

      this.views.push(pending)
      promises.push(this._premigrate(pending))
    }

    await Promise.all(promises)

    this.key = sysCore.key
  }

  _finalize () {
    const promises = []
    for (const { ref, core, batch, length } of this.views) {
      promises.push(this._migrate(ref, core, batch, length))
    }

    return Promise.all(promises)
  }

  async _premigrate (view) {
    this.views.push(view)

    await view.core.ready()

    const prologue = view.core.manifest && view.core.manifest.prologue

    if (prologue && prologue.length > 0 && view.ref.core.length >= prologue.length) {
      try {
        await view.core.core.copyPrologue(view.ref.core.state)
      } catch {
        // we might be missing some nodes for this, just ignore, only an optimisation
      }
    }

    view.batch = view.core.session({ name: 'batch', overwrite: true, checkout: view.length })
    await view.batch.ready()
  }

  async _migrate (ref, core, batch, length) {
    // remake the batch, reset from our prologue in case it replicated inbetween
    // TODO: we should really have an HC function for this

    await ref.batch.state.moveTo(batch, batch.length)
    await batch.close()

    ref.migrated(this.base, core)
  }

  createView (ref, length) {
    const prev = ref.getCore()

    if (prev.length !== 0) throw new Error('Expect zero length core')

    const manifestData = this._manifestData(prev)
    return this.store.getViewCore(this.manifests, ref.name, null, this.manifestVersion, this.entropy, null, manifestData)
  }

  _manifestData (core) {
    if (core.manifest.version > 1) return core.manifest.userData
    return null
  }

  async close () {
    this.destroyed = true
    for (const { core, batch } of this.views) {
      await core.close()
      await batch.close()
    }
  }
}

class Fork {
  constructor (base, store, state, fork) {
    this.base = base
    this.store = store
    this.state = state
    this.indexers = fork.indexers
    this.length = fork.length
    this.system = null
    this.manifestVersion = -1
    this.entropy = null
    this.destroyed = false
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
    return c.encode(ManifestData, { version: 0, legacyBlocks: session.length })
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

  async close () {
    this.destroyed = true
    if (this.system) await this.system.close()
    if (this.encryption) await this.encryption.close()
    for (const core of this.cores) await core.close()
  }
}

module.exports = {
  Migration,
  FastForwardMigration,
  Fork
}

async function getPrologue (core, length = core.length) {
  if (!length) return null

  return {
    hash: await core.treeHash(length),
    length
  }
}
