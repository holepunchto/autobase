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
    this.processed = new Set()
  }

  async upgrade () {
    try {
      await this._upgrade()
      return this.key
    } catch (err) {
      return null
    }
  }

  async _upgrade () {
    const info = this.indexedSystem

    const system = this.store.getViewByName('_system')
    const encryption = this.store.getViewByName('_encryption')

    this.manifests = await this.store.getIndexerManifests(info.indexers)
    this.manifestVersion = system.core.manifest.version
    this.entropy = info.entropy

    for (const view of info.views) {
      const ref = await this.store.findViewByKey(view.key, info.indexers, this.manifestVersion, this.entropy)

      this.processed.add(ref)

      const session = ref.getCore()
      await session.ready()

      const next = await this.createView(ref.name, session, null, view.length)
      await next.ready()

      await this._migrate(ref, next, session, view.length)
    }

    for (const ref of this.store.byName.values()) {
      if (this.processed.has(ref)) continue

      const session = ref.getCore()
      await session.ready()

      const next = await this.createView(ref.name, session, null, 0)
      await next.ready()

      await this._migrate(ref, next, session, 0)
    }

    const enc = await this.createView('_encryption', encryption.atomicBatch, null, info.encryptionLength)
    await enc.ready()

    await this._migrate(encryption, enc, encryption.atomicBatch, info.encryptionLength)

    const sys = await this.createView('_system', system.atomicBatch, [enc.key], this.length)
    await sys.ready()

    await this._migrate(system, sys, system.atomicBatch, this.length, true)

    this.key = sys.core.key
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

  _manifestData (core) {
    if (core.manifest.version > 1) return core.manifest.userData
    return null
  }

  async createView (name, core, linked, length) {
    const prologue = await getPrologue(core, length)
    const manifestData = this._manifestData(core)

    return this.store.getViewCore(this.manifests, name, prologue, this.manifestVersion, this.entropy, linked, manifestData)
  }
}

// TODO: not atomic in regards to the ff, fix that
class FastForwardMigration {
  constructor (base, store, ff, indexedLength) {
    this.base = base
    this.store = store
    this.fastForward = ff
    this.length = indexedLength
    this.key = null
    this.manifestVersion = -1
    this.manifests = null
    this.destroyed = false
    this.processed = new Set()
    this.cores = []
  }

  async upgrade () {
    try {
      await this._upgrade()
      return this.key
    } catch (err) {
      return null
    } finally {
      await this.close()
    }
  }

  async _upgrade () {
    const info = this.fastForward

    const system = this.store.getViewByName('_system')

    this.manifests = await this.store.getIndexerManifests(info.indexers)
    this.manifestVersion = system.core.manifest.version
    this.entropy = info.entropy

    const sysCore = this.store.store.get(info.key)
    this.cores.push(sysCore)

    this.processed.add(system)

    for (const view of info.views) {
      const ref = await this.store.findViewByKey(view.key, info.indexers, this.manifestVersion, this.entropy)

      this.processed.add(ref)

      const next = this.store.store.get(view.key)
      this.cores.push(next)

      await this._migrate(ref, next, view.length)
    }

    for (const ref of this.store.byName.values()) {
      if (this.processed.has(ref)) continue
      await this._migrateZeroLength(ref)
    }

    await this._migrate(system, sysCore, this.length)

    this.key = sysCore.key
  }

  async _migrate (ref, next, length) {
    await next.ready()

    const prologue = next.manifest && next.manifest.prologue

    if (prologue && prologue.length > 0 && ref.core.length >= prologue.length) {
      try {
        await next.core.copyPrologue(ref.core.state)
      } catch {
        // we might be missing some nodes for this, just ignore, only an optimisation
      }
    }

    const batch = next.session({ name: 'batch', overwrite: true, checkout: length })
    await batch.ready()

    // remake the batch, reset from our prologue in case it replicated inbetween
    // TODO: we should really have an HC function for this

    await ref.batch.state.moveTo(batch, batch.length)
    await batch.close()

    ref.migrated(this.base, next)
  }

  async _migrateZeroLength (ref) {
    const core = ref.getCore()

    if (core.length !== 0) throw new Error('Expect zero length core')

    const manifestData = this._manifestData(core)
    const next = this.store.getViewCore(this.manifests, ref.name, null, this.manifestVersion, this.entropy, null, manifestData)
    this.cores.push(next)

    await this._migrate(ref, next, 0)
  }

  _manifestData (core) {
    if (core.manifest.version > 1) return core.manifest.userData
    return null
  }

  async close () {
    this.destroyed = true
    for (const core of this.cores) await core.close()
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
