const rrp = require('resolve-reject-promise')

const SystemView = require('./system.js')
const { EncryptionView } = require('./encryption')

const DEFAULT_OP_TIMEOUT = 5_000
const DEFAULT_MIN_FF = 16

module.exports = class FastForward {
  constructor (base, key, { timeout = DEFAULT_OP_TIMEOUT, verified = true, minimum = DEFAULT_MIN_FF, force = false } = {}) {
    this.base = base
    this.key = key
    this.timeout = timeout
    this.force = force
    this.core = base.store.get({ key, active: true, encryption: null })
    this.length = 0
    this.views = []
    this.internalViews = []
    this.linked = null
    this.indexers = []
    this.minimum = minimum
    this.verified = verified
    this.waiting = new Set()
    this.cores = [this.core]
    this.system = null
    this.destroyed = false
    this.upgrading = null
    this.failed = false
  }

  static MINIMUM = DEFAULT_MIN_FF

  async upgrade () {
    if (!this.upgrading) this.upgrading = this._upgrade()
    try {
      if (!await this.upgrading) return null
      return {
        length: this.length,
        force: this.force,
        key: this.key,
        indexers: this.indexers,
        views: this.views,
        internalViews: this.internalViews
      }
    } catch {
      this.failed = true
      return null
    } finally {
      await this.close()
    }
  }

  _waitForAppend (core) {
    if (core.length > 0) return Promise.resolve()

    const promise = rrp()
    const timeout = setTimeout(this.close.bind(this), this.timeout)
    const wait = { promise, timeout }

    this.waiting.add(wait)

    core.once('append', () => {
      this.waiting.delete(wait)
      clearTimeout(timeout)
      promise.resolve()
    })

    return promise.promise
  }

  _minLength () {
    return this.base.core.length + this.minimum
  }

  async _upgrade () {
    await this.core.ready()

    if (!this.verified) await this._waitForAppend(this.core)
    if (this.destroyed) return false

    if (this.core.manifest.linked) {
      const appends = []

      for (const key of this.core.manifest.linked) {
        const core = this.base.store.get({ key, active: true })
        this.cores.push(core)

        await core.ready()
        appends.push(this._waitForAppend(core))
      }

      await Promise.all(appends)
    }

    this.length = this.core.length

    // note we use the persisted length here, as we might as well continue that work as thats ~ the same length
    if (!this.force && (this.length === 0 || this.length < this._minLength())) return false

    await this.core.get(this.length - 1, { timeout: this.timeout })

    for (const core of this.cores) {
      // no guarantees internal views are in sync with system, but safe since signed length is always < indexed length
      const length = core === this.core ? this.length : core.length
      this.internalViews.push({ key: core.key, length })
    }

    if (this.destroyed) return false

    if (this.base.encryptionKey && !(await this._ensureEncryption())) return false
    if (this.destroyed) return false

    this.system = new SystemView(this.core, { checkout: this.length })
    await this.system.ready()
    if (this.destroyed) return false

    const promises = []

    // ensure local key is locally available always
    promises.push(this.system.get(this.base.local.key, { timeout: this.timeout }))

    for (const v of this.system.views) {
      this.views.push(v)
      const core = this.base.store.get({ key: v.key, active: true })
      this.cores.push(core)
      if (!v.length) continue // encryption view can be 0 length
      promises.push(core.get(v.length - 1, { timeout: this.timeout }))
    }

    for (const idx of this.system.indexers) {
      this.indexers.push(idx)
      if (idx.length === 0) continue // need to make sure we have manifest...
      const core = this.base.store.get({ key: idx.key, active: true })
      this.cores.push(core)
      promises.push(core.get(idx.length - 1, { timeout: this.timeout }))
      promises.push(this.system.get(idx.key, { timeout: this.timeout }))
    }

    for (const head of this.system.heads) {
      promises.push(this.system.get(head.key, { timeout: this.timeout }))
    }

    await Promise.all(promises)
    if (this.destroyed) return false

    return true
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
    for (const { promise, timeout } of this.waiting) {
      clearTimeout(timeout)
      promise.resolve() // silently bail
    }
    if (this.system) await this.system.close()
    if (this.encryption) await this.encryption.close()
    for (const core of this.cores) await core.close()
    await this.core.close()
  }
}
