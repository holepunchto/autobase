const SystemView = require('./system.js')

const DEFAULT_OP_TIMEOUT = 5_000
const DEFAULT_MIN_FF = 16

module.exports = class FastForward {
  constructor (base, key, { timeout = DEFAULT_OP_TIMEOUT, verified = true, minimum = DEFAULT_MIN_FF } = {}) {
    const encryption = base._viewStore.getSystemEncryption()

    this.base = base
    this.key = key
    this.timeout = timeout
    this.core = base.store.get({ key, active: true, encryption })
    this.length = 0
    this.views = []
    this.entropy = null
    this.indexers = []
    this.minimum = minimum
    this.verified = verified
    this.cores = []
    this.system = null
    this.destroyed = false
    this.upgrading = null

    this._appendTimeout = null
    this._resolveAppend = null
  }

  static MINIMUM = DEFAULT_MIN_FF

  async upgrade () {
    if (!this.upgrading) this.upgrading = this._upgrade()
    try {
      if (!await this.upgrading) return null
      return { length: this.length, key: this.key, indexers: this.indexers, views: this.views, entropy: this.entropy }
    } catch {
      return null
    } finally {
      await this.close()
    }
  }

  _waitForAppend () {
    return new Promise((resolve) => {
      this._appendTimeout = setTimeout(this.close.bind(this), this.timeout)
      this._resolveAppend = resolve
      this.core.once('append', this._continueAppend.bind(this))
    })
  }

  _continueAppend () {
    if (this._resolveAppend === null) return

    clearTimeout(this._appendTimeout)
    const resolve = this._resolveAppend

    this._appendTimeout = null
    this._resolveAppend = null

    resolve()
  }

  _minLength () {
    return this.base.system.core.length + this.minimum
  }

  async _upgrade () {
    await this.core.ready()

    if (!this.verified) await this._waitForAppend()
    if (this.destroyed) return false

    this.length = this.core.length
    // note we use the persisted length here, as we might as well continue that work as thats ~ the same length
    if (this.length === 0 || this.length < this._minLength()) return false

    await this.core.get(this.length - 1, { timeout: this.timeout })

    this.system = new SystemView(this.core, { checkout: this.length })
    await this.system.ready()
    if (this.destroyed) return false

    this.entropy = this.system.entropy

    const promises = []

    // ensure local key is locally available always
    promises.push(this.system.get(this.base.local.key))

    for (const v of this.system.views) {
      this.views.push(v)
      if (v.length === 0) continue
      const core = this.base.store.get({ key: v.key, active: true })
      this.cores.push(core)
      promises.push(core.get(v.length - 1, { timeout: this.timeout }))
    }

    for (const idx of this.system.indexers) {
      this.indexers.push(idx)
      if (idx.length === 0) continue
      const core = this.base.store.get({ key: idx.key, active: true })
      this.cores.push(core)
      promises.push(core.get(idx.length - 1, { timeout: this.timeout }))
      promises.push(this.system.get(idx.key))
    }

    for (const head of this.system.heads) {
      promises.push(this.system.get(head.key))
    }

    await Promise.all(promises)
    if (this.destroyed) return false

    return true
  }

  async close () {
    this.destroyed = true
    this._continueAppend()
    if (this.system) await this.system.close()
    for (const core of this.cores) await core.close()
  }
}
