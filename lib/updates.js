module.exports = class UpdateChanges {
  constructor(base) {
    this.base = base
    this.byName = new Map()
    this.tracking = null
  }

  get discoveryKey() {
    return this.base.discoveryKey
  }

  get key() {
    return this.base.key
  }

  get id() {
    return this.base.id
  }

  get system() {
    return this.base.system
  }

  track(state) {
    this.tracking = []
    this.byName.clear()

    if (!state) return

    for (const v of state.views) {
      if (v.ref) this._add(v.ref)
    }

    this._add(state.systemView.ref)
    this._add(state.encryptionView.ref)
  }

  _add(ref) {
    const from = ref.batch.length
    const inf = { ref, from }
    ref.shared = from
    this.tracking.push(inf)
  }

  finalise() {
    if (this.tracking === null) return

    for (const { ref, from } of this.tracking) {
      const core = ref.atomicBatch || ref.batch
      this.byName.set(ref.name, {
        from,
        to: core.length,
        shared: ref.shared
      })
    }

    this.tracking = null
  }

  get(name) {
    return this.byName.get(name)
  }
}
