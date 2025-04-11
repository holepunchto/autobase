module.exports = class UpdateChanges {
  constructor (base) {
    this.base = base
    this.byName = new Map()
    this.tracking = null
  }

  get discoveryKey () {
    return this.base.discoveryKey
  }

  get key () {
    return this.base.key
  }

  get id () {
    return this.base.id
  }

  get system () {
    return this.base.system
  }

  track (state) {
    this.tracking = []
    this.byName.clear()

    if (!state) return

    for (const v of state.views) {
      if (v.ref) this._add(v.ref)
    }

    this._add(state.systemRef)
  }

  _add (ref) {
    this.tracking.push({ ref, from: ref.atomicBatch ? ref.atomicBatch.length : 0 })
  }

  finalise () {
    if (this.tracking === null) return

    for (const { ref, from } of this.tracking) {
      const core = ref.atomicBatch || ref.batch
      const trunc = ref.atomicBatch ? ref.atomicBatch.state.lastTruncation : null

      this.byName.set(ref.name, {
        from,
        to: core ? core.length : from,
        shared: trunc ? trunc.to : from
      })
    }

    this.tracking = null
  }

  get (name) {
    return this.byName.get(name)
  }
}
