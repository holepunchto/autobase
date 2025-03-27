module.exports = class UpdateChanges {
  constructor (base) {
    this.base = base
    this.byName = new Map()
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

  add (ref) {
    const from = ref.atomicBatch ? ref.atomicBatch.length : 0
    this.byName.set(ref.name, {
      ref,
      from
    })
  }

  get (name) {
    const update = this.byName.get(name)
    if (!update) return null

    const { ref, from } = update

    const core = ref.atomicBatch || ref.batch

    const to = core ? core.length : 0
    const trunc = ref.atomicBatch ? ref.atomicBatch.state.lastTruncation : null

    return { from, to, shared: trunc ? trunc.to : from }
  }
}
