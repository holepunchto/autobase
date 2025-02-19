class PublicApplyCalls {
  constructor (base) {
    this.base = base
  }

  get key () {
    return this.base.key
  }

  async addWriter () {
    throw new Error('Not allowed on the public view')
  }

  async ackWriter () {
    throw new Error('Not allowed on the public view')
  }

  async removeWriter () {
    throw new Error('Not allowed on the public view')
  }

  interrupt () {
    throw new Error('Not allowed on the public view')
  }

  removeable () {
    throw new Error('Not allowed on the public view')
  }
}

class PrivateApplyCalls extends PublicApplyCalls {
  constructor (state) {
    super(state.base)
    this.state = state
    this.system = state.system
  }

  async addWriter (key, { indexer = true, isIndexer = indexer } = {}) { // just compat for old version
    await this.system.add(key, { isIndexer })
    await this.base._addWriter(key, this.system)
  }

  async ackWriter (key) {
    await this.system.ack(key)
  }

  async removeWriter (key) { // just compat for old version
    if (!this.state.removeable(key)) {
      throw new Error('Not allowed to remove the last indexer')
    }

    await this.system.remove(key)
    this.base._removeWriter(key)
  }

  interrupt (reason) {
    this.base._interrupt(reason)
  }

  removeable (key) {
    return this.state.removeable(key)
  }
}

module.exports = { PublicApplyCalls, PrivateApplyCalls }
