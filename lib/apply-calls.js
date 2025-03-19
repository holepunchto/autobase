class PublicApplyCalls {
  constructor (base) {
    this.base = base
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
  }

  get system () {
    return this.state.system
  }

  async addWriter (key, { indexer = true, isIndexer = indexer } = {}) { // just compat for old version
    await this.state.system.add(key, { isIndexer })
    await this.base._addWriter(key, this.state.system)
  }

  async ackWriter (key) {
    await this.state.system.ack(key)
  }

  async removeWriter (key) { // just compat for old version
    if (!this.state.removeable(key)) {
      throw new Error('Not allowed to remove the last indexer')
    }

    await this.state.system.remove(key)
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
