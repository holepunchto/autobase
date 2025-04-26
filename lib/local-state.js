const messages = require('./messages.js')
const c = require('compact-encoding')
const b4a = require('b4a')

module.exports = class LocalState {
  constructor (core) {
    this.core = core
    this.tx = null
  }

  static async migrate (core, boot) {
    if (boot.version <= 3) {
      const gte = b4a.from([messages.LINEARIZER_PREFIX])
      const lt = b4a.from([messages.LINEARIZER_PREFIX + 1])

      const tx = core.state.storage.write()

      tx.deleteLocalRange(gte, lt)

      for await (const data of core.state.storage.createLocalStream({ gte, lt })) {
        const value = c.encode(messages.LinearizerUpdate, c.decode(messages.LinearizerUpdateV0, data.value))
        tx.putLocal(data.key, value)
      }

      boot.version = 3
      tx.setUserData('autobase/boot', c.encode(messages.BootRecord, boot))

      await tx.flush()
    }

    return boot
  }

  setBootRecord (boot) {
    return this.core.setUserData('autobase/boot', c.encode(messages.BootRecord, boot))
  }

  async listUpdates () {
    const gte = b4a.from([messages.LINEARIZER_PREFIX])
    const lt = b4a.from([messages.LINEARIZER_PREFIX + 1])
    const updates = []

    for await (const data of this.core.state.storage.createLocalStream({ gte, lt })) {
      const update = c.decode(messages.LinearizerUpdate, data.value)
      const seq = c.decode(messages.LinearizerKey, data.key)
      update.seq = seq
      updates.push(update)
    }

    return updates
  }

  clearUpdates () {
    if (this.tx === null) this.tx = this.core.state.storage.write()

    const gte = b4a.from([messages.LINEARIZER_PREFIX])
    const lt = b4a.from([messages.LINEARIZER_PREFIX + 1])

    this.tx.deleteLocalRange(gte, lt)
  }

  deleteUpdate (update) {
    if (this.tx === null) this.tx = this.core.state.storage.write()

    this.tx.deleteLocal(c.encode(messages.LinearizerKey, update.seq))
  }

  insertUpdate (update) {
    if (this.tx === null) this.tx = this.core.state.storage.write()

    this.tx.putLocal(c.encode(messages.LinearizerKey, update.seq), c.encode(messages.LinearizerUpdate, update))
  }

  flush () {
    const flushing = this.tx.flush()
    this.tx = null
    return flushing
  }
}
