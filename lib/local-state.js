const c = require('compact-encoding')
const b4a = require('b4a')

const messages = require('./messages.js')

const GTE = b4a.from([messages.LINEARIZER_PREFIX])
const LT = b4a.from([messages.LINEARIZER_PREFIX + 1])

module.exports = class LocalState {
  constructor(core) {
    this.core = core
    this.tx = null
  }

  static clear(core) {
    const tx = core.state.storage.write()
    tx.deleteLocalRange(GTE, LT)
    return tx.flush()
  }

  static async moveTo(src, dst) {
    const boot = await src.getUserData('autobase/boot')
    const tx = dst.state.storage.write()

    tx.deleteLocalRange(GTE, LT)

    for await (const data of src.state.storage.createLocalStream({ gte: GTE, lt: LT })) {
      tx.putLocal(data.key, data.value)
    }

    tx.putUserData('autobase/boot', boot)

    await tx.flush()
  }

  setBootRecord(boot) {
    if (this.tx === null) this.tx = this.core.state.storage.write()

    this.tx.putUserData('autobase/boot', c.encode(messages.BootRecord, boot))
  }

  async listUpdates() {
    const updates = []

    for await (const data of this.core.state.storage.createLocalStream({ gte: GTE, lt: LT })) {
      const update = c.decode(messages.LinearizerUpdate, data.value)
      const seq = c.decode(messages.LinearizerKey, data.key)
      update.seq = seq
      updates.push(update)
    }

    return updates
  }

  clearUpdates() {
    if (this.tx === null) this.tx = this.core.state.storage.write()

    this.tx.deleteLocalRange(GTE, LT)
  }

  deleteUpdate(update) {
    if (this.tx === null) this.tx = this.core.state.storage.write()

    this.tx.deleteLocal(c.encode(messages.LinearizerKey, update.seq))
  }

  insertUpdate(update) {
    if (this.tx === null) this.tx = this.core.state.storage.write()

    this.tx.putLocal(
      c.encode(messages.LinearizerKey, update.seq),
      c.encode(messages.LinearizerUpdate, update)
    )
  }

  flush() {
    if (!this.tx) return Promise.resolve()

    const flushing = this.tx.flush()
    this.tx = null
    return flushing
  }
}
