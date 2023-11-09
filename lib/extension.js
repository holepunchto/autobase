const c = require('compact-encoding')
const { Wakeup } = require('./messages')

module.exports = class WakeupExtension {
  constructor (base, core) {
    this.base = base
    this.core = core

    this.extension = this.core.registerExtension('autobase', {
      onmessage: this._onmessage.bind(this)
    })

    const req = c.encode(Wakeup, { type: 0 })

    this.core.on('peer-add', (peer) => {
      this.extension.send(req, peer)
    })

    for (const peer of this.core.peers) {
      this.extension.send(req, peer)
    }
  }

  requestWakeup () {
    this.extension.broadcast(c.encode(Wakeup, { type: 0 }))
  }

  broadcastWakeup () {
    const m = this._encodeWakeup()
    if (m) this.extension.broadcast(m)
  }

  _encodeWakeup () {
    const writers = []

    for (const w of this.base.activeWriters) {
      if (w.isIndexer || w.flushed()) continue
      writers.push(w.core.key)
    }

    if (!writers.length) return null

    return c.encode(Wakeup, { type: 1, writers })
  }

  _onmessage (buf, from) {
    if (!buf) return

    let value = null
    try {
      value = c.decode(Wakeup, buf)
    } catch {
      return
    }

    if (value.type === 0) {
      const m = this._encodeWakeup()
      if (m) this.extension.send(m, from)
      return
    }

    this.base.hintWakeup(value.writers)
  }
}
