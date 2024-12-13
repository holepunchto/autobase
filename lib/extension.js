const c = require('compact-encoding')
const b4a = require('b4a')
const { Wakeup } = require('./messages')

const VERSION = 1

module.exports = class WakeupExtension {
  constructor (base, core, passive) {
    this.base = base
    this.core = core

    this.extension = this.core.registerExtension('autobase', {
      onmessage: this._onmessage.bind(this)
    })

    const req = c.encode(Wakeup, { type: 0 })

    this.core.on('peer-add', (peer) => {
      this.extension.send(req, peer)
    })

    if (!passive) {
      for (const peer of this.core.peers) {
        this.extension.send(req, peer)
      }
    }
  }

  sendWakeup (key, target) {
    const m = this._encodeWakeup(VERSION)
    if (!m) return

    for (const peer of this.core.peers) {
      if (b4a.equals(peer.remotePublicKey, key)) {
        this.extension.send(m, peer)
        break
      }
    }
  }

  requestWakeup () {
    this.extension.broadcast(c.encode(Wakeup, { type: 0 }))
  }

  broadcastWakeup () {
    const m = this._encodeWakeup(VERSION)
    if (m) this.extension.broadcast(m)
  }

  _encodeWakeup (version) {
    const writers = []

    for (const w of this.base.activeWriters) {
      if (w.isActiveIndexer || w.flushed()) continue
      writers.push({ key: w.core.key, length: w.length })
    }

    if (!writers.length) return null

    return c.encode(Wakeup, { version, type: 1, writers })
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
      const m = this._encodeWakeup(value.version)
      if (m) this.extension.send(m, from)
      return
    }

    this.base.hintWakeup(value.writers)
  }
}
