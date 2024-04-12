const c = require('compact-encoding')
const BufferMap = require('tiny-buffer-map')
const { Wakeup } = require('./messages')

module.exports = class WakeupExtension {
  constructor (base, core) {
    this.base = base
    this.core = core

    this._peers = new BufferMap()

    this.extension = this.core.registerExtension('autobase', {
      onmessage: this._onmessage.bind(this)
    })

    const req = c.encode(Wakeup, { type: 0 })

    this.core.on('peer-add', (peer) => {
      this._peers.set(peer.remotePublicKey, peer)
      this.extension.send(req, peer)
    })

    this.core.on('peer-remove', (peer) => {
      this._peers.delete(peer.remotePublicKey)
    })

    for (const peer of this.core.peers) {
      this.extension.send(req, peer)
    }
  }

  sendWakeup (key) {
    const peer = this._peers.get(key)
    if (!peer) return

    const m = this._encodeWakeup()
    if (m) peer.extension('autobase', c.encode(this.extension.encoding, m))
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
