const c = require('compact-encoding')
const ReadyResource = require('ready-resource')
const Protomux = require('protomux')
const BufferMap = require('tiny-buffer-map')
const { Clock } = require('./messages')

const WakeupRequest = {
  preencode (state, m) {
    c.uint.preencode(state, 1) // version
    c.fixed32.preencode(state, m.key)
  },
  encode (state, m) {
    c.uint.encode(state, 1) // version
    c.fixed32.encode(state, m.key)
  },
  decode (state) {
    const v = c.uint.decode(state)
    if (v > 1) throw new Error('Unsupported version: ' + v)

    return { key: c.fixed32.decode(state) }
  }
}

const WakeupResponse = {
  preencode (state, m) {
    c.uint.preencode(state, 1) // version
    c.fixed32.preencode(state, m.key)
    Clock.preencode(state, m.writers)
  },
  encode (state, m) {
    c.uint.encode(state, 1) // version
    c.fixed32.encode(state, m.key)
    Clock.encode(state, m.writers)
  },
  decode (state) {
    const v = c.uint.decode(state)
    if (v > 1) throw new Error('Unsupported version: ' + v)

    return {
      key: c.fixed32.decode(state),
      writers: Clock.decode(state)
    }
  }
}

class WakeupStream {
  constructor (stream, wakeup) {
    this.stream = stream
    this.mux = attachMuxer(stream)

    this.wakeup = wakeup

    this.channel = null
    this.request = null
    this.response = null
    this.key = null

    this.opening = null
    this.opened = false

    this.mux.pair({ protocol: 'autobase/wakeup' }, () => this._openChannel())
    this._openChannel()
  }

  destroy () {
    if (this.channel) this.channel.close()
    this.mux.unpair({ protocol: 'autobase/wakeup' })
  }

  _openChannel () {
    const channel = this.mux.createChannel({
      protocol: 'autobase/wakeup',
      onopen: () => {
        this.key = this.stream.remotePublicKey
        this.wakeup.connections.set(this.key, this)
      },
      onclose: () => {
        this.channel = null
        this.request = null
        this.response = null
      }
    })

    if (channel === null) return // already opened

    this.channel = channel

    this.request = this.channel.addMessage({
      encoding: WakeupRequest,
      onmessage: async (req) => {
        await this.wakeup.onrequest(req, this)
      }
    })

    this.response = this.channel.addMessage({
      encoding: WakeupResponse,
      onmessage: async (res) => {
        await this.wakeup.onresponse(res, this)
      }
    })

    this.channel.open()
  }

  async sendWakeup (wakeup) {
    if (!this.channel) {
      throw new Error('No active stream channel')
    }

    return this.response.send(wakeup)
  }

  async requestWakeup (key) {
    if (!this.channel) {
      throw new Error('No active stream channel')
    }

    return this.request.send({ key })
  }
}

module.exports = class AutobaseWakeup extends ReadyResource {
  constructor (handlers) {
    super()

    this.onrequest = handlers.onRequest
    this.onresponse = handlers.onResponse

    this.connections = new BufferMap()
  }

  addStream (stream) {
    const wakeup = new WakeupStream(stream, this)
    stream.on('close', () => { this.connections.delete(wakeup.key) })
  }

  requestWakeup (key) {
    for (const conn of this.connections.values()) conn.requestWakeup(key)
  }

  broadcastWakeup (wakeup) {
    if (wakeup === null) return
    for (const conn of this.connections.values()) conn.sendWakeup(wakeup)
  }

  sendWakeup (key, wakeup) {
    if (wakeup === null) return false

    const conn = this.connections.get(key)
    if (!conn) return false

    conn.sendWakeup(wakeup)
    return true
  }

  async destroy () {
    for (const conn of this.connections.values()) conn.destroy()
  }
}

function attachMuxer (stream) {
  if (Protomux.isProtomux(stream)) return stream
  if (stream.noiseStream.userData) return stream.noiseStream.userData
  const mux = Protomux.from(stream.noiseStream)
  stream.noiseStream.userData = mux
  return mux
}
