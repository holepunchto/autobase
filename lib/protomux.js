const c = require('compact-encoding')
const ReadyResource = require('ready-resource')
const Protomux = require('protomux')
const BufferMap = require('tiny-buffer-map')
const { Clock } = require('./messages')

const Response = {
  preencode (state, m) {
    c.uint.preencode(state, 1) // version
    Clock.preencode(state, m.writers)
  },
  encode (state, m) {
    c.uint.encode(state, 1) // version
    Clock.encode(state, m.writers)
  },
  decode (state) {
    const v = c.uint.decode(state)
    if (v > 1) throw new Error('Unsupported version: ' + v)

    return {
      writers: Clock.decode(state)
    }
  }
}

const Handshake = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.capability)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.capability)
  },
  decode (state) {
    return {
      capability: c.fixed32.decode(state)
    }
  }
}

class WakeupSession {
  constructor (wakeup, id, handlers) {
    this.wakeup = wakeup
    this.id = id
    this.connections = new Map()

    this.onrequest = handlers.onrequest
    this.onresponse = handlers.onresponse

    for (const conn of wakeup.connections) {
      this.register(conn)
    }
  }

  register (stream) {
    const mux = getMuxer(stream)
    const openChannel = this._openChannel.bind(this, mux)

    mux.pair({ protocol: 'hypercore/alpha', id: this.core.discoveryKey }, makePeer)
  }

  requestWakeup () {
    for (const conn of this.connections.values()) conn.messages[0].send()
  }

  broadcastWakeup (wakeup) {
    if (wakeup === null) return
    for (const conn of this.connections.values()) conn.messages[1].send(wakeup)
  }

  sendWakeup (key, wakeup) {
    if (wakeup === null) return false

    const conn = this.connections.get(key)
    if (!conn) return false

    conn.sendWakeup(wakeup)
    return true
  }

  _openChannel (stream) {
    const mux = getMuxer(stream)
    const isInitiator = stream.isInitiator
    const handshakeHash = stream.handshakeHash
    const hex = b4a.toString(stream.remotePublicKey, 'hex')

    const channel = mux.createChannel({
      userData: null,
      protocol: 'autobase/wakeup',
      id: this.session.id,
      handshake: Handshake,
      messages: [
        { encoding: c.none, onmessage: onrequest },
        { encoding: Response, onmessage: onresponse }
      ],
      onopen: ({ capability }) => {
        const expected = caps.replicate(!isInitiator, this.id, handshakeHash)
        if (b4a.equals(capability, expected)) {
          throw new Error('Invalid capability') // todo: should we bail silently
        }

        // todo: verify capability
        this.connections.set(hex, channel)
      },
      onclose: () => {
        this.connections.delete(hex)
      }
    })

    if (channel === null) return // already opened

    channel.userData = this

    const capability = caps.replicate(isInitiator, this.id, handshakeHash)

    channel.open({ capability })

    const conn = new WakeupStream(channel, this)
    stream.on('close', () => this.connections.delete(conn.key))

    conn.request.send()
  }

  destroy () {
    for (const conn of this.connections.values()) conn.close()
    this.mux.unpair({ protocol: 'autobase/wakeup', id: this.pair })
  }
}

module.exports = class AutobaseWakeup extends ReadyResource {
  constructor (handlers) {
    super()

    this.onrequest = handlers.onRequest
    this.onresponse = handlers.onResponse

    this.sessions = new Set()
    this.streams = new Set()
  }

  session (id) {
    const session = new WakeupSession(this, id)
    this.sessions.add(session)

    return session
  }

  addConnection (stream) {
    this.streams.add(stream)
    for (const s of this.sessions) s.register(stream)

    stream.on('close', () => { this.streams.delete(wakeup.key) })
  }

  async destroy () {
    for (const stream of this.streams.values()) conn.destroy()
  }
}

function getMuxer (stream) {
  if (Protomux.isProtomux(stream)) return stream
  if (stream.noiseStream.userData) return stream.noiseStream.userData
  const mux = Protomux.from(stream.noiseStream)
  stream.noiseStream.userData = mux
  return mux
}

function onrequest (req, c) {
  return c.userData.onrequest(req, c)
}

function onresponse (res, c) {
  return c.userData.onresponse(res, c)
}
