const c = require('compact-encoding')
const ReadyResource = require('ready-resource')
const Protomux = require('protomux')
const BufferMap = require('tiny-buffer-map')
const { Clock } = require('./messages')

const VERSION = 1

const Response = {
  preencode (state, m) {
    Clock.preencode(state, m.writers)
  },
  encode (state, m) {
    Clock.encode(state, m.writers)
  },
  decode (state) {
    return {
      writers: Clock.decode(state)
    }
  }
}

const Handshake = {
  preencode (state, m) {
    c.uint.preencode(state, VERSION)
    c.fixed32.preencode(state, m.capability)
  },
  encode (state, m) {
    c.uint.encode(state, VERSION)
    c.fixed32.encode(state, m.capability)
  },
  decode (state) {
    return {
      version: c.uint.decode(state),
      capability: c.fixed32.decode(state)
    }
  }
}

class Connection {
  constructor (channel, session) {
    this.channel = channel
    this.session = session
    this.stream = this.channel._mux.stream

    this.request = channel.messages[0]
    this.response = channel.messages[1]

    this.opened = false
  }

  open () {
    this.channel.userData = this
    this.channel.open({ capability: this._capability(this.stream.isInitiator) })

    const conn = new WakeupStream(channel, this)
    stream.on('close', () => this.connections.delete(conn.key))
  }

  onopen ({ capability, version }) {
    if (version > VERSION) throw new Error(`Unsupported version: ${v}`)

    if (b4a.equals(capability, this._capability(!isInitiator))) {
      throw new Error('Invalid capability') // todo: should we bail silently
    }

    this.opened = true

    const hex = b4a.toString(this.stream.remotePublicKey, 'hex')
    this.session.connections.set(hex, channel)
  }

  _capability (isInitiator) {
    return caps.replicate(isInitiator, this.session.id, this.stream.handshakeHash)
  }
}

class WakeupSession {
  constructor (wakeup, id, handlers) {
    this.wakeup = wakeup
    this.id = id
    this.connections = new Map()

    this._onrequest = handlers.onrequest
    this._onresponse = handlers.onresponse

    for (const conn of wakeup.connections) this.register(conn)
  }

  register (stream) {
    const mux = getMuxer(stream)
    const openChannel = this._openChannel.bind(this, mux)

    mux.pair({ protocol: 'autobase/wakeup', id: this.core.discoveryKey }, openChannel)

    conn.messages[0].send()
  }

  onrequest (channel) {
    if (!channel.opened) throw new Error('Channel not opened')
    return this._onrequest(channel)
  }

  // todo: should we process response before capability in some cases?
  onresponse (response, channel) {
    if (!channel.opened) throw new Error('Channel not opened')
    return this._onresponse(response, channel)
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
      onopen: onchannelopen,
      onclose: () => { this.connections.delete(hex) }
    })

    if (channel === null) return // already opened

    const connection = new Connection(stream, channel, this)

    connection.open()
  }

  destroy () {
    for (const conn of this.connections.values()) conn.close()
    this.mux.unpair({ protocol: 'autobase/wakeup', id: this.pair })
  }
}

module.exports = class AutobaseWakeup extends ReadyResource {
  constructor (handlers) {
    super()

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

function onrequest (c) {
  return c.userData.session.onrequest(c)
}

function onresponse (res, c) {
  return c.userData.session.onresponse(res, c)
}

function onopen (m, c) {
  return c.userData.onopen(m)
}
