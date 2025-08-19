const { EventEmitter } = require('events')
const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const b4a = require('b4a')

const { OplogMessage } = require('./messages')

class DigestCore extends ReadyResource {
  constructor (core, paused) {
    super()

    this.core = core
    this.paused = paused
    this.closed = false
    this.digest = null
    this.length = this.core.length
  }

  async _open () {
    await this.core.ready()
    this.core.on('append', this.updateBackground.bind(this))
    this.updateBackground()
  }

  _close () {
    this.closed = true
    return this.core.close()
  }

  pause () {
    this.paused = true
  }

  resume () {
    if (!this.paused) return
    this.paused = false
    this.updateBackground()
  }

  async update () {
    if (this.core.length <= this.length || this.closed || this.paused) return

    const length = this.core.length
    const value = await this.core.get(length - 1)

    if (length <= this.length || this.closed || !value.digest || this.paused) return

    const digest = await this._inflateDigest(length, value.digest)

    if (length <= this.length || this.closed || !digest || this.paused) return

    this.length = length
    this.digest = digest

    this.emit('update')
  }

  updateBackground () {
    return this.update().catch(safetyCatch)
  }

  async _inflateDigest (length, dig) {
    if (!dig) return null

    if (dig.pointer === 0) {
      return { key: dig.key, at: length }
    }

    const len = length - dig.pointer

    if (this.digest && len === this.digest.at) return this.digest
    if (len <= 0) return null

    const { digest } = await this.core.get(len - 1)
    if (!digest || !digest.key) return null

    return { key: digest.key, at: len }
  }
}

module.exports = class PeerSet extends EventEmitter {
  constructor (base) {
    super()

    this.base = base
    this.key = base.core.key
    this.peers = new Map()

    this.onupdate = this.checkDigest.bind(this)
  }

  add (key) {
    const hex = b4a.toString(key, 'hex')
    if (this.peers.has(hex)) return

    const core = this.base.store.get({
      key,
      valueEncoding: OplogMessage,
      encryption: this.base.getWriterEncryption()
    })

    const session = new DigestCore(core)
    session.on('update', this.onupdate)

    this.peers.set(hex, session)
  }

  async remove (key) {
    const hex = b4a.toString(key, 'hex')
    const session = this.peers.get(hex)

    if (!session) return

    await session.close()
    this.peers.delete(hex)
  }

  query () {
    const maj = (this.peers.size >> 1) + 1

    const tally = new Map()
    for (const core of this.peers.values()) {
      if (!core.digest) continue

      const hex = b4a.toString(core.digest.key, 'hex')
      let count = tally.get(hex) || 0

      if (++count >= maj) return core.digest.key

      tally.set(hex, count)
    }

    return null
  }

  checkDigest () {
    try {
      const key = this.query()
      if (!key || b4a.equals(key, this.key)) return

      this.key = key
      this.emit('update', this.key)
    } catch (err) {
      this.emit('error', err)
    }
  }

  update () {
    const updates = []
    for (const session of this.peers.values()) {
      updates.push(session.update())
    }

    return Promise.all(updates)
  }

  close () {
    const closing = []
    for (const session of this.peers.values()) {
      closing.push(session.close())
    }

    return Promise.all(closing)
  }
}
