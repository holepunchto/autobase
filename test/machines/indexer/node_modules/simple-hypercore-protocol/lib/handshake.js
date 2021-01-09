const SH = require('simple-handshake')
const DH = require('noise-protocol/dh')
const crypto = require('hypercore-crypto')
const varint = require('varint')

module.exports = class ProtocolHandshake {
  constructor (initiator, payload, opts, done) {
    this.options = opts
    this.ondone = done
    this.buffer = null
    this.length = 0
    this.remotePayload = null
    this.payload = payload
    this.keyPair = opts.keyPair || ProtocolHandshake.keyPair()
    this.remotePublicKey = null
    this.onrecv = onrecv.bind(this)
    this.onsend = onsend.bind(this)
    this.destroyed = false
    this.noise = SH(initiator, {
      pattern: 'XX',
      onhandshake,
      staticKeyPair: this.keyPair,
      onstatickey: onstatickey.bind(this)
    })

    const self = this
    if (this.noise.waiting === false) process.nextTick(start, this)

    function onhandshake (state, cb) {
      process.nextTick(finish, self)
      cb(null)
    }
  }

  recv (data) {
    if (this.destroyed) return

    if (this.buffer) this.buffer = Buffer.concat([this.buffer, data])
    else this.buffer = data

    while (!this.destroyed && !this.noise.finished) {
      if (!this.buffer || this.buffer.length < 3) return
      if (this.length) {
        if (this.buffer.length < this.length) return
        const message = this.buffer.slice(0, this.length)
        this.buffer = this.length < this.buffer.length ? this.buffer.slice(this.length) : null
        this.length = 0
        this.noise.recv(message, this.onrecv)
      } else {
        this.length = varint.decode(this.buffer, 0)
        this.buffer = this.buffer.slice(varint.decode.bytes)
      }
    }
  }

  destroy (err) {
    if (this.destroyed) return
    this.destroyed = true
    if (!this.noise.finished) this.noise.destroy()
    this.ondone(err)
  }

  static keyPair (seed) {
    const obj = {
      // suboptimal but to reduce secure memory overhead on linux with default settings
      // better fix is to batch mallocs in noise-protocol
      publicKey: Buffer.alloc(DH.PKLEN),
      secretKey: Buffer.alloc(DH.SKLEN)
    }

    if (seed) DH.generateSeedKeypair(obj.publicKey, obj.secretKey, seed)
    else DH.generateKeypair(obj.publicKey, obj.secretKey)

    return obj
  }
}

function finish (self) {
  if (self.destroyed) return
  self.destroyed = true
  // suboptimal but to reduce secure memory overhead on linux with default settings
  // better fix is to batch mallocs in noise-protocol
  const split = { rx: Buffer.from(self.noise.split.rx), tx: Buffer.from(self.noise.split.tx) }
  crypto.free(self.noise.split.rx)
  crypto.free(self.noise.split.tx)
  self.ondone(null, self.remotePayload, split, self.buffer, self.remotePublicKey, self.noise.handshakeHash)
}

function start (self) {
  if (self.destroyed) return
  self.noise.send(self.payload, self.onsend)
}

function onsend (err, data) {
  if (err) return this.destroy(err)
  const buf = Buffer.allocUnsafe(varint.encodingLength(data.length) + data.length)
  varint.encode(data.length, buf, 0)
  data.copy(buf, varint.encode.bytes)
  this.options.send(buf)
}

function onrecv (err, data) { // data is reused so we need to copy it if we use it
  if (err) return this.destroy(err)
  if (data && data.length) this.remotePayload = Buffer.concat([data])
  if (this.destroyed || this.noise.finished) return

  if (this.noise.waiting === false) {
    this.noise.send(this.payload, this.onsend)
  }
}

function onstatickey (remoteKey, done) {
  this.remotePublicKey = Buffer.concat([remoteKey])
  if (this.options.onauthenticate) this.options.onauthenticate(this.remotePublicKey, done)
  else done(null)
}
