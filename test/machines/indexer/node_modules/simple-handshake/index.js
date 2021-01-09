var noise = require('noise-protocol')
var NoiseSymmetricState = require('noise-protocol/symmetric-state')
var NoiseHash = require('noise-protocol/hash')
var assert = require('nanoassert')
var EMPTY = Buffer.alloc(0)

function SimpleHandshake (isInitiator, opts) {
  if (!(this instanceof SimpleHandshake)) return new SimpleHandshake(isInitiator, opts)
  opts = opts || {}

  var pattern = opts.pattern || 'NN'
  var prolouge = opts.prolouge || EMPTY

  this.handshakeHash = null
  this.onstatickey = opts.onstatickey || function (_, cb) { cb() }
  this.onephemeralkey = opts.onephemeralkey || function (_, cb) { cb() }
  this.onhandshake = opts.onhandshake || function (_, cb) { cb() }

  this.state = noise.initialize(
    pattern,
    isInitiator,
    prolouge,
    opts.staticKeyPair,
    opts.ephemeralKeyPair,
    opts.remoteStaticKey,
    opts.remoteEphemeralKey
  )

  // initiators should send first message, so if initiator, waiting = false
  // while servers should await any message, so if not initiator, waiting = true
  this.waiting = isInitiator === false
  this.finished = false
  // Will hold the "split" for transport encryption after handshake
  this.split = null

  // ~64KiB is the max noise message length
  this._tx = Buffer.alloc(65535)
  this._rx = Buffer.alloc(65535)
}

SimpleHandshake.prototype.recv = function recv (data, cb) {
  var self = this
  assert(self.finished === false, 'Should not call recv if finished')
  assert(data != null, 'must have data')
  assert(data.byteLength <= self._rx.byteLength, 'too much data received')
  assert(self.waiting === true, 'Wrong state, not ready to receive data')
  assert(self.split == null, 'split should be null')

  var hasREBefore = self.state.re != null
  var hasRSBefore = self.state.rs != null
  try {
    self.split = noise.readMessage(self.state, data, self._rx)
  } catch (ex) {
    return self._finish(ex, null, cb)
  }

  self.waiting = false

  var hasREAfter = self.state.re != null
  var hasRSAfter = self.state.rs != null

  // e and s may come in the same message, so we always have to check static
  // after ephemeral. Assumption here (which holds for all official Noise handshakes)
  // is that e always comes before s
  if (hasREBefore === false && hasREAfter === true) {
    return self.onephemeralkey(self.state.re, checkStatic)
  }

  return checkStatic()

  function checkStatic (err) {
    if (err) return ondone(err)

    if (hasRSBefore === false && hasRSAfter === true) {
      return self.onstatickey(self.state.rs, ondone)
    }

    return ondone()
  }

  function ondone (err) {
    if (err) return self._finish(err, null, cb)

    var msg = self._rx.subarray(0, noise.readMessage.bytes)
    if (self.split) return self._finish(null, msg, cb)

    cb(null, msg)
  }
}

SimpleHandshake.prototype.send = function send (data, cb) {
  assert(this.finished === false, 'Should not call send if finished')
  assert(this.waiting === false, 'Wrong state, not ready to send data')
  assert(this.split == null, 'split should be null')

  data = data || EMPTY

  try {
    this.split = noise.writeMessage(this.state, data, this._tx)
  } catch (ex) {
    return this._finish(ex, null, cb)
  }

  this.waiting = true

  var buf = this._tx.subarray(0, noise.writeMessage.bytes)

  if (this.split != null) return this._finish(null, buf, cb)

  return cb(null, buf)
}

SimpleHandshake.prototype.destroy = function () {
  this._finish(null, null, function () {})
}

SimpleHandshake.prototype._finish = function _finish (err, msg, cb) {
  assert(this.finished === false, 'Already finished')
  const self = this

  self.finished = true
  self.waiting = false

  if (self.split) {
    self.handshakeHash = Buffer.alloc(NoiseHash.HASHLEN)
    NoiseSymmetricState.getHandshakeHash(self.state.symmetricState, self.handshakeHash)
  }
  if (err) return ondone(err)
  self.onhandshake(self.state, ondone)

  function ondone (err) {
    noise.destroy(self.state)

    cb(err, msg, self.split)

    // Should be sodium_memzero?
    self._rx.fill(0)
    self._tx.fill(0)
  }
}

SimpleHandshake.keygen = noise.keygen
SimpleHandshake.seedKeygen = noise.seedKeygen

module.exports = SimpleHandshake
