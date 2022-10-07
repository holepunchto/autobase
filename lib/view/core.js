const ReadyResource = require('ready-resource')

const PROMISES = Symbol.for('hypercore.promises')

class LinearizedCore extends ReadyResource {
  constructor (view, id, opts = {}) {
    super()
    this[PROMISES] = true
    this.byteLength = 0
    this.writable = opts.writable !== false
    this.valueEncoding = opts.valueEncoding

    this._view = view
    this._id = id
    this._sessionIdx = -1
    this._snapshotted = opts.snapshot === true

    this._activeRequests = []
    this._unwrapped = opts.unwrap === true
    this._pinned = opts.pin === true
    this._checkout = opts.checkout || null
  }

  // LinearizedCoreSession API

  get status () {
    return this._view._coreStatus(this)
  }

  unwrap () {
    return this.session({ unwrap: true })
  }

  wrap () {
    return this.session({ unwrap: false })
  }

  // Hypercore API

  _close () {
    for (const req of this._activeRequests) {
      req.cancel()
    }
    return this._view._coreClose(this)
  }

  _open () {
    return this._view._coreReady(this)
  }

  get length () {
    return this._view._coreLength(this)
  }

  append (block, opts) {
    return this._view._coreAppend(this, block, opts)
  }

  update () {
    return this._view._coreUpdate(this)
  }

  snapshot (opts) {
    return this.session({ ...opts, snapshot: true })
  }

  checkout (clock) {
    return this.session({ checkout: clock })
  }

  session (opts) {
    return this._view._coreSession(this, {
      pin: this._pinned,
      unwrap: this._unwrapped,
      ...opts
    })
  }

  async get (seq, opts = {}) {
    if (!this.opened) await this.ready()
    let block = await this._view._coreGet(this, seq, { ...opts, active: this._activeRequests })
    if (!block) return null
    if (this._unwrapped) block = block.value
    if (this.valueEncoding && !opts.valueEncoding) {
      return this.valueEncoding.decode(block)
    } else if (opts.valueEncoding) {
      return opts.valueEncoding.decode(block)
    }
    return block
  }
}

module.exports = LinearizedCore
