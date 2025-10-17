const SignalPromise = require('signal-promise')

module.exports = class AppendBatch {
  constructor(base) {
    this.base = base
    this.blocks = []
    this.closed = false
    this.flushing = false
    this._flushed = null
  }

  async _acquire() {
    while (this.base.activeBatch !== null && this.base.activeBatch !== this)
      await this.base.activeBatch.flushed()
    this.base.activeBatch = this
  }

  async append(value) {
    if (this.base.opened === false) await this.base.ready()
    if (this.base._advancing !== null) await this.base._advancing

    if (this.closed) throw new Error('Batch is closed')
    if (this.base.activeBatch !== this) await this._acquire()
    if (this.closed) throw new Error('Batch is closed')

    this.blocks.push(value)
    return this.base.local.length + this.blocks.length
  }

  async flush() {
    if (this.closed) throw new Error('Batch is closed')
    if (this.flushing) return this.flushed()
    this.flushing = true
    if (this.blocks.length) await this.base._appendBatch(this.blocks)
    return this.close()
  }

  flushed() {
    if (this.closed) return Promise.resolve()
    if (this._flushed) return this._flushed.wait()
    this._flushed = new SignalPromise()
    return this._flushed.wait()
  }

  close() {
    if (this.base.activeBatch !== this) return
    this.base.activeBatch = null
    this.closed = true
    if (this._flushed) this._flushed.notify(null)
  }
}
