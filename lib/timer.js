const safetyCatch = require('safety-catch')

const MAX_WAIT = 2 * 60 * 1000
const DEFAULT_INTERVAL = 10 * 1000

module.exports = class Timer {
  constructor (handler, interval, opts = {}) {
    this.handler = handler || noop
    this.interval = interval || DEFAULT_INTERVAL
    this.limit = opts.limit || MAX_WAIT

    this.execute = this._execute.bind(this)
    this._executing = null

    this._timer = null
    this._start = null
    this._stopped = false

    this._unref = opts.unref !== false

    this._timerCallback = () => {
      this._executing = this.execute().catch(safetyCatch)
    }
  }

  async _execute () {
    await this.handler()
    this.start = null
    this._executing = null
    this.bump()
  }

  async bump () {
    if (this._stopped || this._executing) return

    if (!this._start) this._start = Date.now()
    else if (Date.now() - this._start > this.limit) return

    const interval = random3over2(this.interval)

    clearTimeout(this._timer)
    this._timer = setTimeout(this._timerCallback, interval)
    if (this._unref && this._timer.unref) this._timer.unref()
  }

  async trigger () {
    if (this._stopped) return
    if (this._executing) await this._executing
    if (this._stopped) return

    clearTimeout(this._timer)

    this._timerCallback()

    return this.bump()
  }

  reset (handler, interval) {
    this.stop()
    if (handler) this.handler = handler
    if (interval) this.interval = interval
    this._stopped = false
    this.bump()
  }

  stop () {
    if (this._timer) clearTimeout(this._timer)
    this._timer = null
    this._start = null
    this._stopped = true
  }

  unref () {
    if (this._timer && this._timer.unref) this._timer.unref()
  }
}

function random3over2 (n) {
  return n + Math.random() * n / 2
}

function noop () {}
