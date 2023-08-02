const safetyCatch = require('safety-catch')

const MAX_WAIT = 2 * 60 * 1000
const DEFAULT_INTERVAL = 10 * 1000

module.exports = class Timer {
  constructor (handler, interval, opts = {}) {
    this.handler = handler || noop
    this.interval = interval || DEFAULT_INTERVAL
    this.limit = opts.limit || MAX_WAIT

    this._executing = null

    this._interval = this.interval
    this._timer = null
    this._start = null
    this._stopped = false

    this._unref = opts.unref !== false
    this._timerCallback = this._executeBackground.bind(this, false)
  }

  _executeBackground (triggered) {
    this._executing = this._execute(triggered)
    this._executing.catch(safetyCatch) // make sure it doesnt crash in the bg
  }

  async _execute (triggered) {
    await this.handler(triggered)
    this.start = null
    this._executing = null
    this.bump()
  }

  async bump () {
    if (this._stopped || this._executing) return

    if (!this._start) this._start = Date.now()
    else if (Date.now() - this._start > this.limit) return

    const interval = random2over1(this._interval)

    clearTimeout(this._timer)
    this._timer = setTimeout(this._timerCallback, interval)
    if (this._unref && this._timer.unref) this._timer.unref()
  }

  async trigger () {
    if (this._stopped) return
    if (this._executing) await this._executing
    if (this._stopped) return

    clearTimeout(this._timer)

    this._executeBackground(true)

    return this.bump()
  }

  reset () {
    if (this._interval === this.interval) return
    this._interval = this.interval
    this.bump()
  }

  extend () {
    this._interval <<= 1
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

// random value x between n <= x < 2n
function random2over1 (n) {
  return n + Math.random() * n
}

function noop () {}
