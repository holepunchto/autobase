const safetyCatch = require('safety-catch')

const MAX_WAIT = 2 * 60 * 1000
const DEFAULT_INTERVAL = 10 * 1000

module.exports = class Timer {
  constructor (handler, interval, opts = {}) {
    this.handler = handler || noop
    this.interval = interval || DEFAULT_INTERVAL
    this.limit = opts.limit || MAX_WAIT

    this._executing = null

    this._limit = random2over1(this.limit)
    this._timer = null
    this._resolve = null
    this._start = 0
    this._stopped = false
    this._asap = false
    this._standalone = new Set()

    this._unref = opts.unref !== false
    this._timerCallback = this._executeBackground.bind(this)
  }

  _executeBackground () {
    this._executing = this._execute()
    this._executing.catch(safetyCatch) // make sure it doesnt crash in the bg
  }

  async _execute () {
    this._asap = false
    await this.handler()
    this._start = 0
    this._executing = null
    this.bump()
  }

  bump () {
    if (this._stopped || this._executing || this._asap) return

    if (!this._start) this._start = Date.now()
    else if (Date.now() - this._start > this._limit) return

    const interval = random2over1(this.interval)

    clearTimeout(this._timer)
    this._timer = setTimeout(this._timerCallback, interval)
    if (this._unref && this._timer.unref) this._timer.unref()
  }

  async trigger () {
    if (this._stopped) return
    if (this._executing) await this._executing
    if (this._stopped) return

    clearTimeout(this._timer)
    this._timer = null

    this._executeBackground()
    await this._executing
  }

  async flush () {
    if (this._executing) await this._executing
  }

  // business-as-usual
  bau () {
    if (!this._asap) return
    this._asap = false
    this.bump()
  }

  asap () {
    if (this._asap) return
    this._asap = true

    const interval = Math.floor(Math.random() * this.interval / 3)
    clearTimeout(this._timer)
    this._timer = setTimeout(this._timerCallback, interval)
    if (this._unref && this._timer.unref) this._timer.unref()
  }

  stop () {
    if (this._timer) clearTimeout(this._timer)
    this._timer = null
    this._start = 0
    this._asap = false
    this._stopped = true

    for (const { timer, resolve } of this._standalone) {
      clearTimeout(timer)
      resolve()
    }

    this._standalone.clear()
  }

  asapStandalone () {
    const interval = Math.floor(Math.random() * this.interval / 3)
    return new Promise((resolve) => {
      const ref = { timer: null, resolve }
      ref.timer = setTimeout(resolveStandalone, interval, ref, this._standalone)
      if (ref.timer.unref) ref.timer.unref()
      this._standalone.add(ref)
    })
  }

  unref () {
    if (this._timer && this._timer.unref) this._timer.unref()
  }
}

function resolveStandalone (ref, set) {
  set.delete(ref)
  ref.resolve()
}

// random value x between n <= x < 2n
function random2over1 (n) {
  return Math.floor(n + Math.random() * n)
}

function noop () {}
