const b4a = require('b4a')
const safetyCatch = require('safety-catch')

// TODO: should really be expressable with a corestore

const LINGER_TIME = 30_000

module.exports = class CorePool {
  constructor () {
    this.pool = new Map()
  }

  linger (core) {
    const hex = b4a.toString(core.key, 'hex')
    if (this.pool.has(hex)) return

    const session = core.session()

    const wrap = {
      session,
      timeout: setTimeout(ontimeout, LINGER_TIME, this, session)
    }

    this.pool.set(hex, wrap)
  }

  get (key) {
    const hex = b4a.toString(key, 'hex')
    const wrap = this.pool.get(hex)
    if (!wrap) return null

    this.pool.delete(hex)
    clearTimeout(wrap.timeout)
    return wrap.session
  }

  clear () {
    const closing = []
    for (const { session, timeout } of this.pool.values()) {
      clearTimeout(timeout)
      closing.push(session.close())
    }
    this.pool.clear()
    return Promise.all(closing)
  }
}

function ontimeout (pool, core) {
  const hex = b4a.toString(core.key, 'hex')
  core.close().catch(safetyCatch)
  pool.pool.delete(hex)
}
