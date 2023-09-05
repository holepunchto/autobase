const b4a = require('b4a')
const c = require('compact-encoding')
const ReadyResource = require('ready-resource')

const WakeupEntry = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.key)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    return {
      key: c.fixed32.decode(state),
      length: c.uint.decode(state)
    }
  }
}

module.exports = class AutoWakeup extends ReadyResource {
  constructor (base) {
    super()

    this.base = base
    this.flushing = null

    this._rootStore = getRootCorestore(this.base.store)
    this._addBound = this.add.bind(this)
    this._preupdateBound = this._preupdate.bind(this)
    this._needsFlush = false
    this._map = new Map()
  }

  [Symbol.iterator] () {
    return this._map.values()
  }

  async _preupdate (batch, key) {
    this.queue(key, batch.length)
    await this.flush()
    this.base._onwakeup()
  }

  async _save () {
    const slab = b4a.allocUnsafe(8 + this._map.size * 40) // 32 + 8
    const state = { start: 0, end: 0, buffer: slab }

    c.uint.encode(state, this._map.size)
    for (const m of this._map.values()) WakeupEntry.encode(state, m)

    await this.base.local.setUserData('autobase/wakeup', slab.subarray(0, state.start))
  }

  async _load () {
    const buffer = await this.base.local.getUserData('autobase/wakeup')
    if (!buffer) return

    const state = { start: 0, end: buffer.byteLength, buffer }

    let len = c.uint.decode(state)
    while (len-- > 0) {
      const m = WakeupEntry.decode(state)
      this._map.set(b4a.toString(m.key, 'hex'), m)
    }
  }

  async _open () {
    await this._load()

    this._rootStore.setMaxListeners(0)
    this._rootStore.on('core-open', this._addBound)

    for (const core of this._rootStore.cores.values()) {
      if (core.opened === false) await core.ready().catch(noop)
      if (!core.closing) this.add(core)
    }
  }

  async _close () {
    this._rootStore.removeListener('core-open', this._addBound)

    for (const core of this._rootStore.cores.values()) {
      if (core.opened && !core.closing) this.remove(core)
    }

    this._map.clear()

    while (this.flushing) {
      try {
        await this.flushing
      } catch {}
    }
  }

  queue (key, length) {
    const hex = b4a.toString(key, 'hex')
    const m = this._map.get(hex)

    if (m && m.length > length) {
      return false
    }

    this._needsFlush = true
    this._map.set(hex, { key, length })

    return true
  }

  unqueue (key, length) {
    const hex = b4a.toString(key, 'hex')
    const m = this._map.get(hex)

    if (!m) return true
    if (m.length > length) return false

    this._needsFlush = true
    this._map.delete(hex)

    return true
  }

  async flush () {
    if (this.closing) throw new Error('Closing')

    // wait for someone
    if (this.flushing) await this.flushing

    // if another still active they flushed us
    if (this.flushing) return this.flushing

    if (this._needsFlush === false) return
    this._needsFlush = false

    try {
      this.flushing = this._save()
      return await this.flushing
    } finally {
      this.flushing = null
    }
  }

  add (core) {
    if (core.writable || !core.opened) return // no need to track local cores...
    return this._add(core.core)
  }

  remove (core) {
    return this._remove(core.core)
  }

  _add (core) {
    const userData = core.header.userData // unsafe but sync so much less race-condition-ey
    let referrer = null

    for (const { key, value } of userData) {
      if (key === 'referrer') referrer = value
      else if (key === 'autobase/view') return false
    }

    if (referrer === null || !b4a.equals(referrer, this.base.key)) return false

    core.preupdate = this._preupdateBound

    return true
  }

  _remove (core) {
    if (core.preupdate !== this._preupdateBound) return false
    core.preupdate = null
    return true
  }
}

function getRootCorestore (store) {
  return store._root // TODO: make public
}

function noop () {}
