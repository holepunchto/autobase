const BufferMap = require('tiny-buffer-map')

// This is basically just a Map atm, but leaving it as an abstraction for now
// in case we wanna optimize it for our exact usecase

module.exports = class Clock {
  constructor () {
    this.seen = new BufferMap()
  }

  get size () {
    return this.seen.size
  }

  has (key) {
    return this.seen.has(key)
  }

  includes (key, length) {
    return this.seen.has(key) && this.seen.get(key) >= length
  }

  get (key) {
    return this.seen.get(key) || 0
  }

  set (key, len) {
    this.seen.set(key, len)
    return len
  }

  add (clock) {
    for (const [key, l] of clock) {
      if (this.get(key) < l) this.set(key, l)
    }
  }

  [Symbol.iterator] () {
    return this.seen[Symbol.iterator]()
  }
}
