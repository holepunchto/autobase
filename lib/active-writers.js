const b4a = require('b4a')

module.exports = class ActiveWriters {
  constructor () {
    this.map = new Map()
  }

  get size () {
    return this.map.size
  }

  [Symbol.iterator] () {
    return this.map.values()
  }

  get (key) {
    return this.map.get(b4a.toString(key, 'hex')) || null
  }

  has (key) {
    return this.get(key) !== null
  }

  add (writer) {
    this.map.set(b4a.toString(writer.core.key, 'hex'), writer)
  }

  delete (writer) {
    this.map.delete(b4a.toString(writer.core.key, 'hex'))
  }

  clear () {
    const p = []
    for (const w of this.map.values()) p.push(w.close())
    this.map.clear()

    return Promise.all(p)
  }
}
