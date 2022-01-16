const b = require('b4a')
const c = require('compact-encoding')
const { Node: NodeSchema, decodeKeys } = require('./nodes/messages')

module.exports = class KeyCompression {
  constructor (core) {
    this.core = core
    this.initialized = false
    this.byKey = new Map()
    this.bySeq = new Map()
  }

  async decompress (compressedClock, seq) {
    const clock = new Map()

    // Load all the pointers into the cache
    await Promise.all(compressedClock.map(({ key }) => this.resolvePointer(key)))

    for (const { key, length } of compressedClock) {
      const fullKey = this.bySeq.get(key.seq)[key.offset]
      clock.set(b.toString(fullKey, 'hex'), length)
    }

    if (!this.initialized) {
      this.initialized = true
    }

    return clock
  }

  async compress (clock, seq) {
    let keys = this.bySeq.get(seq)
    if (!keys) {
      keys = []
      this.bySeq.set(seq, keys)
    }
    const compressed = []

    if (!this.initialized && this.core.length > 0) {
      const headBuf = await this.core.get(this.core.length - 1)
      const state = { start: 0, end: headBuf.length, buffer: headBuf }
      const head = NodeSchema.decode(state)
      await this._decompress(head.clock, this.core.length - 1)
    }

    for (const [key, length] of clock) {
      let keyPointer = this.byKey.get(key)
      if (!keyPointer) {
        keys.push(b.from(key, 'hex'))
        keyPointer = { seq, offset: keys.length - 1 }
        this.byKey.set(key, keyPointer)
      }
      compressed.push({ key: keyPointer, length })
    }

    return { keys, clock: compressed }
  }

  truncate (length) {
    const badSeqs = []
    for (const seq of this.bySeq.keys()) {
      if (seq < length) continue
      badSeqs.push(seq)
    }
    for (const seq of badSeqs) {
      const keys = this.bySeq.get(seq)
      this.bySeq.delete(seq)
      for (const key of keys) {
        this.byKey.delete(b.toString(key, 'hex'))
      }
    }
  }

  async resolvePointer (keyPointer) {
    let keys = this.bySeq.get(keyPointer.seq)
    if (keys) return keys[keyPointer.offset]
    const block = await this.core.get(keyPointer.seq)
    keys = decodeKeys(block)
    this.bySeq.set(keyPointer.seq, keys)
    for (let i = 0; i < keys.length; i++) {
      this.byKey.set(b.toString(keys[i], 'hex'), { seq: keyPointer.seq, offset: i })
    }
    return keys[keyPointer.offset]
  }

  resolveKey (key) {
    const id = b.toString(key, 'hex')
    return this.byKey.get(id)
  }
}
