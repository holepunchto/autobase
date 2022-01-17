const HashMap = require('turbo-hash-map')
const { Node: NodeSchema, decodeKeys } = require('./nodes/messages')

module.exports = class KeyCompressor {
  constructor (core) {
    this.core = core
    this.initialized = false
    this.byKey = new HashMap()
    this.bySeq = new Map()
  }

  async decompress (compressedClock, seq) {
    const clock = new HashMap()
    if (!compressedClock) return clock

    // Load all the pointers into the cache
    const uniqueSeqs = new Set()
    for (const { key } of compressedClock) {
      if (this.bySeq.has(key.seq)) continue
      uniqueSeqs.add(key.seq)
    }
    const seqs = [...uniqueSeqs]

    const blocks = await Promise.all(seqs.map(seq => this.core.get(seq)))
    for (let i = 0; i < blocks.length; i++) {
      const keys = decodeKeys(blocks[i])
      this.bySeq.set(seqs[i], keys)
      for (let j = 0; j < keys.length; j++) {
        this.byKey.set(keys[j], { seq: seqs[i], offset: j })
      }
    }

    for (const { key, length } of compressedClock) {
      clock.set(this.bySeq.get(key.seq)[key.offset], length)
    }

    if (!this.initialized) {
      this.initialized = true
    }

    return clock
  }

  async compress (clock, seq) {
    const keys = []
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
        keys.push(key)
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
        this.byKey.delete(key)
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
      this.byKey.set(keys[i], { seq: keyPointer.seq, offset: i })
    }
    return keys[keyPointer.offset]
  }

  resolveKey (key) {
    return this.byKey.get(key)
  }
}
