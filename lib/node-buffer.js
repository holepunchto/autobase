const DEFAULT_SIZE = 1024

module.exports = class NodeBuffer {
  constructor (offset, hwm) {
    this.hwm = hwm || DEFAULT_SIZE
    this.buffer = new Array(hwm)

    this.offset = offset || 0
    this.length = this.offset
    this.btm = this.offset
  }

  get size () {
    return this.length - this.btm
  }

  grow () {
    while (this.length - this.btm > this.hwm >> 1) this.hwm <<= 1 // grow
    while (this.length - this.btm < this.hwm >> 2) this.hwm >>= 1 // shrink
    const buffer = new Array(this.hwm)
    for (let i = this.btm; i < this.length; i++) {
      buffer[i - this.btm] = this.buffer[i - this.offset]
    }
    this.offset = this.btm
    this.buffer = buffer
  }

  push (data) {
    if ((this.length - this.offset) === this.buffer.length) this.grow()
    this.buffer[this.length - this.offset] = data
    return this.length++
  }

  shift () {
    const last = this.buffer[this.btm - this.offset]
    if (last === undefined) return null
    this.buffer[this.btm++ - this.offset] = undefined
    if (this.btm - this.offset >= this.hwm >> 2) this.grow() // shrink
    return last
  }

  get (seq) {
    if (seq < this.btm || seq >= this.length) return null
    return this.buffer[seq - this.offset]
  }

  isEmpty () {
    return this.buffer[this.btm - this.offset] === undefined
  }
}
