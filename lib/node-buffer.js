const DEFAULT_SIZE = 32

module.exports = class NodeBuffer {
  constructor (offset, hwm) {
    this.hwm = hwm || DEFAULT_SIZE
    this.defaultHwm = this.hwm
    this.mask = this.hwm - 1
    this.top = 0
    this.btm = 0
    this.buffer = new Array(this.hwm)
    this.offset = offset || 0
    this.length = this.offset
  }

  get size () {
    return this.length - this.offset
  }

  isEmpty () {
    return this.length === this.offset
  }

  isFull () {
    return this.size === this.buffer.length
  }

  grow () {
    this.hwm <<= 1

    const size = this.size
    const buffer = new Array(this.hwm)
    const mask = this.hwm - 1

    for (let i = 0; i < size; i++) {
      buffer[i] = this.buffer[(this.btm + i) & this.mask]
    }

    this.mask = mask
    this.top = size
    this.btm = 0
    this.buffer = buffer
  }

  push (data) {
    if (this.isFull()) this.grow()

    this.buffer[this.top] = data
    this.top = (this.top + 1) & this.mask

    return this.length++
  }

  shift () {
    if (this.isEmpty()) return null

    const last = this.buffer[this.btm]

    this.buffer[this.btm] = undefined
    this.btm = (this.btm + 1) & this.mask
    this.offset++

    // reset on empty
    if (this.isEmpty() && this.hwm !== this.defaultHwm) {
      this.buffer = new Array(this.defaultHwm)
      this.hwm = this.buffer.length
      this.mask = this.hwm - 1
      this.top = this.btm = 0
    }

    return last
  }

  get (seq) {
    if (seq < this.offset || seq >= this.length) return null
    return this.buffer[(this.btm + (seq - this.offset)) & this.mask]
  }
}
