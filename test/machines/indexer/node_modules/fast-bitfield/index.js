'use strict'

const ctz = require('count-trailing-zeros')

module.exports = () => new Bitfield()

class Page {
  constructor (level) {
    const buf = new Uint8Array(level ? 8456 : 4360)
    const b = buf.byteOffset

    this.buffer = buf
    this.bits = level ? null : new Uint32Array(buf.buffer, b, 1024)
    this.children = level ? new Array(32768) : null
    this.level = level

    this.allOne = level
      ? [
        new Uint32Array(buf.buffer, b, 1024),
        new Uint32Array(buf.buffer, b + 4096, 32),
        new Uint32Array(buf.buffer, b + 4224, 1)
      ]
      : [
        this.bits,
        new Uint32Array(buf.buffer, b + 4096, 32),
        new Uint32Array(buf.buffer, b + 4224, 1)
      ]

    this.oneOne = level
      ? [
        new Uint32Array(buf.buffer, b + 4228, 1024),
        new Uint32Array(buf.buffer, b + 8324, 32),
        new Uint32Array(buf.buffer, b + 8452, 1)
      ]
      : [
        this.bits,
        new Uint32Array(buf.buffer, b + 4228, 32),
        new Uint32Array(buf.buffer, b + 4356, 1)
      ]
  }
}

const ZEROS = [new Page(0), new Page(1), new Page(2), new Page(3)]
const MASK = new Uint32Array(32)
const MASK_INCL = new Uint32Array(32)

for (var i = 0; i < 32; i++) {
  MASK[i] = Math.pow(2, 31 - i) - 1
  MASK_INCL[i] = Math.pow(2, 32 - i) - 1
}

const LITTLE_ENDIAN = new Uint8Array(MASK.buffer, MASK.byteOffset, 1)[0] === 0xff

class Bitfield {
  constructor () {
    this.length = 32768
    this.littleEndian = LITTLE_ENDIAN

    this._path = new Uint16Array(5)
    this._offsets = new Uint16Array(this._path.buffer, this._path.byteOffset + 2, 4)
    this._parents = new Array(4).fill(null)
    this._page = new Page(0)
    this._allocs = 1
  }

  last () {
    var page = this._page
    var b = 0

    while (true) {
      for (var i = 2; i >= 0; i--) {
        const c = ctz(page.oneOne[i][b])
        if (c === 32) return -1
        b = (b << 5) + (31 - c)
      }

      this._path[page.level] = b
      if (!page.level) return defactor(this._path)
      page = page.children[b]
      b = 0
    }
  }

  set (index, bit) {
    const page = this._getPage(index, bit)
    if (!page) return false

    const i = this._path[0]
    const r = i & 31
    const b = i >>> 5
    const prev = page.bits[b]

    page.bits[b] = bit
      ? (prev | (0x80000000 >>> r))
      : (prev & ~(0x80000000 >>> r))

    const upd = page.bits[b]
    if (upd === prev) return false

    this._updateAllOne(page, b, upd)
    this._updateOneOne(page, b, upd)

    return true
  }

  get (index) {
    const page = this._getPage(index, false)
    if (!page) return false

    const i = this._path[0]
    const r = i & 31

    return (page.bits[i >>> 5] & (0x80000000 >>> r)) !== 0
  }

  iterator () {
    return new Iterator(this)
  }

  fill (val, start, end) {
    if (!start) start = 0
    if (val === true) return this._fillBit(true, start, end === 0 ? end : (end || this.length))
    if (val === false) return this._fillBit(false, start, end === 0 ? end : (end || this.length))
    this._fillBuffer(val, start, end === 0 ? end : (end || (start + 8 * val.length)))
  }

  grow () {
    if (this._page.level === 3) throw new Error('Cannot grow beyond ' + this.length)
    const page = this._page
    this._page = new Page(page.level + 1)
    this._page.children[0] = page
    if (this._page.level === 3) this.length = Number.MAX_SAFE_INTEGER
    else this.length *= 32768
  }

  _fillBuffer (buf, start, end) {
    if ((start & 7) || (end & 7)) throw new Error('Offsets must be a multiple of 8')

    start /= 8
    while (end > this.length) this.grow()
    end /= 8

    const offset = start
    var page = this._getPage(8 * start, true)

    while (start < end) {
      const delta = end - start < 4096 ? end - start : 4096
      const s = start - offset

      start += this._setPageBuffer(page, buf.subarray(s, s + delta), start & 1023)
      if (start !== end) page = this._nextPage(page, 8 * start)
    }
  }

  _fillBit (bit, start, end) {
    var page = this._getPage(start, bit)

    // TODO: this can be optimised a lot in the case of end - start > 32768
    // in that case clear levels of 32768 ** 2 instead etc

    while (start < end) {
      const delta = end - start < 32768 ? end - start : 32768
      start += this._setPageBits(page, bit, start & 32767, delta)
      if (start !== end) page = this._nextPage(page, start)
    }
  }

  _nextPage (page, start) {
    const i = ++this._offsets[page.level]
    return i === 32768
      ? this._getPage(start, true)
      : this._parents[page.level].children[i] || this._addPage(this._parents[page.level], i)
  }

  _setPageBuffer (page, buf, start) {
    new Uint8Array(page.bits.buffer, page.bits.byteOffset, page.bits.length * 4).set(buf, start)
    start >>>= 2
    this._update(page, start, start + (buf.length >>> 2) + (buf.length & 3 ? 1 : 0))
    return buf.length
  }

  _setPageBits (page, bit, start, end) {
    const s = start >>> 5
    const e = end >>> 5
    const sm = 0xffffffff >>> (start & 31)
    const em = ~(0xffffffff >>> (end & 31))

    if (s === e) {
      page.bits[s] = bit
        ? page.bits[s] | (sm & em)
        : page.bits[s] & ~(sm & em)
      this._update(page, s, s + 1)
      return end - start
    }

    page.bits[s] = bit
      ? page.bits[s] | sm
      : page.bits[s] & (~sm)

    if (e - s > 2) page.bits.fill(bit ? 0xffffffff : 0, s + 1, e - 1)

    if (e === 1024) {
      page.bits[e - 1] = bit ? 0xffffffff : 0
      this._update(page, s, e)
      return end - start
    }

    page.bits[e] = bit
      ? page.bits[e] | em
      : page.bits[e] & (~em)

    this._update(page, s, e + 1)
    return end - start
  }

  _update (page, start, end) {
    for (; start < end; start++) {
      const upd = page.bits[start]
      this._updateAllOne(page, start, upd)
      this._updateOneOne(page, start, upd)
    }
  }

  _updateAllOne (page, b, upd) {
    var i = 1

    do {
      for (; i < 3; i++) {
        const buf = page.allOne[i]
        const r = b & 31
        const prev = buf[b >>>= 5]
        buf[b] = upd === 0xffffffff
          ? (prev | (0x80000000 >>> r))
          : (prev & ~(0x80000000 >>> r))
        upd = buf[b]
        if (upd === prev) return
      }

      b += this._offsets[page.level]
      page = this._parents[page.level]
      i = 0
    } while (page)
  }

  _updateOneOne (page, b, upd) {
    var i = 1

    do {
      for (; i < 3; i++) {
        const buf = page.oneOne[i]
        const r = b & 31
        const prev = buf[b >>>= 5]
        buf[b] = upd !== 0
          ? (prev | (0x80000000 >>> r))
          : (prev & ~(0x80000000 >>> r))
        upd = buf[b]
        if (upd === prev) return
      }

      b += this._offsets[page.level]
      page = this._parents[page.level]
      i = 0

      if (upd === 0 && page) {
        // all zeros, non root -> free page
        page.children[this._offsets[page.level - 1]] = undefined
      }
    } while (page)
  }

  _getPage (index, createIfMissing) {
    factor(index, this._path)

    while (index >= this.length) {
      if (!createIfMissing) return null
      this.grow()
    }

    var page = this._page

    for (var i = page.level; i > 0 && page; i--) {
      const p = this._path[i]
      this._parents[i - 1] = page
      page = page.children[p] || (createIfMissing ? this._addPage(page, p) : null)
    }

    return page
  }

  _addPage (page, i) {
    this._allocs++
    page = page.children[i] = new Page(page.level - 1)
    return page
  }
}

class Iterator {
  constructor (bitfield) {
    this._bitfield = bitfield
    this._path = new Uint16Array(5)
    this._offsets = new Uint16Array(this._path.buffer, this._path.byteOffset + 2, 4)
    this._parents = new Array(4).fill(null)
    this._page = null
    this._allocs = bitfield._allocs

    this.seek(0)
  }

  seek (index) {
    this._allocs = this._bitfield._allocs

    if (index >= this._bitfield.length) {
      this._page = null
      return this
    }

    factor(index, this._path)

    this._page = this._bitfield._page
    for (var i = this._page.level; i > 0; i--) {
      this._parents[i - 1] = this._page
      this._page = this._page.children[this._path[i]] || ZEROS[i - 1]
    }

    return this
  }

  next (bit) {
    return bit ? this.nextTrue() : this.nextFalse()
  }

  nextFalse () {
    if (this._allocs !== this._bitfield._allocs) {
      // If a page has been alloced while we are iterating
      // and we have a zero page in our path we need to reseek
      // in case that page has been overwritten
      this.seek(defactor(this._path))
    }

    var page = this._page
    var b = this._path[0]
    var mask = MASK_INCL

    while (page) {
      for (var i = 0; i < 3; i++) {
        const r = b & 31
        const clz = Math.clz32((~page.allOne[i][b >>>= 5]) & mask[r])
        if (clz !== 32) return this._downLeftFalse(page, i, b, clz)
        mask = MASK
      }

      b = this._offsets[page.level]
      page = this._parents[page.level]
    }

    return -1
  }

  _downLeftFalse (page, i, b, clz) {
    while (true) {
      while (i) {
        b = (b << 5) + clz
        clz = Math.clz32(~page.allOne[--i][b])
      }

      b = (b << 5) + clz

      if (!page.level) break

      this._parents[page.level - 1] = page
      this._path[page.level] = b

      page = page.children[b]
      i = 3
      clz = b = 0
    }

    this._page = page
    this._path[0] = b

    return this._inc()
  }

  nextTrue () {
    var page = this._page
    var b = this._path[0]
    var mask = MASK_INCL

    while (page) {
      for (var i = 0; i < 3; i++) {
        const r = b & 31
        const clz = Math.clz32(page.oneOne[i][b >>>= 5] & mask[r])
        if (clz !== 32) return this._downLeftTrue(page, i, b, clz)
        mask = MASK
      }

      b = this._offsets[page.level]
      page = this._parents[page.level]
    }

    return -1
  }

  _downLeftTrue (page, i, b, clz) {
    while (true) {
      while (i) {
        b = (b << 5) + clz
        clz = Math.clz32(page.oneOne[--i][b])
      }

      b = (b << 5) + clz

      if (!page.level) break

      this._parents[page.level - 1] = page
      this._path[page.level] = b

      page = page.children[b]
      i = 3
      clz = b = 0
    }

    this._page = page
    this._path[0] = b

    return this._inc()
  }

  _inc () {
    const n = defactor(this._path)
    if (this._path[0] < 32767) this._path[0]++
    else this.seek(n + 1)
    return n
  }
}

function defactor (out) {
  return ((((out[3] * 32768 + out[2]) * 32768) + out[1]) * 32768) + out[0]
}

function factor (n, out) {
  n = (n - (out[0] = (n & 32767))) / 32768
  n = (n - (out[1] = (n & 32767))) / 32768
  out[3] = ((n - (out[2] = (n & 32767))) / 32768) & 32767
}
