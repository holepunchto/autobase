const POW = 2

module.exports = class HypercoreBisector {
  constructor (core, opts = {}) {
    this.core = core
    this.value = null
    this.seq = null
    this.invalid = false

    this._cmp = opts.cmp
    this._validate = opts.validate
    this._skip = opts.skip

    this._done = false
    this._galloping = true
    this._high = 0
    this._low = 1
  }

  async _get (idx) {
    this.seq = Math.min(Math.max(idx, 0), this.core.length - 1)
    const node = await this.core.get(this.seq)
    return node
  }

  async _gallop () {
    console.log('GALLOPING AND HIGH:', this._high, 'LENGTH:', this.core.length)
    if (this._high > this.core.length - 1) {
      // If the gallop advances beyond the length of the core, the value must be smaller than the smallest
      this._done = true
      this.value = null
      return false
    }

    let block = await this._get(this.core.length - this._low)
    if (this._skip) {
      const skip = this._skip(block)
      this._low += skip
      block = await this._get(this.core.length - this._low)
    }
    if (block === null || (this._validate && !this._validate(block))) {
      this.invalid = true
      this._done = true
      return false
    }

    const cmp = this._cmp(block)
    if (cmp === 0) {
      this._done = true
      this.value = block
      return false
    } else if (cmp < 0) {
      this._high = this._low
      this._low *= POW
      return true
    } else {
      this._galloping = false
      return true
    }
  }

  async _bisect () {
    console.log('BISECTING AND LOW IS:', this._low, 'HIGH IS:', this._high)
    if (this._low === 1 || this._high > this._low) {
      // If the bisect lower bound advances to the head, then the value must be larger than the largest
      this._done = true
      return false
    }

    let mid = Math.floor((this._low - this._high) / 2) + this._high
    let block = await this._get(this.core.length - mid)
    if (this._skip) {
      const skip = this._skip(block)
      mid += skip
      block = await this._get(this.core.length - mid)
    }
    if (block === null || (this._validate && !this._validate(block))) {
      this.invalid = true
      this._done = true
      return false
    }

    const cmp = this._cmp(block)
    if (cmp === 0) {
      this._done = true
      this.value = block
      return false
    } else if (mid - this._high === 1) {
      this._done = true
      this._value = null
      return true
    } else if (cmp < 0) {
      this._high = mid
    } else {
      this._low = mid + 1
    }

    return true
  }

  async step () {
    console.log('CALLING STEP WHEN DONE IS:', this._done)
    if (this._done) return false
    if (this._galloping) return this._gallop()
    return this._bisect()
  }

  async search () {
    while (await this.step());
    return this.value
  }
}
