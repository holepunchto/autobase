const { eq } = require('../clock')

module.exports = class AppliedView {
  constructor (autobase, { applied, clock }) {
    this.autobase = autobase
    this.applied = applied
    this.clock = clock
    this.popped = 0
    this.head = this.applied.length ? this.applied[this.applied.length - 1] : null
    this._ite = null
  }

  async pop () {
    while (this.head && eq(this.head.clock, this.clock)) {
      this.head = this.applied[this.applied.length - ++this.popped]
    }
    if (this.head) {
      console.log('head here is:', this.head)
      this.clock = this.head.clock
      return this.head
    }
    if (!this._ite) {
      console.log('creating causal stream for clock:', this.clock)
      const stream = this.autobase.createCausalStream({ clock: this.clock })
      this._ite = stream[Symbol.asyncIterator]()
    }
    const next = await this._ite.next()
    if (!next.done) {
      this.clock = next.value.clock
    }
    return null
  }

  destroy () {
    if (!this._ite) return
    return this._ite.return()
  }
}
