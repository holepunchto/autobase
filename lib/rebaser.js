module.exports = class Rebaser {
  constructor (index, opts = {}) {
    this.index = index
    this.buf = []
    this.added = 0
    this.removed = 0
    this.truncation = 0

    this._alreadyIndexed = false
    this._map = opts.map
  }

  _getIndexLength () {
    return this.index.length - this.truncation
  }

  async _getIndexHead () {
    const length = this._getIndexLength()
    if (length <= 1) return null
    return this.index.get(length - 1)
  }

  async update (node) {
    if (this.index.length <= 1) {
      this.added++
      this.buf.push(node)
      return false
    }

    let indexNode = await this._getIndexHead()

    if (indexNode && node.lte(indexNode) && indexNode.gte(node)) {
      this._alreadyIndexed = true
      return true
    }

    while (indexNode && indexNode.contains(node)) {
      this.removed++
      this.truncation += indexNode.batch
      indexNode = await this._getIndexHead()
    }

    this.added++
    this.buf.push(node)
    return false
  }

  async commit (opts = {}) {
    const leftover = this._getIndexLength()

    if (!this._alreadyIndexed && leftover > 1) {
      this.removed += leftover - 1
      await this.index.truncate(1)
    } else if (this.truncation) {
      await this.index.truncate(this.index.length - this.truncation)
    }

    while (this.buf.length) {
      const next = this.buf.pop()
      let appending = this._map ? [] : [next]
      if (this._map) {
        const res = await this._map(next)
        if (Array.isArray(res)) appending.push(...res)
        else appending.push(res)
        appending = appending.map(v => ({ ...next, value: v }))
      }
      await this.index.append(appending)
    }

    if (this.index.commit && opts.flush !== false) return this.index.commit(opts)
  }
}
