const { IndexNode } = require('./nodes')
const { Header } = require('./messages')

const INDEX_TYPE = '@autobase/output'

module.exports = class Rebaser {
  constructor (index, opts = {}) {
    this.index = index
    this.buf = []
    this.added = 0
    this.removed = 0
    this.truncation = 0
    this._alreadyIndexed = false
    this._reduce = opts.reduce
    this._init = opts.init
  }

  _getIndexLength () {
    return this.index.length - this.truncation
  }

  async _getIndexHead () {
    const length = this._getIndexLength()
    if (length <= 1) return null
    const blk = await this.index.get(length - 1)
    return IndexNode.decode(blk)
  }

  async update (node) {
    if (!this.index.length) {
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

  async commit () {
    if (!this.index.length) {
      await this.index.append(Header.encode({
        protocol: INDEX_TYPE
      }))
    }

    const leftover = this._getIndexLength()

    if (!this._alreadyIndexed && leftover > 1) {
      this.removed += leftover - 1
      await this.index.truncate(1)
    } else if (this.truncation) {
      await this.index.truncate(this.index.length - this.truncation)
    }

    if (this._init && (this.truncation || this.index.length <= 1)) {
      await this._init(this.index)
    }

    while (this.buf.length) {
      const next = this.buf.pop()
      let appending = this._reduce ? [] : [next]
      if (this._reduce) {
        const res = await this._reduce(next)
        if (Array.isArray(res)) appending.push(...res)
        else appending.push(res)
        appending = appending.map(v => ({ ...next, value: v }))
      }
      await this.index.append(appending.map(IndexNode.encode))
    }
  }
}
