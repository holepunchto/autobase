const { IndexNode } = require('./nodes')

class Rebaser {
  constructor (index) {
    this.index = index
    this.added = 0
    this.removed = 0
    this.changes = []
    this.baseLength = index.length
  }

  get length () {
    return this.baseLength - this.removed + this.added
  }

  async _head () {
    const length = this.baseLength - this.removed
    const node = length > 1 ? await this.index.get(length - 1) : null
    return node && IndexNode.decode(node)
  }

  async update (node) {
    if (this.baseLength <= 1) {
      this.added++
      this.changes.push(node)
      return false
    }

    let head = await this._head()
    if (await this.alreadyIndexed(node, head)) {
      return true
    }
    while (head && head.contains(node)) {
      this.removed++
      head = await this._head()
    }

    this.added++
    this.changes.push(node)

    return false
  }

  async alreadyIndexed (node, head) {
    if (!head) head = await this._head()
    return head && node.lte(head) && head.gte(node)
  }
}

module.exports = class RebasedHypercore {
  constructor (autobase, indexes, opts = {}) {
    this.autobase = autobase
    this.indexes = Array.isArray(indexes) ? indexes : [indexes]

    this._unwrap = !!opts.unwrap
    this._includeInputNodes = opts.includeInputNodes !== false
    this._userApply = opts.apply
    this._applying = null

    this.best = null
    this.changes = []
  }

  get length () {
    return this.best && this.best.length
  }

  get added () {
    return this.best && this.best.added
  }

  get removed () {
    return this.best && this.best.removed
  }

  get index () {
    return this.best && this.best.index
  }

  async update (opts) {
    await Promise.all(this.indexes.map(i => i.update(opts)))

    const rebasers = []
    for (const index of this.indexes) {
      rebasers.push(new Rebaser(index))
    }

    let best = null
    for await (const inputNode of this.autobase.createCausalStream(opts)) {
      for (const rebaser of rebasers) {
        if (!(await rebaser.update(inputNode))) continue
        best = rebaser
        break
      }
      if (best) break
    }

    if (!best) best = rebasers[0]
    this.best = best
    this.changes = []

    for (let i = best.changes.length - 1; i >= 0; i--) {
      this._applying = best.changes[i]
      await this._userApply ? this._userApply(this._applying, this) : this.append()
    }
  }

  async get (seq, opts) {
    if (!this.best) await this.update(opts)
    const block = (seq < this.length) ? await this.best.index.get(seq, opts) : this.changes[seq - this.length]
    const decoded = IndexNode.decode(block)
    if (this._includeInputNodes) {
      const inputNode = await this.autobase._getInputNode(decoded.node.id, decoded.node.seq)
      if (inputNode) {
        inputNode.key = decoded.node.key
        inputNode.seq = decoded.node.seq
        decoded.node = inputNode
      }
    }
    if (this._unwrap) {
      return decoded.value || decoded.node.value
    }
    return decoded
  }

  append (block) {
    if (!this._applying) throw new Error('Cannot append to a RebasedHypercore outside of an update operation.')
    if (block) this._applying.value = block
    this.changes.push(IndexNode.encode(this._applying))
    this._applying = null
  }

  // TODO: This should all be atomic
  async commit (output) {
    if (this.best.removed) {
      await output.truncate(output.length - this.best.removed)
    }

    await output.append(this.changes)
    this.changes = []
  }
}
