const cenc = require('compact-encoding')
const debounce = require('debounceify')

const { Header } = require('./messages')
const { IndexNode } = require('./nodes')

const promises = Symbol.for('hypercore.promises')
const INDEX_TYPE = '@autobase/index'

class Rebaser {
  constructor (index) {
    this.index = index
    this.added = 0
    this.removed = 0
    this.changes = []
    this.baseLength = index.length
  }

  async _head () {
    const length = this.baseLength - this.removed
    const node = length > 1 ? await this.index.get(length - 1) : null
    if (Buffer.isBuffer(node)) return IndexNode.decode(node)
    return node
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
    this[promises] = true
    this.autobase = autobase
    this.indexes = Array.isArray(indexes) ? indexes : [indexes]

    this._apply = opts.apply || defaultApply
    this._parentOpen = opts.open
    this._unwrap = !!opts.unwrap
    this._autocommit = opts.autocommit
    this._applying = null

    this.update = debounce(this._update.bind(this))

    this._bestIndex = null
    this._bestIndexLength = 0
    this._lastRebaser = null
    this._changes = []

    this.opened = false
    this._opening = null
    this._opening = this.ready()
    this._opening.catch(noop)
  }

  get status () {
    if (!this._lastRebaser) return {}
    return {
      added: this._lastRebaser.added,
      removed: this._lastRebaser.removed
    }
  }

  get length () {
    return this._bestIndexLength + this._changes.length
  }

  get byteLength () {
    // TODO: This is hard and probably not worth it to implement
    return 0
  }

  async ready () {
    if (this._opening) return this._opening
    await this._parentPromise
    await Promise.all(this.indexes.map(i => i.ready()))

    if (this.indexes.length === 1) {
      const index = this.indexes[0]
      this._autocommit = (this._autocommit !== false) && index.writable
    } else {
      this._autocommit = false
    }

    if (this._autocommit && this.indexes[0].length === 0) {
      await this.indexes[0].append(cenc.encode(Header, {
        protocol: INDEX_TYPE
      }))
    }

    for (const index of this.indexes) {
      if (!this._bestIndex || this._bestIndexLength < index.length) {
        this._bestIndex = index
        this._bestIndexLength = index.length
      }
    }

    this.opened = true
  }

  async _update () {
    if (!this.opened) await this._opening
    await Promise.all(this.indexes.map(i => i.update()))

    // TODO: Short-circuit if no work to do

    const rebasers = []
    const indexes = !this._autocommit ? [...this.indexes, this] : this.indexes
    for (const index of indexes) {
      rebasers.push(new Rebaser(index))
    }

    this._lastRebaser = await bestRebaser(this.autobase.createCausalStream(), rebasers)
    this._bestIndex = this._lastRebaser.index
    this._bestIndexLength = this._bestIndex.length - this._lastRebaser.removed

    this._changes = []
    let batch = []

    for (let i = this._lastRebaser.changes.length - 1; i >= 0; i--) {
      const node = this._lastRebaser.changes[i]
      batch.push(node)
      if (node.batch[1] > 0) continue
      this._applying = batch[0]

      const start = this._changes.length
      await this._apply(batch, this)
      for (let j = start; j < this._changes.length; j++) {
        const change = this._changes[j]
        change.batch[0] = j - start
        change.batch[1] = this._changes.length - j - 1
      }

      this._applying = null
      batch = []
    }
    if (batch.length) throw new Error('Cannot rebase: partial batch in index')

    if (this._autocommit) return this.commit()
  }

  async get (seq, opts) {
    if (!this.opened) await this._opening
    if (!this._bestIndex) await this.update(opts)

    let block = (seq < this._bestIndexLength)
      ? IndexNode.decode(await this._bestIndex.get(seq, { ...opts, valueEncoding: null }))
      : this._changes[seq - this._bestIndexLength]

    if (this._unwrap) block = block.value
    if (opts && opts.valueEncoding) block = opts.valueEncoding.decode(block)

    return block
  }

  async append (block) {
    if (!this.opened) await this._opening
    if (!this._applying) throw new Error('Cannot append to a RebasedHypercore outside of an update operation.')
    if (!Array.isArray(block)) block = [block]

    for (const val of block) {
      const node = new IndexNode({
        value: val,
        batch: [0, 0],
        clock: this._applying.clock,
        change: this._applying.change
      })
      this._changes.push(node)
    }

    return this.length
  }

  // TODO: This should all be atomic
  async commit () {
    if (!this._bestIndex.writable) throw new Error('Can only commit to a writable index')
    if (!this.opened) await this._opening

    if (this._bestIndexLength < this._bestIndex.length) {
      await this._bestIndex.truncate(this._bestIndex.length - this._lastRebaser.removed)
    }
    await this._bestIndex.append(this._changes.map(IndexNode.encode))

    this._bestIndexLength = this._bestIndex.length
    this._changes = []
  }
}

function defaultApply (batch, index) {
  return index.append(batch.map(b => b.value))
}

async function bestRebaser (causalStream, rebasers) {
  for await (const inputNode of causalStream) {
    for (const rebaser of rebasers) {
      if (await rebaser.update(inputNode)) return rebaser
    }
  }
  return rebasers[0]
}

function noop () { }
