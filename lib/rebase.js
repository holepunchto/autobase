const cenc = require('compact-encoding')
const debounce = require('debounceify')
const safetyCatch = require('safety-catch')

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

  async update (node, latestClock) {
    if (this.baseLength < 1) {
      this.added++
      this.changes.push(node)
      return false
    }

    let head = await this._head()
    if (head && node.eq(head)) { // Already indexed
      return true
    }

    while (head && !node.eq(head) && (head.contains(node) || !latestClock.has(head.id))) {
      this.removed++
      head = await this._head()
    }

    if (head && node.eq(head)) return true

    this.added++
    this.changes.push(node)

    return false
  }
}

module.exports = class RebasedHypercore {
  constructor (autobase, indexes, opts = {}) {
    this[promises] = true
    this.autobase = autobase
    this.indexes = null

    this._indexes = indexes
    this._apply = opts.apply || defaultApply
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

    this.indexes = await this._indexes
    if (!Array.isArray(this.indexes)) this.indexes = [this.indexes]
    await Promise.all(this.indexes.map(i => i.ready()))

    for (const index of this.indexes) {
      if (index.writable && this._autocommit !== false) {
        this.indexes = [index] // If you pass in a writable index, remote ones are ignored.
        this._autocommit = true
        break
      }
    }

    if (this._autocommit === undefined) this._autocommit = false

    if (this._autocommit && this.indexes[0].length === 0) {
      await this.indexes[0].append(cenc.encode(Header, {
        protocol: INDEX_TYPE
      }))
    } else if (!this.indexes.length) {
      this._changes.push(cenc.encode(Header, {
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

    const latestClock = await this.autobase.latest()

    // TODO: Short-circuit if no work to do

    const rebasers = []
    // If we're not autocommmitting, include this index because it's a memory-only view.
    const indexes = this._autocommit ? this.indexes : [...this.indexes, this]

    for (const index of indexes) {
      rebasers.push(new Rebaser(index))
    }

    this._lastRebaser = await bestRebaser(this.autobase.createCausalStream(), rebasers, latestClock)
    this._bestIndex = this._lastRebaser.index
    this._bestIndexLength = this._bestIndex.length - this._lastRebaser.removed

    this._changes = []
    let batch = []

    for (let i = this._lastRebaser.changes.length - 1; i >= 0; i--) {
      const node = this._lastRebaser.changes[i]
      batch.push(node)
      if (node.batch[1] > 0) continue
      this._applying = batch[batch.length - 1]

      // TODO: Make sure the input clock is the right one to pass to _apply
      const inputNode = await this.autobase._getInputNode(node.change, this._applying.seq)
      const clocks = {
        local: inputNode.clock,
        global: this._applying.clock
      }

      const start = this._changes.length

      try {
        await this._apply(batch, clocks, node.change, this)
      } catch (err) {
        safetyCatch(err)
      }

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

  async _get (seq, opts) {
    return (seq < this._bestIndexLength)
      ? IndexNode.decode(await this._bestIndex.get(seq, { ...opts, valueEncoding: null }))
      : this._changes[seq - this._bestIndexLength]
  }

  async get (seq, opts) {
    if (!this.opened) await this._opening
    if (!this._bestIndex) await this.update(opts)

    let block = await this._get(seq, opts)

    // TODO: support OOB gets
    if (!block) throw new Error('Out of bounds gets are currently not supported')

    if (!this._unwrap) return block
    block = block.value

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

function defaultApply (batch, clock, change, index) {
  return index.append(batch.map(b => b.value))
}

async function bestRebaser (causalStream, rebasers, latestClock) {
  for await (const inputNode of causalStream) {
    for (const rebaser of rebasers) {
      if (await rebaser.update(inputNode, latestClock)) return rebaser
    }
  }
  return rebasers[0]
}

function noop () { }
