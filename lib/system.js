const Hyperbee = require('hyperbee')
const SubEncoder = require('sub-encoder')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const c = require('compact-encoding')
const messages = require('./messages')

module.exports = class SystemView extends ReadyResource {
  constructor (base, core) {
    super()

    const enc = new SubEncoder()

    this.base = base
    this.core = core

    // todo: support extension
    this.db = new Hyperbee(core, { extension: false })
    this.subs = {
      writers: enc.sub('writers')
    }

    this.index = 0
    this.checkpointer = 0

    this._updating = []

    this.digest = { heads: [], writers: [], indexes: ['system'], indexerHeads: [] }
    this.indexerHeads = []
  }

  async _open () {
    await this.db.ready()

    const digest = await this.db.get('digest')
    if (!digest) return

    const value = c.decode(messages.SystemDigest, digest.value)
    this.digest = value
  }

  get bootstrapping () {
    return this.core.length === 0
  }

  // triggered by base
  async _onindex () {
    const db = this.db.checkout(this.core.indexedLength)
    const rawDigest = await db.get('digest')

    this.digest = c.decode(messages.SystemDigest, rawDigest.value)

    this._checkpointer = 0
  }

  async _updateIndex (name, digest) {
    for (let i = 0; i < digest.indexes.length; i++) {
      if (digest.indexes[i] === name) return i
    }

    return digest.indexes.push(name) - 1
  }

  addWriter (key, indexer = true) {
    this.base._onaddwriter(key, indexer)
    this._updating.push({ method: 'add', key, indexer: !!indexer })
  }

  get length () {
    return this.db.feed.core.tree.length
  }

  treeHash () {
    return this.db.feed.core.tree.hash()
  }

  checkpoint () {
    return {
      index: 0,
      checkpointer: this.checkpointer,
      checkpoint: this.checkpointer ? null : this._checkpoint()
    }
  }

  _checkpoint () {
    return this.db.feed._source._checkpoint()
  }

  async isIndexed (key, length) {
    for (const idx of this.digest.writers) {
      if (b4a.equals(key, idx.key)) {
        return idx.length >= length
      }
    }
    return false
  }

  getIndex (name) {
    for (let i = 0; i < this.digest.indexes.length; i++) {
      if (this.digest.indexes[i] === name) return i
    }
    return -1
  }

  async flush (update, node) {
    const batch = await this.db.batch()

    const raw = await batch.get('digest')
    const digest = raw
      ? c.decode(messages.SystemDigest, raw.value)
      : { writers: [], heads: [], indexes: [], indexerHeads: [] }

    let changes = 0

    // add writers
    for (const c of this._updating) {
      if (c.method !== 'add') continue

      let hasWriter = false
      for (const writer of digest.writers) {
        if (b4a.equals(writer.key, c.key)) {
          hasWriter = true
          break
        }
      }

      if (hasWriter) continue

      digest.writers.push({ key: c.key, length: 0, indexer: c.indexer })
      await batch.put(c.key, b4a.alloc(0), { keyEncoding: this.subs.writers })

      changes++
    }

    this._updating = []

    // update heads
    updateHeads(digest.heads, node)

    if (node.writer.isIndexer) {
      updateHeads(digest.indexerHeads, node)
    }

    // update indexes
    for (const { core } of update.user) {
      const index = await this._updateIndex(core.name, digest)
      core.index = index
    }

    // update writers
    for (const writer of this.base.writers) {
      for (const idx of digest.writers) {
        if (b4a.equals(idx.key, writer.core.key)) {
          const seen = node.clock.get(writer.core.key)
          if (idx.length < seen) idx.length = seen
          break
        }
      }
    }

    const value = c.encode(messages.SystemDigest, digest)
    await batch.put('digest', value)

    await batch.flush()

    this.base._onsystemappend(changes)
  }
}

function toHead (node) {
  return {
    key: node.writer.core.key,
    length: node.length
  }
}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}

function updateHeads (heads, node) {
  for (let i = 0; i < heads.length; i++) {
    const head = heads[i]
    const length = node.clock.get(head.key)
    if (!length || head.length > length) continue
    if (popAndSwap(heads, i)) i--
  }
  heads.push(toHead(node))
}
