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
    this.db = new Hyperbee(core)
    this.subs = {
      writers: enc.sub('writers')
    }

    this.index = 0
    this.checkpointer = 0

    this.indexes = ['system']

    this.changes = []
    this.digest = { heads: [], writers: [], indexerHeads: [] }
    this.indexerHeads = []
  }

  async _open () {
    await this.db.ready()

    const digest = await this.db.get('digest')
    if (!digest) return

    const value = c.decode(messages.SystemDigest, digest.value)
    this.digest = value

    const indexes = await this.db.get('indexes')

    if (indexes) this.indexes = c.decode(messages.SystemIndex, indexes.value)
    else return this.db.put('indexes', c.encode(messages.SystemIndex, this.indexes))
  }

  get bootstrapping () {
    return this.db.feed.length === 0 && this.changes.length === 0
  }

  // triggered by base
  _onindex (added) {
    this.changes = this.changes.slice(added)
    this.checkpointer = 0
  }

  // triggered by base
  _onundo (removed) {
    while (removed-- > 0) this.changes.pop()
  }

  hasWriter (key) {
    for (const w of this.digest.writers) {
      if (b4a.equals(w.key, key)) return true
    }

    for (const c of this.changes) {
      if (b4a.equals(c.add, key)) return true
    }

    return false
  }

  addWriter (key, indexer = true) {
    if (this.hasWriter(key)) return

    this.changes.push({ add: key, indexer: !!indexer })
    this.base._onaddwriter(key, indexer)
    this.base._onsystemappend(1)
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
    const tree = this.db.core.core.tree

    return {
      publicKey: this.db.feed.key,
      treeHash: tree.hash(),
      length: tree.length
    }
  }

  async isIndexed (key, length) {
    for (const idx of this.digest.writers) {
      if (b4a.equals(key, idx.key)) {
        return idx.length >= length
      }
    }
    return false
  }

  async getIndex (name) {
    for (let i = 0; i < this.indexes.length; i++) {
      if (this.indexes[i] === name) return i
    }
    return -1
  }

  async _getIndex (name, db = this.db) {
    for (let i = 0; i < this.indexes.length; i++) {
      if (this.indexes[i] === name) return i
    }

    const index = this.indexes.push(name) - 1
    const value = c.encode(messages.SystemIndex, this.indexes)

    await db.put('indexes', value)

    return index
  }

  async flush (changes, user, writers, heads) {
    const batch = this.db.batch()

    for (const u of user.sort(cmpIndex)) {
      if (u.index < 0) u.index = await this._getIndex(u.name, batch)
    }

    for (let i = 0; i < changes; i++) {
      const c = this.changes[i]

      if (c.add) {
        const value = b4a.alloc(0)
        await batch.put(c.add, value, { keyEncoding: this.subs.writers })

        this.digest.writers.push({
          key: c.add,
          length: 0,
          indexer: c.indexer
        })
      }
    }

    for (const writer of writers) {
      for (const idx of this.digest.writers) {
        if (b4a.equals(idx.key, writer.core.key)) {
          idx.length = writer.indexed
          break
        }
      }
    }

    this.digest.heads = heads.map(toHead)

    const indexerHeads = []

    for (const head of this.digest.heads) {
      for (const idx of this.digest.writers) {
        if (b4a.equals(idx.key, head.key)) {
          if (idx.indexer) indexerHeads.push(head)
          break
        }
      }
    }

    // only store when indexer heads have changed
    if (indexerHeads.length) this.digest.indexerHeads = indexerHeads

    const value = c.encode(messages.SystemDigest, this.digest)
    await batch.put('digest', value)

    await batch.flush()
  }
}

function toHead (node) {
  return {
    key: node.writer.core.key,
    length: node.length
  }
}

function cmpIndex (a, b) {
  return a.name < b.name ? -1 : (a.name > b.name ? 1 : 0)
}
