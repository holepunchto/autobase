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
      writers: enc.sub('writers'),
      indexes: enc.sub('indexes', { keyEncoding: 'utf-8' })
    }
    this.changes = []
    this.digest = { heads: [], writers: [] }
  }

  async _open () {
    await this.db.ready()

    const digest = await this.db.get('digest')
    if (!digest) return

    const value = c.decode(messages.SystemDigest, digest.value)
    this.digest = value
  }

  get bootstrapping () {
    return this.db.feed.length === 0 && this.changes.length === 0
  }

  _onindex (added) {
    this.changes = this.changes.slice(added)
  }

  // triggered by base
  _onundo (removed) {
    while (removed-- > 0) {
      const { add } = this.changes.pop()
      this.base._onremovewriter(add)
    }
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

  addWriter (key) {
    if (this.hasWriter(key)) return

    this.changes.push({ add: key })
    this.base._onaddwriter(key)
    this.base._onsystemappend(1)
  }

  get length () {
    return this.db.feed.core.tree.length
  }

  treeHash () {
    return this.db.feed.core.tree.hash()
  }

  checkpoint () {
    const tree = this.db.feed.core.tree

    return {
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
    const data = await this.db.get(name, { keyEncoding: this.subs.indexes })
    if (!data) return null

    const { treeHash, length } = c.decode(messages.Checkpoint, data.value)
    return { name, treeHash, length }
  }

  async listIndexes () {
    const all = []
    for await (const data of this.db.createReadStream(this.subs.indexes.range())) {
      const { treeHash, length } = c.decode(messages.Checkpoint, data.value)
      all.push({ name: data.key, treeHash, length })
    }
    return all
  }

  async flush (changes, user, writers, heads) {
    const batch = this.db.batch()

    for (const u of user.sort(cmpIndex)) {
      const value = c.encode(messages.Checkpoint, { treeHash: u.treeHash, length: u.length })
      await batch.put(u.name, value, { keyEncoding: this.subs.indexes })
    }

    for (let i = 0; i < changes; i++) {
      const c = this.changes[i]

      if (c.add) {
        const value = b4a.alloc(0)
        await batch.put(c.add, value, { keyEncoding: this.subs.writers })

        this.digest.writers.push({
          key: c.add,
          length: 0
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
