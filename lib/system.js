const Hyperbee = require('hyperbee')
const SubEncoder = require('sub-encoder')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')

module.exports = class SystemView extends ReadyResource {
  constructor (base, core) {
    super()

    const enc = new SubEncoder()

    this.base = base
    this.db = new Hyperbee(core, { valueEncoding: 'json' })
    this.subs = {
      writers: enc.sub('writers', { keyEncoding: 'utf-8' }),
      indexes: enc.sub('indexes', { keyEncoding: 'utf-8' })
    }
    this.changes = []
    this.digest = { heads: [], indexers: [] }
  }

  async _open () {
    await this.db.ready()

    const digest = await this.db.get('digest')
    if (!digest) return

    this.digest = digest.value
  }

  get bootstrapping () {
    return this.db.feed.length === 0 && this.changes.length === 0
  }

  _onindex (added) {
    this.changes = this.changes.slice(added)
  }

  // triggered by base
  _onundo (removed) {
    this.changes = this.changes.slice(0, -removed)
  }

  addWriter (key) {
    this.changes.push({ add: key, clock: this.base._clock })
    this.base._onsystemappend(this, 1)
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
    for (const idx of this.digest.indexers) {
      if (b4a.toString(key, 'hex') === idx.key) {
        return idx.length >= length
      }
    }
    return false
  }

  async getWriter (key) {
    const data = await this.db.get(b4a.toString(key, 'hex'), { keyEncoding: this.subs.writers })
    return data && { key }
  }

  async listWriters () {
    const all = []
    const seen = new Set()

    for await (const data of this.db.createReadStream(this.subs.writers.range())) {
      seen.add(data.key)
      all.push({ key: b4a.from(data.key, 'hex') })
    }

    for (const change of this.changes) {
      if (change.add && !seen.has(change.add)) {
        seen.add(change.add)
        all.push({ key: b4a.from(change.add, 'hex') })
      }
    }

    return all
  }

  async listIndexers () {
    const all = []
    for await (const data of this.db.createReadStream(this.subs.writers.range())) {
      all.push({ key: b4a.from(data.key, 'hex') })
    }
    return all
  }

  async getIndex (name) {
    const data = await this.db.get(name, { keyEncoding: this.subs.indexes })
    return data && { name: data.key, treeHash: b4a.from(data.value.treeHash, 'hex'), length: data.value.length }
  }

  async listIndexes () {
    const all = []
    for await (const data of this.db.createReadStream(this.subs.indexes.range())) {
      all.push({ name: data.key, treeHash: b4a.from(data.value.treeHash, 'hex'), length: data.value.length })
    }
    return all
  }

  async flush (changes, user, indexers, heads) {
    const batch = this.db.batch()
    const { indexes, writers } = this.subs

    for (const u of user.sort(cmpIndex)) {
      const value = {
        treeHash: b4a.toString(u.treeHash, 'hex'),
        length: u.length
      }

      await batch.put(u.name, value, { keyEncoding: indexes })
    }

    for (let i = 0; i < changes; i++) {
      const c = this.changes[i]

      if (c.add) {
        const value = {}
        await batch.put(b4a.toString(c.add, 'hex'), value, { keyEncoding: writers })
      }
    }

    if (changes > 0) {
      this.changes = this.changes.slice(changes)

      for await (const data of batch.createReadStream(this.subs.writers.range())) {
        let found = false
        for (const idx of this.digest.indexers) {
          if (idx.key === data.key) {
            found = true
            break
          }
        }
        if (!found) {
          this.digest.indexers.push({
            key: data.key,
            length: 0
          })
        }
      }
    }

    for (const writer of indexers) {
      for (const idx of this.digest.indexers) {
        if (idx.key === b4a.toString(writer.core.key, 'hex')) {
          idx.length = writer.indexed
        }
      }
    }

    this.digest.heads = heads.map(h => ({ key: b4a.toString(h.writer.core.key, 'hex'), length: h.length }))

    await batch.put('digest', this.digest)

    await batch.flush()
  }
}

function cmpIndex (a, b) {
  return a.name < b.name ? -1 : (a.name > b.name ? 1 : 0)
}
