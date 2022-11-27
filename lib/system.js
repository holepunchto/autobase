const Hyperbee = require('hyperbee')
const SubEncoder = require('sub-encoder')
const b4a = require('b4a')

module.exports = class SystemView {
  constructor (base, core) {
    const enc = new SubEncoder({ keyEncoding: 'utf-8' })

    this.base = base
    this.db = new Hyperbee(core, { valueEncoding: 'json' })
    this.subs = { writers: enc.sub('writers'), indexes: enc.sub('indexes') }
    this.changes = []
  }

  _onindex (added) {
    console.log('updated system also!!')
  }

  // triggered by base
  _onundo (removed) {
    this.changes = this.changes.slice(0, -removed)
  }

  addWriter (key) {
    this.changes.push({ add: key })
    this.base._onsystemappend(this, 1)
  }

  checkpoint () {
    const tree = this.db.feed.core.tree

    return {
      treeHash: tree.hash(),
      length: tree.length
    }
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

  async flush (changes, user) {
    if (changes === 0 && user.length === 0) return

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
    }

    await batch.flush()
  }
}

function cmpIndex (a, b) {
  return a.name < b.name ? -1 : (a.name > b.name ? 1 : 0)
}
