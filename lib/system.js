const Hyperbee = require('hyperbee')

const DIGEST_KEY = 'digest'

class SystemViewBatch {
  constructor (system) {
    this.system = system
    this.batch = this.system.bee.batch()
  }

  commit () {
    return this.batch.flush()
  }

  close () {
    return this.batch.close()
  }

  async listIndexers () {
    const digest = this.batch.get(DIGEST_KEY)
    return Promise.all(digest.indexers.map(i => this.batch.get(i.key, { keyEncoding: this.system.subs.writers })))
  }

  async listWriters () {
    const writers = []
    for await (const node of this.batch.createReadStream(this.system.subs.writers.range())) {
      writers.push(node)
    }
    return writers
  }

  removeWriter (key) {
    return this.batch.del(key, { keyEncoding: this.system.subs.writers })
  }

  async addWriter (key) {
    const existing = await this.batch.get(key, { keyEncoding: this.system.subs.writers })
    if (existing && !b4a.equals(existing.key, key)) return
    await this.batch.put(key, null)
    this.
  }

  removeIndexer (key) {
    return this.batch.del(key)
  }

  addIndexer (key) {
    return this.batch.put(key, null, { keyEncoding: this.system.sub.indexers })
  }
}

module.exports = class SystemView {
  constructor (core) {
    this.bee = new Hyperbee(core, { valueEncoding: 'json' })

    const enc = new SubEncoder()
    this.subs = {
      writers: enc.sub('writers')
    }
  }

  async addWriter (key, metadata) {
    const batch = new SystemViewBatch(this)
    await batch.addWriter(key, metadata)
    return batch.commit()
  }

  async removeWriter (key, length) {
    const batch = new SystemViewBatch(this)
    await batch.removeWriter(key, length)
    return batch.commit()
  }

  async addIndexer (key) {
    const batch = new SystemViewBatch(this)
    await batch.addIndexer(key)
    return batch.commit()
  }

  async removeIndexer (key) {
   const batch = new SystemViewBatch(this)
   await batch.addIndexer(key)
   return batch.commit()
  }

  async listIndexers () {
    const batch = new SystemViewBatch(this)
    const indexers = await batch.listIndexers()
    await batch.close()
    return indexers
  }

  async listWriters () {
    const batch = new SystemViewBatch(this)
    const writers = await batch.listWriters()
    await batch.close()
    return writers
  }

  batch () {
    return new SystemViewBatch(this)
  }

  reset () {

  }
}
