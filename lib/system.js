const Hyperbee = require('hyperbee')
const SubEncoder = require('sub-encoder')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const { Info, Member } = require('./messages')

const subs = new SubEncoder()

const VERSION = 1
const DIGEST = subs.sub(b4a.from([0]))
const MEMBERS = subs.sub(b4a.from([1]))

module.exports = class SystemView extends ReadyResource {
  constructor (core) {
    super()

    this.core = core
    this.db = new Hyperbee(core, { keyEncoding: 'binary', extension: false })

    this.version = 0
    this.members = { active: 0, total: 0 }
    this.heads = []
    this.indexers = []
    this.views = []

    this._fork = 0
    this._length = 0
    this._indexerUpdate = false
    this._indexerMap = new Map()
    this._clockUpdates = new Map()
    this._batch = null
  }

  static async getIndexedInfo (core) {
    const sys = new this(core.session())

    try {
      return await sys.getIndexedInfo()
    } finally {
      await sys.close()
    }
  }

  get bootstrapping () {
    return this.members.total === 0
  }

  async _open () {
    // this should NEVER fail, if so we have a bug elsewhere (should always be consistent)
    const info = await this.db.get('info', { valueEncoding: Info, keyEncoding: DIGEST, update: false, wait: false })
    await this._reset(info)
  }

  async _close () {
    await this.db.close()
  }

  async getIndexedInfo () {
    if (this.opened === false) await this.ready()

    if (this.core.indexedLength === this.core.length) {
      return { version: this.version, members: this.members, heads: this.heads, indexers: this.indexers, views: this.views }
    }

    const co = this.db.checkout(this.core.indexedLength)

    try {
      const info = await co.get('info', { valueEncoding: Info, keyEncoding: DIGEST })
      if (info === null) return { version: 0, members: { active: 0, total: 0 }, heads: [], indexers: [], views: [] }
      return info.value
    } finally {
      await co.close()
    }
  }

  async update () {
    if (this.opened === false) await this.ready()
    if (this._fork === this.core.fork && this._length === this.core.length) return

    await this._reset(await this.db.get('info', { valueEncoding: Info, keyEncoding: DIGEST }))
  }

  async _reset (info) {
    this.version = info === null ? VERSION : info.value.version
    this.members = info === null ? { active: 0, total: 0 } : info.value.members
    this.heads = info === null ? [] : info.value.heads
    this.indexers = info === null ? [] : info.value.indexers
    this.views = info === null ? [] : info.value.views

    this._indexerUpdate = false
    this._indexerMap.clear()
    this._clockUpdates.clear()
    this._length = this.core.length
    this._fork = this.core.fork

    for (const idx of this.indexers) {
      this._indexerMap.set(b4a.toString(idx.key, 'hex'), idx)
    }

    if (this._batch) await this._batch.close()
    this._batch = this.db.batch({ update: false })
  }

  _updateView (name, length, likelyIndex) {
    if (likelyIndex > -1 && likelyIndex < this.views.length) {
      const v = this.views[likelyIndex]
      if (v.name === name) {
        v.length = length
        return likelyIndex
      }
    }

    for (let i = 0; i < this.views.length; i++) {
      const v = this.views[i]
      if (v.name === name) {
        v.length = length
        return i
      }
    }

    this.views.push({
      name,
      length
    })

    return this.views.length - 1
  }

  async flush (update) {
    for (const [hex, length] of this._clockUpdates) {
      const isIndexer = this._indexerMap.get(hex) !== undefined
      const key = b4a.from(hex, 'hex')
      const value = { isIndexer, isWriter: true, length }

      await this._batch.put(key, value, { valueEncoding: Member, keyEncoding: MEMBERS })
    }

    this._clockUpdates.clear()
    this._updateView('_system', this.core.length + this._batch.length + 1, 0) // plus 1 due to info itself

    for (const { core } of update.views) {
      if (core.name === '_system') continue // updated above...
      core.likelyIndex = this._updateView(core.name, core.length, core.likelyIndex)
    }

    const info = {
      version: this.version,
      members: this.members,
      heads: this.heads,
      indexers: this.indexers,
      views: this.views
    }

    await this._batch.put('info', info, { valueEncoding: Info, keyEncoding: DIGEST })
    await this._batch.flush()

    this._length = this.core.length // should be ok
    this._batch = this.db.batch({ update: false })

    if (this._indexerUpdate === false) return false
    this._indexerUpdate = false
    return true
  }

  addHead (node) {
    const h = { key: node.writer.core.key, length: node.length }

    for (let i = 0; i < this.heads.length; i++) {
      const head = this.heads[i]
      if (!hasDependency(node, head)) continue
      const popped = this.heads.pop()
      if (popped !== head) this.heads[i--] = popped
    }

    this.heads.push(h)

    const hex = b4a.toString(h.key, 'hex')

    this._clockUpdates.set(hex, h.length)

    const idx = this._indexerMap.get(hex)
    if (idx !== undefined) idx.length = h.length
  }

  async add (key, { isIndexer = false, length = 0 } = {}) {
    if (isIndexer) {
      const hex = b4a.toString(key, 'hex')
      const idx = this._indexerMap.get(hex)

      if (idx === undefined) {
        const newIdx = { key, length }
        this._indexerUpdate = true
        this._indexerMap.set(hex, newIdx)
        this.indexers.push(newIdx)
      } else {
        idx.length = length
      }
    }

    let wasWriter = false
    let wasTracked = false

    if (length === 0) { // a bit hacky atm due to cas limitations...
      const node = await this._batch.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
      if (node) length = node.value.length
    }

    await this._batch.put(key, {
      isIndexer,
      isWriter: true,
      length
    }, {
      valueEncoding: Member,
      keyEncoding: MEMBERS,
      cas (older, newer) {
        if (older === null) return true

        wasWriter = older.value.isWriter
        wasTracked = true

        const o = older.value
        const n = newer.value

        return o.isWriter !== n.isWriter || o.isIndexer !== n.isIndexer || o.length !== n.length
      }
    })

    if (!wasWriter) this.members.active++
    if (!wasTracked) this.members.total++
  }

  async remove (key) {
    // TODO: remove the writable flag from the record
  }

  async has (key, opts) {
    // could be optimised...
    return await this.get(key, opts) !== null
  }

  async get (key, opts = {}) {
    const node = this._batch.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
    if (node === null) return null
    return (opts.onlyActive !== false && node.value.isWriter) ? node.value : null
  }

  list () {
    return this._batch.createReadStream({
      valueEncoding: Member,
      keyEncoding: MEMBERS
    })
  }

  async isIndexed (key, length) {
    const co = this.db.checkout(this.core.indexedLength)
    try {
      const node = await co.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
      return node !== null && node.value.length >= length
    } finally {
      await co.close()
    }
  }
}

function hasDependency (node, dep) {
  for (const h of node.heads) {
    if (sameNode(h, dep)) return true
  }
  return false
}

function sameNode (a, b) {
  return b4a.equals(a.key, b.key) && a.length === b.length
}
