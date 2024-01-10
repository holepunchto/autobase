const Hyperbee = require('hyperbee')
const SubEncoder = require('sub-encoder')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const c = require('compact-encoding')
const { Info, Member } = require('./messages')

const subs = new SubEncoder()

const VERSION = 1
const DIGEST = subs.sub(b4a.from([0]))
const MEMBERS = subs.sub(b4a.from([1]))

module.exports = class SystemView extends ReadyResource {
  constructor (core, checkout = 0) {
    super()

    this.core = core
    // sessions is a workaround for batches not having sessions atm...
    this.db = new Hyperbee(core, { keyEncoding: 'binary', extension: false, checkout, sessions: typeof core.session === 'function' })

    this.version = 0
    this.members = 0
    this.pendingIndexers = []
    this.indexers = []
    this.heads = []
    this.views = []

    this._fork = 0
    this._length = 0
    this._indexerUpdate = false
    this._indexerMap = new Map()
    this._clockUpdates = new Map()
    this._batch = null
  }

  static async getIndexedInfo (core, length) {
    const sys = new this(core.session())

    try {
      return await sys.getIndexedInfo(length)
    } finally {
      await sys.close()
    }
  }

  get bootstrapping () {
    return this.members === 0
  }

  requestWakeup () {
    if (this.core._source.wakeupExtension) {
      this.core._source.wakeupExtension.requestWakeup()
    }
  }

  broadcastWakeup () {
    if (this.core._source.wakeupExtension) {
      this.core._source.wakeupExtension.broadcastWakeup()
    }
  }

  async _open () {
    // this should NEVER fail, if so we have a bug elsewhere (should always be consistent)
    const info = await this.db.get('info', { valueEncoding: Info, keyEncoding: DIGEST, update: false, wait: false })
    await this._reset(info)
  }

  async _close () {
    await this.db.close()
  }

  async getIndexedInfo (length = this.core.indexedLength) {
    if (this.opened === false) await this.ready()

    if (length === this.core.length) {
      return { version: this.version, members: this.members, pendingIndexers: this.pendingIndexers, indexers: this.indexers, heads: this.heads, views: this.views }
    }

    const node = length === 0 ? null : await this.db.getBySeq(length - 1)
    if (node === null) return { version: 0, members: 0, pendingIndexers: [], indexers: [], heads: [], views: [] }

    return c.decode(Info, node.value)
  }

  async update () {
    if (this.opened === false) await this.ready()
    if (this._fork === this.core.fork && this._length === this.core.length) return

    await this._reset(await this.db.get('info', { valueEncoding: Info, keyEncoding: DIGEST }))
  }

  async _reset (info) {
    this.version = info === null ? VERSION : info.value.version
    this.members = info === null ? 0 : info.value.members
    this.pendingIndexers = info === null ? [] : info.value.pendingIndexers
    this.indexers = info === null ? [] : info.value.indexers
    this.heads = info === null ? [] : info.value.heads
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

  _updateView (length, core, likelyIndex) {
    if (likelyIndex > -1 && likelyIndex < this.views.length) {
      const v = this.views[likelyIndex]
      if (core.isKeyOwner(v.key)) {
        v.length = length
        v.key = core.latestKey
        return likelyIndex
      }
    }

    for (let i = 0; i < this.views.length; i++) {
      const v = this.views[i]
      if (core.isKeyOwner(v.key)) {
        v.length = core.length
        v.key = core.latestKey
        return i
      }
    }

    // store indexer count so we can derive the key on restart
    this.views.push({
      length,
      key: core.latestKey
    })

    return this.views.length - 1
  }

  async flush (update) {
    for (const [hex, length] of this._clockUpdates) {
      const isIndexer = this._indexerMap.get(hex) !== undefined
      const key = b4a.from(hex, 'hex')
      const value = { isIndexer, isRemoved: true, length }

      await this._batch.put(key, value, { valueEncoding: Member, keyEncoding: MEMBERS })
    }

    this._clockUpdates.clear()
    const i = this._updateView(
      this.core.length + this._batch.length + 1, // plus 1 due to info itself
      this.core._source,
      0
    )

    for (const { core } of update.views) {
      if (core.name === '_system') {
        core.likelyIndex = i
        continue // updated above...
      }
      core.likelyIndex = this._updateView(
        core.length,
        core,
        core.likelyIndex
      )
    }

    const info = {
      version: this.version,
      members: this.members,
      pendingIndexers: this.pendingIndexers,
      indexers: this.indexers,
      heads: this.heads,
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

      if (!hasDependency(node, head)) {
        if (!b4a.equals(node.writer.core.key, head.key)) continue

        // todo: remove in next major because bug was fixed here:
        // https://github.com/holepunchto/autobase-next/pull/237

        // filter out any bad heads introduced by a bug to
        // prevent inconsistencies being written to the oplog
        if (head.length > h.length) return false
      }

      const popped = this.heads.pop()
      if (popped !== head) this.heads[i--] = popped
    }

    this.heads.push(h)

    const hex = b4a.toString(h.key, 'hex')

    this._clockUpdates.set(hex, h.length)

    if (this.pendingIndexers.length > 0) {
      for (let i = 0; i < this.pendingIndexers.length; i++) {
        if (!b4a.equals(this.pendingIndexers[i], h.key)) continue
        this._updateIndexer(h.key, h.length, false, i)
        return true
      }
    }

    const idx = this._indexerMap.get(hex)
    if (idx !== undefined) idx.length = h.length

    return false
  }

  _updateIndexer (key, length, isPending, i) {
    const hex = b4a.toString(key, 'hex')

    if (isPending && this._indexerMap.has(hex)) {
      isPending = length > 0
    }

    for (; i < this.pendingIndexers.length; i++) {
      if (b4a.equals(this.pendingIndexers[i], key)) break
    }

    if (isPending) {
      if (i >= this.pendingIndexers.length) this.pendingIndexers.push(key)
      return
    }

    if (i < this.pendingIndexers.length) {
      const top = this.pendingIndexers.pop()
      if (i < this.pendingIndexers.length) this.pendingIndexers[i] = top
    }

    const idx = this._indexerMap.get(hex)

    if (idx === undefined) {
      const newIdx = { key, length }
      this._indexerMap.set(hex, newIdx)
      this.indexers.push(newIdx)
      this._indexerUpdate = true
    } else {
      idx.length = length
    }
  }

  _seenLength (key) {
    return this._clockUpdates.get(b4a.toString(key, 'hex')) || 0
  }

  async add (key, { isIndexer = false, length = this._seenLength(key), isPending = true } = {}) {
    if (isIndexer) this._updateIndexer(key, length, isPending, 0)

    let wasTracked = false

    if (length === 0) { // a bit hacky atm due to cas limitations...
      const node = await this._batch.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
      if (node) length = node.value.length
    }

    await this._batch.put(key, {
      isIndexer,
      isRemoved: false,
      length
    }, {
      valueEncoding: Member,
      keyEncoding: MEMBERS,
      cas (older, newer) {
        if (older === null) return true

        wasTracked = true

        const o = older.value
        const n = newer.value

        return o.isRemoved !== n.isRemoved || o.isIndexer !== n.isIndexer || o.length !== n.length
      }
    })

    if (!wasTracked) this.members++
  }

  async remove (key) {
    // TODO: remove the writable flag from the record
  }

  async has (key, opts) {
    // could be optimised...
    return await this.get(key, opts) !== null
  }

  async get (key, opts = {}) {
    const node = await this._batch.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
    if (node === null) return null
    return (opts.onlyActive !== false || !node.value.isRemoved) ? node.value : null
  }

  async getLocalLength (key) {
    try {
      const node = await this.db.get(key, { valueEncoding: Member, keyEncoding: MEMBERS, update: false, wait: false })
      return node === null ? 0 : node.value.length
    } catch {
      return 0
    }
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
  for (const h of node.actualHeads) {
    if (sameNode(h, dep)) return true
  }
  return false
}

function sameNode (a, b) {
  return b4a.equals(a.key, b.key) && a.length === b.length
}
