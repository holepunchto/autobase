const Hyperbee = require('hyperbee')
const SubEncoder = require('sub-encoder')
const ReadyResource = require('ready-resource')
const b4a = require('b4a')
const c = require('compact-encoding')

const { Info, Member } = require('./messages')

const subs = new SubEncoder()

const DIGEST = subs.sub(b4a.from([0]))
const MEMBERS = subs.sub(b4a.from([1]))

const INFO_KEY = DIGEST.encode('info')
const AUTOBASE_VERSION = 1

module.exports = class SystemView extends ReadyResource {
  constructor (core, { checkout = 0 } = {}) {
    super()

    this.core = core

    // sessions is a workaround for batches not having sessions atm...
    this.db = new Hyperbee(core, { keyEncoding: 'binary', extension: false, checkout, sessions: typeof core.session === 'function' })

    this.version = AUTOBASE_VERSION
    this.members = 0
    this.pendingIndexers = []
    this.indexers = []
    this.heads = []
    this.views = []

    this.indexerUpdate = false

    this._fork = 0
    this._length = 0
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

  async checkout (length) {
    const checkout = new SystemView(this.core.session(), {
      checkout: length
    })

    await checkout.ready()

    return checkout
  }

  async _open () {
    // this should NEVER fail, if so we have a bug elsewhere (should always be consistent)
    const info = await this.db.get('info', { valueEncoding: Info, keyEncoding: DIGEST, update: false, wait: false })
    await this._reset(info)
  }

  async _close () {
    await this.db.close()
  }

  async getIndexedInfo (length = this.core.signedLength) {
    if (this.opened === false) await this.ready()

    if (length === this.core.length) {
      return { version: this.version, members: this.members, pendingIndexers: this.pendingIndexers, indexers: this.indexers, heads: this.heads, views: this.views }
    }

    const node = length === 0 ? null : await this.db.getBySeq(length - 1)
    if (node === null) return { version: 0, members: 0, pendingIndexers: [], indexers: [], heads: [], views: [] }

    return c.decode(Info, node.value)
  }

  static sameIndexers (a, b) {
    if (a.length !== b.length) return false

    for (let i = 0; i < a.length; i++) {
      if (!b4a.equals(a[i].key, b[i].core.key)) return false
    }

    return true
  }

  sameIndexers (indexers) {
    return SystemView.sameIndexers(this.indexers, indexers)
  }

  async history (since) {
    const checkout = this.db.checkout(since)
    const seen = new Map()

    const nodes = []
    const updates = []

    const checkoutNode = since === 0 ? null : await this.db.getBySeq(since - 1)

    let prevInfo = checkoutNode === null ? null : c.decode(Info, checkoutNode.value)
    let updateBatch = 0

    for await (const data of this.db.createHistoryStream({ gte: since })) {
      if (b4a.equals(data.key, INFO_KEY)) {
        const info = c.decode(Info, data.value)

        updates.push({
          batch: updateBatch,
          views: [],
          indexers: !sameIndexers(info, prevInfo),
          systemLength: data.seq + 1
        })

        prevInfo = info
        updateBatch = 0
        continue
      }

      const key = data.key.subarray(2)
      const hex = b4a.toString(key, 'hex')
      const len = c.decode(Member, data.value).length

      if (!seen.has(hex)) {
        const node = await checkout.get(data.key)
        if (node === null) {
          seen.set(hex, 0)
        } else {
          const { length } = c.decode(Member, node.value)
          seen.set(hex, length)
        }
      }

      const prev = seen.get(hex)
      const batch = len - prev
      seen.set(hex, len)

      if (batch === 0) continue

      updateBatch += batch

      if (nodes.length > 0) {
        const top = nodes[nodes.length - 1]
        if (b4a.equals(top.key, key)) {
          top.length = len
          top.batch += batch
          continue
        }
      }

      nodes.push({
        key,
        length: len,
        batch
      })
    }

    await checkout.close()

    return { updates, nodes }
  }

  async update () {
    if (this.opened === false) await this.ready()

    if (this._fork === this.core.fork && this._length === this.core.length) return false

    await this._reset(await this.db.get('info', { valueEncoding: Info, keyEncoding: DIGEST }))
    return true
  }

  async _reset (info) {
    this.version = info === null ? AUTOBASE_VERSION : info.value.version
    this.members = info === null ? 0 : info.value.members
    this.pendingIndexers = info === null ? [] : info.value.pendingIndexers
    this.indexers = info === null ? [] : info.value.indexers
    this.heads = info === null ? [] : info.value.heads
    this.views = info === null ? [] : info.value.views

    this.indexerUpdate = false
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

  async flush (views) {
    for (const [hex, length] of this._clockUpdates) {
      const isIndexer = this._indexerMap.get(hex) !== undefined
      const key = b4a.from(hex, 'hex')

      const info = await this._batch.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
      const value = { isIndexer, isRemoved: info.value.isRemoved, length }

      await this._batch.put(key, value, { valueEncoding: Member, keyEncoding: MEMBERS })
    }

    this._clockUpdates.clear()

    this.views = []

    for (const { core, length, key } of views) {
      this.views.push({ key, length: core ? core.length : length })
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

    if (this.indexerUpdate) this.indexerUpdate = false
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
        this._updateIndexer(h.key, h.length, true, i)
        return true
      }
    }

    const idx = this._indexerMap.get(hex)
    if (idx !== undefined) {
      idx.length = h.length
    }

    return false
  }

  _updateIndexer (key, length, isIndexer, i) {
    const hex = b4a.toString(key, 'hex')

    if (!isIndexer) {
      const existing = this._indexerMap.get(hex)
      if (existing) {
        this.indexerUpdate = true
        this.indexers.splice(this.indexers.indexOf(existing), 1)
        this._indexerMap.delete(hex)
      }
      return
    }

    for (; i < this.pendingIndexers.length; i++) {
      if (b4a.equals(this.pendingIndexers[i], key)) break
    }

    if (length === 0) {
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

      // bootstrap is "silently" added so that initial views have no prologue
      if (!this.bootstrapping) this.indexerUpdate = true
    } else {
      idx.length = length
    }
  }

  _seenLength (key) {
    return this._clockUpdates.get(b4a.toString(key, 'hex')) || 0
  }

  async add (key, { isIndexer = false, length = this._seenLength(key) } = {}) {
    let wasTracked = false
    let wasIndexer = false

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

        const o = older.value
        const n = newer.value

        if (!o.isRemoved) wasTracked = true
        if (o.isIndexer) wasIndexer = true

        if (length === 0 && o.length) length = o.length
        return o.isRemoved !== n.isRemoved || o.isIndexer !== n.isIndexer || o.length !== n.length
      }
    })

    if (!wasTracked) this.members++

    if (wasIndexer || isIndexer) this._updateIndexer(key, length, isIndexer, 0)
  }

  async remove (key) {
    let isIndexer = false

    for (const idx of this.indexers) {
      isIndexer = b4a.equals(idx.key, key)
      if (isIndexer) break
    }

    if (isIndexer) this._updateIndexer(key, null, false, 0)

    let wasTracked = false

    const node = await this._batch.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
    const length = node ? node.value.length : 0

    await this._batch.put(key, {
      isIndexer: false,
      isRemoved: true,
      length
    }, {
      valueEncoding: Member,
      keyEncoding: MEMBERS,
      cas (older, newer) {
        if (older === null) return true

        wasTracked = !!older.value.isRemoved
        return !wasTracked
      }
    })

    if (!wasTracked) this.members--

    return isIndexer
  }

  async has (key, opts) {
    // could be optimised...
    return await this.get(key, opts) !== null
  }

  async get (key, opts = {}) {
    const node = await this._batch.get(key, { timeout: opts.timeout, valueEncoding: Member, keyEncoding: MEMBERS })
    if (node === null) return null
    return (opts.onlyActive !== false || !node.value.isRemoved) ? node.value : null
  }

  async hasLocal (key) {
    try {
      const node = await this.db.get(key, { valueEncoding: Member, keyEncoding: MEMBERS, update: false, wait: false })
      return node !== null
    } catch {
      return false
    }
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

function sameIndexers (a, b) {
  if (a.views.length > 0 && b.views.length > 0) return b4a.equals(a.views[0].key, b.views[0].key)
  if (a.indexers.length !== b.indexers.length) return false

  for (let i = 0; i < a.indexers.length; i++) {
    if (!b4a.equals(a.indexers[i].key, b.indexers[i].key)) return false
  }

  return true
}
