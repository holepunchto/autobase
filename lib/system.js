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
    this._changes = []
    this._indexerMap = new Map()
    this._clockUpdates = new Map()
  }

  static async getIndexedInfo (core, length) {
    const sys = new this(core.session())

    try {
      return await sys.getIndexedInfo(length)
    } finally {
      await sys.close()
    }
  }

  static async * flushes (core, { reverse, lt = core.length, gte = 0, wait = true } = {}) {
    if (lt <= 0) return

    // ensure block
    await core.get(lt - 1)
    const sys = new SystemView(core)

    try {
      for await (const data of sys.db.createHistoryStream({ lt, gte, wait, reverse: true })) {
        if (!b4a.equals(data.key, INFO_KEY)) continue
        const info = c.decode(Info, data.value)
        yield { length: data.seq + 1, info }
      }
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
          indexers: prevInfo === null || !sameIndexers(info, prevInfo),
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
    this._changes = []

    for (const idx of this.indexers) {
      this._indexerMap.set(b4a.toString(idx.key, 'hex'), idx)
    }
  }

  async flush (views) {
    const batch = this.db.batch({ update: false })

    for (let i = 0; i < this._changes.length; i++) {
      const c = this._changes[i]
      if (this._clockUpdates.has(b4a.toString(c.key, 'hex'))) continue
      await batch.put(c.key, c.value, { valueEncoding: Member, keyEncoding: MEMBERS })
    }

    for (const [hex, length] of this._clockUpdates) {
      const isIndexer = this._indexerMap.get(hex) !== undefined
      const key = b4a.from(hex, 'hex')

      const info = await this._get(key, 0)

      const value = { isIndexer, isRemoved: info ? info.isRemoved : true, length }
      await batch.put(key, value, { valueEncoding: Member, keyEncoding: MEMBERS })
    }

    this._clockUpdates.clear()

    let maxIndex = -1
    for (const view of views) {
      if (view.mappedIndex > maxIndex) maxIndex = view.mappedIndex
    }

    while (this.views.length > maxIndex + 1) this.views.pop()

    for (const view of views) {
      const length = view.core ? view.core.length : view.length
      if (!length) continue

      const v = { key: view.key, length }

      if (view.mappedIndex !== -1) {
        this.views[view.mappedIndex] = v
      } else {
        view.mappedIndex = this.views.push(v) - 1
      }
    }

    const info = {
      version: this.version,
      members: this.members,
      pendingIndexers: this.pendingIndexers,
      indexers: this.indexers,
      heads: this.heads,
      views: this.views
    }

    await batch.put('info', info, { valueEncoding: Info, keyEncoding: DIGEST })
    await batch.flush()

    this._length = this.core.length // should be ok
    this._changes = []

    if (this.indexerUpdate) this.indexerUpdate = false
  }

  checkpoint () {
    return {
      version: this.version,
      members: this.members,
      pendingIndexers: this.pendingIndexers.slice(0),
      indexers: cloneNodes(this.indexers),
      heads: cloneNodes(this.heads),
      views: cloneNodes(this.views),
      indexerUpdate: this.indexerUpdate,
      changes: this._changes.slice(0),
      indexerMap: new Map([...this._indexerMap]),
      clockUpdates: new Map([...this._clockUpdates])
    }
  }

  applyCheckpoint (checkpoint) {
    this.version = checkpoint.version
    this.members = checkpoint.members
    this.pendingIndexers = checkpoint.pendingIndexers
    this.indexers = checkpoint.indexers
    this.heads = checkpoint.heads
    this.views = checkpoint.views
    this.indexerUpdate = checkpoint.indexerUpdate

    this._changes = checkpoint.changes
    this._indexerMap = checkpoint.indexerMap
    this._clockUpdates = checkpoint.clockUpdates
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
      if (b4a.equals(this.pendingIndexers[i], key)) {
        break
      }
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

  async ack (key) {
    const value = await this._get(key, 0)
    const length = this._seenLength(key)
    if (value && value.length === length) return

    const isIndexer = value ? value.isIndexer : false
    const isRemoved = value ? value.isRemoved : true

    this._changes.push({ key, value: { isIndexer, isRemoved, length } })
  }

  async add (key, { isIndexer = false, length = this._seenLength(key) } = {}) {
    let value = null
    let found = false
    let changed = true

    for (let i = this._changes.length - 1; i >= 0; i--) {
      const c = this._changes[i]
      if (b4a.equals(key, c.key)) {
        value = c.value
        found = true
        break
      }
    }

    if (!found) {
      const node = await this.db.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })

      if (node) {
        value = node.value
        found = true
      }
    }

    let wasTracked = false
    let wasIndexer = false

    if (found) {
      if (!value.isRemoved) wasTracked = true
      if (value.isIndexer) wasIndexer = true
      if (length < value.length) length = value.length
      if (value.length === length && value.isIndexer === isIndexer && value.isRemoved === false) changed = false
    }

    if (changed) {
      this._changes.push({ key, value: { isIndexer, isRemoved: false, length } })
    }

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

    let value = null
    let found = false

    for (let i = this._changes.length - 1; i >= 0; i--) {
      const c = this._changes[i]
      if (b4a.equals(key, c.key)) {
        value = c.value
        found = true
        break
      }
    }

    if (!found) {
      const node = await this.db.get(key, { valueEncoding: Member, keyEncoding: MEMBERS })
      if (node) {
        value = node.value
        found = true
      }
    }

    const changed = !found || !value.isRemoved
    const wasTracked = found && !value.isRemoved
    const length = found ? value.length : 0

    if (changed) {
      this._changes.push({ key, value: { isIndexer: false, isRemoved: true, length } })
    }

    if (wasTracked) this.members--

    return isIndexer
  }

  async linkable (key, length) {
    const len = this._seenLength(key)
    if (len > 0) return length > len

    const info = await this._get(key, 0)
    const prevLength = info ? info.length : 0

    return length > prevLength
  }

  async has (key, opts) {
    // could be optimised...
    return await this.get(key, opts) !== null
  }

  async _get (key, timeout) {
    let value = null
    let found = false

    for (let i = this._changes.length - 1; i >= 0; i--) {
      const c = this._changes[i]
      if (b4a.equals(key, c.key)) {
        value = c.value
        found = true
        break
      }
    }

    if (!found) {
      const node = await this.db.get(key, { timeout, valueEncoding: Member, keyEncoding: MEMBERS })
      if (node) {
        value = node.value
        found = true
      }
    }

    return found ? value : null
  }

  async get (key, opts = {}) {
    const value = await this._get(key, opts.timeout || 0)
    return (opts.onlyActive !== false || !value.isRemoved) ? value : null
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

  list () { // note, NOT safe to use during apply atm
    return this.db.createReadStream({
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

function cloneNodes (arr) {
  const c = []
  for (let i = 0; i < arr.length; i++) {
    c.push({ key: arr[i].key, length: arr[i].length })
  }
  return c
}
