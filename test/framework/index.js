const Corestore = require('corestore')
const { sync } = require('autobase-test-helpers')
const b4a = require('b4a')

const Autobase = require('../../')

class Base {
  constructor (storage, opts = {}) {
    this.store = new Corestore(storage)

    this.root = opts.root || null
    this.isRoot = !this.root

    const baseOpts = validateOpts(opts)
    const bootstrap = this.root ? this.root.base.bootstrap : null

    this.base = new Autobase(this.store, bootstrap, baseOpts)

    this._streams = new Map()
    this._addWriter = opts.addWriter || defaultAddWriter
    this._message = opts.message || defaultMessage

    this.messageCount = 0
  }

  ready () {
    return this.base.ready()
  }

  get key () {
    return this.base.local.key
  }

  get hex () {
    return b4a.toString(this.key, 'hex')
  }

  async join ({ indexer = false, base = this.root } = {}) {
    const writable = new Promise(resolve => {
      this.base.once('writable', resolve)
    })

    base.addWriter(this.base.local.key, indexer)
    await writable

    await this.append(null)

    await base.sync([this])
    await base.append(null)
  }

  addWriter (key, indexer) {
    return this.base.append(this._addWriter(key, indexer))
  }

  sync (bases) {
    if (!bases) return this._syncAll()

    if (!Array.isArray(bases)) bases = [bases]
    return sync([this.base].concat(bases.map(b => b.base)))
  }

  _syncAll () {
    return sync([this.base, ...this._streams.keys()])
  }

  replicate (bases) {
    if (!Array.isArray(bases)) bases = [bases]
    const added = []
    for (const base of bases) {
      if (this._replicate(base)) added.push(base)
    }

    return added
  }

  _replicate (remote) {
    if (this._streams.has(remote.base) || remote === this) {
      return false
    }

    const s1 = this.store.replicate(true)
    const s2 = remote.store.replicate(false)

    this._streams.set(remote.base, streamGc(s1))
    remote._streams.set(this.base, streamGc(s2))

    s1.on('close', () => this._streams.delete(remote.base))
    s2.on('close', () => remote._streams.delete(this.base))

    s1.pipe(s2).pipe(s1)

    return true
  }

  unreplicate (bases) {
    if (!bases) return this.offline()
    if (!Array.isArray(bases)) bases = [bases]

    const closing = []
    for (const base of bases) {
      const gc = this._streams.get(base.base)
      if (gc) closing.push(gc())
    }

    return Promise.all(closing)
  }

  offline (bases) {
    const closing = []
    for (const gc of this._streams.values()) {
      closing.push(gc())
    }
    return Promise.all(closing)
  }

  append (data) {
    return this.base.append(data)
  }

  getState () {
    return {
      base: this.base,
      view: this.base.view,
      linearizer: this.base.linearizer,
      indexers: this.base.linearizer.indexers
    }
  }

  async spam (messages) {
    while (messages-- > 0) {
      await this.append(this._message(this.messageCount++))
    }
  }
}

class Room {
  constructor (storage, opts = {}) {
    this._storage = storage
    this.root = new Base(this._storage(), opts)
    this.opts = { ...opts, root: this.root }

    this.members = new Map()
    this.indexers = []

    this.size = opts.size === undefined ? 1 : opts.size
    this.opened = false
  }

  async ready () {
    if (this.opened) return
    this.opened = true

    await this.root.ready()
    this.members.set(this.root.hex, this.root)
    this._addIndexer(this.root)

    await this.createMembers(this.size - 1)
  }

  * [Symbol.iterator] () {
    yield * this.members.values()
  }

  async createMember () {
    const member = new Base(this._storage(), this.opts)
    await member.ready()

    this.members.set(member.hex, member)
    return member
  }

  async createMembers (n) {
    const create = []
    for (let i = 0; i < n; i++) {
      create.push(this.createMember())
    }

    const members = await Promise.all(create)

    for (const base of this.members.values()) {
      for (const other of this.members.values()) {
        if (base === other) continue
        base.replicate(other)
      }
    }

    return members
  }

  _addIndexer (base) {
    this.indexers.push(base)
    base.base.system.core.on('append', () => {
      this._updateIndexers(base)
    })
  }

  // todo: minimize async ops here
  async _updateIndexers (base) {
    const info = await base.base.system.getIndexedInfo()
    const current = this.indexers.length
    if (info.indexers.length <= current) return

    for (const { key } of info.indexers.slice(current)) {
      const member = this.members.get(b4a.toString(key, 'hex'))
      this._addIndexer(member)
    }
  }

  async sync (bases = [...this.members.values()]) {
    await bases[0].sync(bases)
  }

  replicate (bases = [...this.members.values()]) {
    const adds = []
    for (const left of bases) {
      // todo: maybe double loop too slow at one point
      const added = left.replicate(bases)
      if (added.length) adds.push(left, added)
    }

    return unreplicate

    async function unreplicate () {
      const closing = []
      for (const [base, others] of adds) {
        closing.push(base.unreplicate(others))
      }
      return Promise.all(closing)
    }
  }

  async addWriters (writers, { indexers = this.indexers, indexer = false, serial = false, random = false } = {}) {
    const joins = []
    const start = this.indexers.length

    for (let i = 0; i < writers.length; i++) {
      const writer = writers[i]
      const base = random
        ? indexers[random(indexers.length) - 1]
        : this.root

      const join = writer.join({ indexer, base })

      if (serial) await join
      else joins.push(join)
    }

    if (!indexer) return

    while (this.indexers.length < start + writers.length) {
      await this._confirm()
    }
  }

  async confirm (indexers) {
    await this._confirm(indexers)
    await this._confirm(indexers)
  }

  async _confirm (indexers = this.indexers) {
    const maj = (this.indexers.length >> 1) + 1

    const idx = shuffle(indexers).slice(0, maj)
    const added = idx[0].replicate(idx)

    await idx[0].sync(idx)
    await idx[idx.length - 1].append(null)

    for (let i = 0; i < maj; i++) {
      await idx[i].sync()
      await idx[i].append(null)
    }

    await idx[0].sync(idx)
    return idx[0].unreplicate(added)
  }

  netsplit (left, right) {
    const waits = []
    for (const base of left) {
      waits.push(base.unreplicate(right))
    }
    return Promise.all(waits)
  }

  spam (writers, messages) {
    if (typeof messages === 'number') {
      messages = writers.map(() => messages)
    }

    const complete = []
    for (let i = 0; i < writers.length; i++) {
      if (!writers[i].base.writable) continue
      complete.push(writers[i].spam(messages[i]))
    }

    return Promise.all(complete)
  }
}

module.exports = { Base, Room }

function defaultAddWriter (key, indexer) {
  return {
    add: {
      key: b4a.toString(key, 'hex'),
      indexer
    }
  }
}

function defaultMessage (seq) {
  return 'message ' + seq
}

function validateOpts (opts) {
  const baseOpts = {
    apply: defaultApply,
    open: defaultOpen,
    valueEncoding: 'json',
    ackInterval: 100 // smaller for testing
  }

  if (opts.apply) baseOpts.apply = opts.apply
  if (opts.open) baseOpts.open = opts.open
  if (opts.close) baseOpts.close = opts.close
  if (opts.valueEncoding) baseOpts.valueEncoding = opts.valueEncoding
  if (opts.ackInterval) baseOpts.ackInterval = opts.ackInterval

  return baseOpts
}

function defaultOpen (store) {
  return store.get('default')
}

async function defaultApply (nodes, view, base) {
  for (const node of nodes) {
    if (node.value.add) {
      const { key, indexer } = node.value.add
      await base.addWriter(b4a.from(key, 'hex'), { indexer })
      continue
    }

    await view.append(node.value)
  }
}

function streamGc (s) {
  return () => {
    s.destroy()
    return new Promise(resolve => {
      s.on('close', resolve)
    })
  }
}

function shuffle (arr, random = Math.random) {
  const shuffled = arr.slice()
  const len = shuffled.length
  for (let i = 0; i < shuffled.length; i++) {
    const offset = i + Math.floor(random() * (len - i))

    const swap = shuffled[offset]
    shuffled[offset] = shuffled[i]
    shuffled[i] = swap
  }

  return shuffled
}
