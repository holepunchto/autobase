const Corestore = require('corestore')
const { Replicator, Network } = require('replication-simulator')
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
    this.replicator = new Replicator(this, { replicate: Base.replicate })

    this._addWriter = opts.addWriter || defaultAddWriter
    this._message = opts.message || defaultMessage

    this.messageCount = 0
  }

  static replicate (base, isInitiator) {
    return base.store.replicate(isInitiator)
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
    if (!this.base.writable) throw new Error('Not writable.')
    return this.base.append(this._addWriter(key, indexer))
  }

  sync (bases) {
    if (!bases) return this._syncAll()

    if (!Array.isArray(bases)) bases = [bases]
    return sync([this.base].concat(bases.map(b => b.base)))
  }

  _syncAll () {
    const peers = [this, ...this.replicator.peers.keys()]
    return sync(peers.map(p => p.base))
  }

  replicate (remote) {
    const s1 = this.store.replicate(true)
    const s2 = remote.store.replicate(false)

    s1.pipe(s2).pipe(s1)

    return close

    async function close () {
      const p = new Promise(resolve => s1.on('close', resolve))
      s1.destroy()

      return p
    }
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

// entire autobase system
class Room {
  constructor (storage, opts = {}) {
    this._storage = storage
    this.root = opts.root || new Base(this._storage(), opts)
    this.opts = { ...opts, root: this.root }

    this.members = new Map()
    this.indexers = []
    this.rng = opts.rng || Math.random

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

  get key () {
    return this.root.base.bootstrap
  }

  * [Symbol.iterator] () {
    yield * this.members.values()
  }

  async createMember () {
    const member = new Base(this._storage(), this.opts)
    const unreplicate = this.root.replicate(member)

    await member.ready()
    await unreplicate()

    this.members.set(member.hex, member)
    return member
  }

  async createMembers (n) {
    const create = []
    for (let i = 0; i < n; i++) {
      create.push(this.createMember())
    }

    return Promise.all(create)
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
    return new Network(bases.map(b => b.replicator))
  }

  async addIndexers (writers, opts) {
    return this.addWriters(writers, { ...opts, indexer: true })
  }

  async addWriters (writers, { indexers = this.indexers, indexer = false, serial = false, random = false } = {}) {
    const joins = []
    const start = this.indexers.length

    for (let i = 0; i < writers.length; i++) {
      const writer = writers[i]
      const base = random
        ? indexers[getRandom(indexers.length, this.rng) - 1]
        : this.root

      const join = writer.join({ indexer, base })

      if (serial) await join
      else joins.push(join)
    }

    if (!indexer) return

    while (this.indexers.length < start + writers.length) {
      await this._confirm()
      this.replicate()
      await this.sync()
    }
  }

  async confirm (indexers) {
    await this._confirm(indexers)
    await this._confirm(indexers)
  }

  async _confirm (indexers = this.indexers) {
    const maj = (this.indexers.length >> 1) + 1

    if (indexers.length < maj) throw new Error('Not enough indexers to confirm.')

    const selected = shuffle(indexers, this.rng).slice(0, maj)

    await sync(selected.map(s => s.base))
    await selected[selected.length - 1].append(null)

    for (let i = 0; i < maj; i++) {
      await sync(selected.map(s => s.base))
      await selected[i].append(null)
    }

    await sync(selected.map(s => s.base))
  }

  spam (writers, messages) {
    if (typeof messages === 'number') {
      messages = writers.map(() => messages)
    } else {
      while (messages.length < writers.length) {
        messages.push(messages[0])
      }
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
  if (opts.ackInterval !== undefined) baseOpts.ackInterval = opts.ackInterval
  if (opts.ackThreshold !== undefined) baseOpts.ackThreshold = opts.ackThreshold
  if (opts.fastForward !== undefined) baseOpts.fastForward = opts.fastForward

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

function getRandom (n, rng) {
  return Math.floor(rng() * n)
}

function shuffle (arr, rng) {
  const shuffled = arr.slice()
  const len = shuffled.length
  for (let i = 0; i < shuffled.length; i++) {
    const offset = i + Math.floor(rng() * (len - i))

    const swap = shuffled[offset]
    shuffled[offset] = shuffled[i]
    shuffled[i] = swap
  }

  return shuffled
}
