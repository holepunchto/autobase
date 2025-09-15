const sodium = require('sodium-universal')
const crypto = require('hypercore-crypto')
const tmpDir = require('test-tmp')
const b4a = require('b4a')
const Corestore = require('corestore')

const Autobase = require('../')

class Room {
  constructor (base) {
    this.base = base
  }

  get id () {
    return this.base.local.id
  }

  get key () {
    return this.base.local.key
  }

  get view () {
    return this.base.view
  }

  static open (store, base) {
    return store.get('view', { valueEncoding: 'json' })
  }

  static async apply (batch, view, base) {
    for (const node of batch) {
      switch (node.value.op) {
        case 'add':
        case 'indexer-remove': {
          const key = b4a.from(node.value.data, 'hex')
          await base.addWriter(key, { indexer: false })
          break
        }

        case 'indexer-add': {
          const key = b4a.from(node.value.data, 'hex')
          await base.addWriter(key, { indexer: true })
          break
        }

        case 'remove': {
          const key = b4a.from(node.value.data, 'hex')
          await base.removeWriter(key)
          break
        }

        case 'data':
          await view.append(node.value.data)
          break
      }
    }
  }

  state () {
    if (!this.base._applyState) return null
    const sys = this.base._applyState.system

    return sys.getIndexedInfo(sys.core.length)
  }

  write ({ op, data = null, optimistic = false } = {}) {
    if (!op) op = this.randomOp()
    if (!data) data = this.randomData(op)

    this.base.append({ op, data })
  }

  add (key) {
    return this.write({ op: 'add', data: b4a.toString(key, 'hex') })
  }

  remove (key) {
    return this.write({ op: 'remove', data: b4a.toString(key, 'hex') })
  }

  addIndexer (key) {
    return this.write({ op: 'indexer-add', data: b4a.toString(key, 'hex') })
  }

  removeIndexer (key) {
    return this.write({ op: 'indexer-remove', data: b4a.toString(key, 'hex') })
  }

  data (data) {
    return this.write({ op: 'data', data })
  }

  ack () {
    return this.base.ack()
  }

  encryption () {
    throw new Error('Not implemented')
  }

  fork () {
    throw new Error('Not implemented')
  }
}

class State {
  constructor (host) {
    this.host = host

    this.root = null
    this.key = null

    this.rooms = new Map()
    this.writers = new Set()
    this.indexers = new Set()

    this._replicating = false
    this._pendingIndexerAdd = 0
    this._pendingIndexerRemove = 0

    this.ops = new Set([
      { name: 'add', fn: this.add.bind(this) },
      { name: 'remove', fn: this.remove.bind(this) },
      { name: 'indexer-add', fn: this.addIndexer.bind(this) },
      { name: 'indexer-remove', fn: this.removeIndexer.bind(this) },
      { name: 'write', fn: this.write.bind(this) },
      { name: 'ack', fn: this.ack.bind(this) }
    ])

    this.trace = []

    this.stats = {
      indexerAdd: 0,
      indexerRemove: 0,
      add: 0,
      remove: 0,
      write: 0,
      ack: 0
    }
  }

  get bases () {
    return [...this.rooms.values()].map(room => room.base)
  }

  async ready () {
    this.root = await this.join()

    this.key = this.root.key

    this.indexers.add(this.root.id)
    this.writers.add(this.root.id)
  }

  async close () {
    await this._unreplicate()
  }

  async join () {
    const base = await this.host._create()

    const room = new Room(base)
    this.rooms.set(room.id, room)

    base.on('writable', () => this.writers.add(room.id))
    base.on('unwritable', () => this.writers.delete(room.id))
    base.on('is-indexer', () => {
      this.indexers.add(room.id)
      this._pendingIndexerAdd--
    })

    base.on('is-non-indexer', () => {
      this.indexers.delete(room.id)
      this._pendingIndexerRemove--
    })

    if (this._replicating) {
      for (const peer of this.rooms.values()) {
        if (peer !== room) this._replicate(room, peer)
      }
    }

    return room
  }

  async add () {
    const writer = this.getWriter()
    const target = await this.getNonWriter()

    this.trace.push(['add', writer.id, target.id])
    this.stats.add++

    return writer.add(target.key)
  }

  async remove () {
    const writer = this.getWriter()

    let target = await this.getNonIndexer()
    while (!this.writers.has(target.id)) {
      target = await this.getNonIndexer()
    }

    this.trace.push(['remove', writer.id, target.id])
    this.stats.remove++

    return writer.remove(target.key)
  }

  async addIndexer () {
    this._pendingIndexerAdd++

    const writer = this.getWriter()
    const target = await this.getNonIndexer()

    this.trace.push(['indexer-add', writer.id, target.id])
    this.stats.indexerAdd++

    return writer.addIndexer(target.key)
  }

  removeIndexer () {
    this._pendingIndexerRemove++

    const writer = this.getWriter()
    const target = this.getIndexer()

    this.trace.push(['indexer-remove', writer.id, target.id])
    this.stats.indexerRemove++

    return writer.removeIndexer(target.key)
  }

  write () {
    const writer = this.getWriter()
    const data = this.getData()

    this.trace.push(['data', writer.id, data])
    this.stats.write++

    return writer.data(data)
  }

  ack () {
    const indexer = this.getIndexer()

    this.trace.push(['ack', indexer.id])
    this.stats.ack++

    return indexer.ack()
  }

  getWriter () {
    return this.rooms.get(randomElement(this.writers))
  }

  getIndexer () {
    return this.rooms.get(randomElement(this.indexers))
  }

  async getNonWriter () {
    while (true) {
      if (this.writers.size === this.rooms.size) {
        return this.join()
      }

      const [id, room] = randomElement(this.rooms)
      if (this.writers.has(id)) continue

      return room
    }
  }

  async getNonIndexer () {
    while (true) {
      if (this.indexers.size === this.writers.size) {
        return this.join()
      }

      const [id, room] = randomElement(this.rooms)
      if (this.indexers.has(id)) continue

      return room
    }
  }

  getData () {
    return 'some data'
  }

  _replicate (local, remote) {
    const teardown = replicate(local.base, remote.base)

    this._streams.push({
      local: local.id,
      remote: remote.id,
      teardown
    })
  }

  replicate () {
    this._replicating = true
    this._streams = []

    const rooms = [...this.rooms.values()]

    while (rooms.length > 1) {
      const local = rooms.shift()
      for (const remote of rooms) {
        this._replicate(local, remote)
      }
    }
  }

  unreplicate () {
    return Promise.all(this._streams.map(s => s.teardown()))
  }

  valid (operation) {
    switch (operation.name) {
      case 'remove':
        if (this.indexers.size === this.writers.size) return false
        break

      case 'indexer-remove':
        if ((this.indexers.size - this._pendingIndexerRemove) === 1) {
          return false
        }
        break

      case 'indexer-add':
        if (this.indexers.size + this._pendingIndexerAdd - this._pendingIndexerRemove > 5) return false
        break
    }

    return true
  }
}

class Fuzzer {
  constructor (opts = {}) {
    this.key = null
    this.seed = opts.seed || crypto.randomBytes(32)
    this.storage = opts.storage || (() => tmpDir(this))

    this.state = new State(this)

    this.opts = {
      valueEncoding: 'json',
      ackInterval: 0,
      ackThreshold: 0,
      fastForward: false,
      ...opts
    }

    this._teardown = []
  }

  async ready () {
    await this.state.ready()
    this.teardown(() => this.state.close())
  }

  async _create () {
    const primaryKey = this.deterministic(32)
    const dir = await this.storage()

    const store = new Corestore(dir, { primaryKey })

    const base = new Autobase(store, this.state.key, {
      open: Room.open,
      apply: Room.apply,
      ...this.opts
    })

    this.teardown(() => store.close(), { order: 2 })
    this.teardown(() => base.close())

    await base.ready()
    return base
  }

  teardown (fn, { order = 0 } = {}) {
    this._teardown.push({ fn, order })
  }

  deterministic (bytes) {
    const buf = b4a.alloc(bytes)
    sodium.crypto_generichash(buf, this.seed)

    this.seed = buf
    return buf
  }

  action () {
    let operation = randomElement(this.state.ops)
    while (!this.state.valid(operation)) {
      operation = randomElement(this.state.ops)
    }

    return operation.fn()
  }
}

run()

async function run () {
  const fuzzer = new Fuzzer()
  await fuzzer.ready()

  const b = await fuzzer.state.join()
  await fuzzer.state.root.add(b.key)

  fuzzer.state.replicate()

  for (let i = 0; i < 20; i++) {
    await fuzzer.action()
    await new Promise(resolve => setTimeout(resolve, 300))
  }

  console.log(fuzzer.state.trace)
  console.log(fuzzer.state.stats)
  console.log(await fuzzer.state.root.base._applyState.system.getIndexedInfo())
}

function randomElement (set) {
  let index = randomIndex(set.size)
  for (const element of set) {
    if (index-- === 0) return element
  }
}

function randomIndex (n) {
  return Math.floor(Math.random() * n)
}

function replicate (a, b) {
  const s1 = a.store.replicate(true)
  const s2 = b.store.replicate(false)

  s1.pipe(s2).pipe(s1)

  return teardown

  function teardown () {
    const closing = Promise.all([
      new Promise(resolve => s1.on('close', resolve)),
      new Promise(resolve => s1.on('close', resolve))
    ])

    s1.destroy()
    s2.destroy()

    return closing
  }
}
