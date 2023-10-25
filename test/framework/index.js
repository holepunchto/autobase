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
  }

  ready () {
    return this.base.ready()
  }

  async join ({ indexer = false, base = this.root } = {}) {
    const writable = new Promise(resolve => {
      this.base.once('writable', resolve)
    })

    base.addWriter(this.base.local.key, indexer)
    await writable

    await this.append(null)
    await base.sync()
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
    for (const base of bases) this._replicate(base)
  }

  _replicate (remote) {
    if (this._streams.has(remote.base)) return

    const s1 = this.store.replicate(true)
    const s2 = remote.store.replicate(false)

    this._streams.set(remote.base, streamGc(s1))
    remote._streams.set(this.base, streamGc(s2))

    s1.on('close', () => this._streams.delete(remote.base))
    s2.on('close', () => remote._streams.delete(this.base))

    s1.pipe(s2).pipe(s1)
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
}

module.exports = { Base }

function defaultAddWriter (key, indexer) {
  return {
    add: {
      key: b4a.toString(key, 'hex'),
      indexer
    }
  }
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
