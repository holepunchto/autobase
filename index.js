const AutobaseCore = require('./core')
const { Manifest } = require('./lib/manifest')
const MemoryView = require('./lib/views/memory')

const INPUT_NAME = '@autobase/input'
const INDEX_NAME = '@autobase/output'

const TOKEN = Buffer.from('46d08fc8fa0344bd5d6a09d5e119584e31c553812ea506e5e56bd0d41e9eb0e2', 'hex')

module.exports = class Autobase {
  constructor (store, manifest) {
    this.store = store
    this.manifest = manifest

    this._base = null
    this._inputs = null
    this._indexes = null
    this._localInput = null
    this._localIndex = null

    this._opening = this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  async _open () {
    this.manifest = Manifest.inflate(this.store, this.manifest)
    this._inputs = []
    this._indexes = []

    const indexes = new Set()
    const inputs = new Set()
    for (const { input, index } of this.manifest) {
      if (input) {
        this._inputs.push(input)
        inputs.add(input.key.toString('hex'))
      }
      if (index) {
        this._indexes.push(index)
        indexes.add(index.key.toString('hex'))
      }
    }

    this._localInput = this.store.get({ name: INPUT_NAME, token: TOKEN })
    this._localIndex = this.store.get({ name: INDEX_NAME, token: TOKEN })
    await this._localInput.ready()
    await this._localIndex.ready()

    if (!inputs.has(this._localInput.key.toString('hex'))) {
      await this._localInput.close()
      this._localInput = null
    }
    if (!indexes.has(this._localIndex.key.toString('hex'))) {
      await this._localIndex.close()
      this._localIndex = null
    }

    this._base = new AutobaseCore(this._inputs)
    this._opening = null
  }

  async _refresh (opts = {}) {
    const rebasePromise = this._localIndex
      ? this._base.rebaseInto(opts.view || this._localIndex, opts)
      : this._base.rebasedView(opts.view || this._indexes, opts)
    const result = await rebasePromise
    return result.index
  }

  async createIndex (opts = {}) {
    await this._refresh(opts)
    let refreshing = false
    const view = new MemoryView(this._base, this._localIndex, {
      unwrap: true,
      includeInputNodes: true,
      onupdate: async () => {
        if (refreshing) return
        refreshing = true
        try {
          await this._refresh({ ...opts, view })
        } finally {
          refreshing = false
        }
      }
    })
    return view
  }

  async append (value, links) {
    if (this._opening) await this._opening
    if (!this._localInput) throw new Error('Autobase does not have write capabilities.')
    links = links || await this._base.latest()
    return this._base.append(this._localInput, value, links)
  }

  static async createUser (store, opts = {}) {
    const user = {
      input: opts.input !== false ? store.get({ name: INPUT_NAME, token: TOKEN }) : null,
      index: opts.index !== false ? store.get({ name: INDEX_NAME, token: TOKEN }) : null
    }
    await Promise.allSettled([user.input.ready(), user.index.ready()])
    // TODO: Store in immutable-store-extension by default
    // const id = await store.immutable.put(User.deflate(user))
    return { user, id: null }
  }
}

function noop () { }
