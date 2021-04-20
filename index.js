const AutobaseCore = require('./core')
const { Manifest, User } = require('./lib/manifest')
const MemoryView = require('./lib/views/memory')

const INPUT_NAME = '@autobase/input'
const INDEX_NAME = '@autobase/output'

module.exports = class Autobase {
  constructor (store, manifest, local) {
    this.store = store
    this.manifest = manifest
    this.local = local

    this._base = null
    this._inputs = null
    this._indexes = null
    this._localInput = null
    this._localIndex = null

    this._opening = this._open()
    this._opening.catch(noop)
  }

  async _open () {
    this.manifest = Manifest.inflate(this.store, this.manifest)
    this.local = Manifest.inflate(this.store, this.local)

    this._writers = this.manifest.filter(u => !!u.input)
    this._indexes = this.manifest.filter(u => !!u.index)
    this._localInput = this.local.input
    this._localIndex = this.local.index

    this._base = new AutobaseCore(this._inputs)
    this._opening = null
  }

  async _refresh (view, opts = {}) {
    opts = { ...opts, map: opts.apply }
    const rebasePromise = this._localIndex
      ? this._base.rebaseInto(view || this._localIndex, opts)
      : this._base.rebasedView(view || this._indexes, opts)
    const { index } = await rebasePromise
    return index
  }

  async createIndex (opts = {}) {
    const view = await MemoryView.from(this._base, this._localIndex, {
      unwrap: true,
      includeInputNodes: true,
      onupdate: () => this._refresh(view, opts)
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
      input: opts.input !== false ? store.get({ name: INPUT_NAME, token: opts.token }) : null,
      index: opts.index !== false ? store.get({ name: INDEX_NAME, token: opts.token }) : null
    }
    await Promise.allSettled([user.input.ready(), user.index.ready()])
    const id = await store.immutable.put(User.deflate(user))
    return { user, id }
  }
}

function noop () { }
