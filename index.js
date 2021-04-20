const cenc = require('compact-encoding')

const AutobaseCore = require('./core')
const { Manifest, User } = require('./lib/manifest')

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
    this.manifest = await inflateManifest(this.store, this.manifest)
    this.local = await inflateUser(this.store, this.local)

    this._writers = this.manifest.writers
    this._localInput = this.local.input

    this._indexes = this.manifest.indexes
    this._localIndex = this.local.index

    this._base = new AutobaseCore(this._inputs)
    this._opening = null
  }

  async _refresh (opts = {}) {
    opts = {
      ...opts,
      includeInputNodes: true,
      unwrap: false,
      map: opts.apply
    }
    const rebasePromise = this._localIndex
      ? this._base.rebaseInto(this._localIndex, opts)
      : this._base.rebasedView(this._indexes, opts)
    const { index } = await rebasePromise
    return index
  }

  createIndex (opts = {}) {
    let currentView = null
    const update = async (...args) => {
      currentView = await this._refresh(opts)
      return currentView.update(...args)
    }
    const get = (...args) => {
      if (!currentView) return null
      return currentView.get(...args)
    }
    const proxy = new Proxy(currentView, {
      get (target, prop) {
        if (prop === 'update') return update
        if (prop === 'get') return get
        return target[prop]
      }
    })
    return proxy
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

  static async create (store, users, opts = {}) {
    const manifest = [
      ...users,
    ]
  }
}

function noop () {}
