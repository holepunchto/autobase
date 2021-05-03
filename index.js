const AutobaseCore = require('./core')
const MemoryView = require('./lib/views/memory')
const { Manifest } = require('./lib/manifest')

const INPUT_NAME = '@autobase/input'
const INDEX_NAME = '@autobase/index'

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
      console.log('CLOSING LOCAL INPUT')
      await this._localInput.close()
      this._localInput = null
    }
    if (!indexes.has(this._localIndex.key.toString('hex'))) {
      console.log('CLOSING LOCAL INDEX')
      await this._localIndex.close()
      this._localIndex = null
    }

    this._base = new AutobaseCore(this._inputs)
    this._opening = null
  }

  async refresh (opts = {}) {
    console.log('in refresh, awaiting open')
    if (this._opening) await this._opening
    console.log('in refresh, opening finished, local index?', !!this._localIndex)
    const rebasePromise = this._localIndex
      ? this._base.rebaseInto(this._localIndex, opts)
      : this._base.rebasedView(this._indexes, opts)
    const result = await rebasePromise
    console.log('in refresh, rebasePromise resolved')
    console.log('REFRESH RESULT:', { added: result.added, removed: result.removed })
    return result.index
  }

  createIndex (opts = {}) {
    const self = this
    let refreshing = false
    let currentIndex = self._localIndex

    const indexWrapper = MemoryView.async(open)
    return indexWrapper

    async function onupdate () {
      if (refreshing) return
      refreshing = true
      try {
        currentIndex = await self.refresh({ ...opts, view: currentIndex })
      } finally {
        refreshing = false
      }
    }

    async function open () {
      if (self._opening) await self._opening
      return {
        base: self._base,
        core: currentIndex,
        opts: {
          unwrap: false,
          includeInputNodes: true,
          onupdate
        }
      }
    }
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
