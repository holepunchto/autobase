const Autocore = require('./core')

module.exports = class AutoStore {
  constructor (base) {
    this.base = base
    this.opened = new Map()
    this.waiting = []
  }

  get (opts, moreOpts) {
    if (typeof opts === 'string') opts = { name: opts }
    if (moreOpts) opts = { ...opts, ...moreOpts }

    const name = opts.name
    const valueEncoding = opts.valueEncoding || null

    if (this.opened.has(name)) return this.opened.get(name).createSession(valueEncoding)

    const core = this.base.store.get({ name: 'view/' + name, cache: opts.cache, exclusive: true })
    const ac = new Autocore(this.base, core, name)

    this.waiting.push(ac)
    this.opened.set(name, ac)

    return ac.createSession(valueEncoding)
  }

  getIndexedCores () {
    const cores = []

    for (let i = 0; i < this.base.system.views.length; i++) {
      const v = this.base.system.views[i]
      const core = this.opened.get(v.name)
      if (!core || (!core.indexedLength && !core.indexing)) break
      core.likelyIndex = i // just in case its out of date...
      cores.push(core)
    }

    return cores
  }

  async flush () {
    while (this.waiting.length) {
      const core = this.waiting.pop()
      await core.ready()
    }
  }
}
