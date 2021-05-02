const Hyperbee = require('hyperbee')
const HyperbeeExtension = require('hyperbee/lib/extension')

const Autobase = require('..')

class AutobeeExtension extends HyperbeeExtension {
  constructor (view) {
    super()
    this.view = view
  }

  get (version, key) {
    return super.get(this.view.indexLength, key)
  }
}

module.exports = class Autobee {
  constructor (corestore, manifest, local, opts = {}) {
    this.autobase = new Autobase(corestore, manifest, local)

    const index = this.autobase.createIndex({
      apply: this._apply.bind(this)
    })
    let ext = null
    if (opts.extension !== false) {
      ext = new AutobeeExtension(index)
      index.registerExtension('hyperbee', ext)
    }
    this.bee = new Hyperbee(index, {
      ...opts,
      extension: ext
    })
    if (ext) ext.db = this.bee

    this._opening = this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  _open () {
    return this.autobase.ready()
  }

  async _apply ({ node }) {
    const op = JSON.parse(node.value.toString())
    const b = this.bee.batch()

    const process = async op => {
      if (op.type === 'put') await b.put(op.key, op.value)
    }
    if (Array.isArray(op)) await Promise.all(op.map(process))
    else await process(op)

    await b.flush()
  }

  async put (key, value) {
    const op = Buffer.from(JSON.stringify({ type: 'put', key, value }))
    return this.autobase.append(op)
  }

  async get (key) {
    return this.bee.get(key)
  }

  createReadStream (...args) {
    return this.bee.createReadStream(...args)
  }

  refresh (...args) {
    return this.autobase.refresh(...args)
  }
}

function noop () {}
