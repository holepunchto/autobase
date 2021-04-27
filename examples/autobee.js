const Hyperbee = require('hyperbee')

const Autobase = require('..')

module.exports = class Autobee {
  constructor (corestore, manifest, local, opts) {
    this.autobase = new Autobase(corestore, manifest, local)
    this.opts = opts

    this.bee = null // Set in _open

    this._opening = this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  async _open () {
    await this.autobase.ready()
    const index = await this.autobase.createIndex({
      apply: this._apply.bind(this)
    })
    this.bee = new Hyperbee(index, {
      ...this.opts,
      extension: false
    })
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
    if (this._opening) await this._opening
    const op = Buffer.from(JSON.stringify({ type: 'put', key, value }))
    return this.autobase.append(op)
  }

  async get (key) {
    if (this._opening) await this._opening
    return this.bee.get(key)
  }
}

function noop () {}
