const Hyperbee = require('hyperbee')

const Autobase = require('..')

module.exports = class Autobee {
  constructor (corestore, manifest, local, opts = {}) {
    this.autobase = new Autobase(corestore, manifest, local)

    const index = this.autobase.createIndex({
      unwrap: true,
      apply: this._apply.bind(this)
    })
    this.bee = new Hyperbee(index, {
      ...opts,
      extension: false
    })

    this._opening = this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  _open () {
    return this.autobase.ready()
  }

  async _apply (batch, index) {
    const b = this.bee.batch({ update: false })
    for (const node of batch) {
      const op = JSON.parse(node.value.toString())
      switch (op.type) {
        case 'put':
          await b.put(op.key, op.value)
          break
        default:
          // Discard malformed messages
          break
      }
    }
    return b.flush()
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
