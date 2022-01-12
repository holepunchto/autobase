const Hyperbee = require('hyperbee')

module.exports = class SimpleAutobee {
  constructor (autobase, opts) {
    this.autobase = autobase
    this.autobase.start({
      unwrap: true,
      apply: this._apply.bind(this)
    })
    this.bee = new Hyperbee(this.autobase.view, {
      ...opts,
      extension: false
    })
  }

  ready () {
    return this.autobase.ready()
  }

  // A real apply function would need to handle conflicts, beyond last-one-wins.
  async _apply (batch) {
    const b = this.bee.batch({ update: false })
    for (const node of batch) {
      const op = JSON.parse(node.value.toString())
      // TODO: Handle deletions
      if (op.type === 'put') await b.put(op.key, op.value)
    }
    await b.flush()
  }

  async put (key, value, opts = {}) {
    const op = Buffer.from(JSON.stringify({ type: 'put', key, value }))
    return await this.autobase.append(op)
  }

  async get (key) {
    return await this.bee.get(key)
  }
}
