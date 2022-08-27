const Hyperbee = require('hyperbee')

module.exports = class SimpleAutobee {
  constructor (autobase, opts) {
    this.autobase = autobase
    this.autobase.start({
      unwrap: true,
      apply: applyAutobeeBatch,
      view: core => new Hyperbee(core.unwrap(), {
        ...opts,
        extension: false
      })
    })
    this.bee = this.autobase.view
  }

  ready () {
    return this.autobase.ready()
  }

  async put (key, value) {
    const op = Buffer.from(JSON.stringify({ type: 'put', key, value }))
    return await this.autobase.append(op)
  }

  async get (key) {
    return await this.bee.get(key)
  }
}

// A real apply function would need to handle conflicts, beyond last-one-wins.
async function applyAutobeeBatch (bee, batch) {
  const b = bee.batch({ update: false })
  for (const node of batch) {
    const op = JSON.parse(node.value.toString())
    // TODO: Handle deletions
    if (op.type === 'put') await b.put(op.key, op.value)
  }
  await b.flush()
}
