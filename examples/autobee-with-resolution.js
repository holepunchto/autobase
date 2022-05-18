const Hyperbee = require('hyperbee')

/*
 * This version of Autobee does very limited conflict resolution:
 * 1) It only supports put operations
 * 2) If a key has multiple conflicts, it will only show the latest one
 *
 * More sophisticated conflict resolution strategies (deletion handling) will require adding
 * additional causal metadata to each input operation at the Autobee layer.
 *
 * As an example, each operation must "link" to any previous operation it's overwriting.
 */
module.exports = class Autobee {
  constructor (autobase, opts) {
    this.autobase = autobase
    this.autobase.start({
      unwrap: true,
      apply: applyAutobeeBatch,
      view: core => {
        return new Hyperbee(core.unwrap(), {
          ...opts,
          extension: false
        })
      }
    })
    this.bee = this.autobase.view
  }

  ready () {
    return this.autobase.ready()
  }

  async put (key, value, opts) {
    const op = Buffer.from(JSON.stringify({ type: 'put', key, value }))
    return await this.autobase.append(op, opts)
  }

  async get (key) {
    const node = await this.bee.get(key)
    if (!node) return null
    node.value = decode(node.value).value
    return node
  }
}

async function applyAutobeeBatch (bee, batch, clocks, change) {
  const localClock = clocks.local
  const b = bee.batch({ update: false })

  for (const node of batch) {
    const op = JSON.parse(node.value.toString())
    if (op.type === 'put') {
      const existing = await b.get(op.key, { update: false })
      await b.put(op.key, encode(op.value, change, node.seq))
      if (!existing) continue
      await handleConflict(existing)
    }
  }

  return await b.flush()

  async function handleConflict (existing) {
    const { change: existingChange, seq: existingSeq } = decode(existing.value)
    // If the existing write is not causally contained in the current clock.
    // TODO: Write a helper for this.
    const conflictKey = ['_conflict', existing.key].join('/')
    if (!localClock.has(existingChange) || (localClock.get(existingChange) < existingSeq)) {
      await b.put(conflictKey, existing.value)
    } else {
      await b.del(conflictKey)
    }
  }
}

function encode (value, change, seq) {
  return JSON.stringify({ value, change: change.toString('hex'), seq })
}

function decode (raw) {
  return JSON.parse(raw)
}
