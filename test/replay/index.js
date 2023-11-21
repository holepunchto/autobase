const b4a = require('b4a')
const c = require('compact-encoding')

const Linearizer = require('../../lib/linearizer')
const Writer = require('../../lib/writer')
const { OplogMessage } = require('../../lib/messages')

class MockAutobase {
  constructor (writers, linearizer, opts = {}) {
    this.writers = writers
    this.linearizer = linearizer
    this.valueEncoding = opts.valueEncoding || c.from('binary')

    this._wakeup = {
      add () {}
    }

    this._viewStore = {
      getByIndex () {}
    }
  }

  _getWriterByKey (key) {
    return this.writers.get(b4a.toString(key, 'hex'))
  }
}

// Replays an autobase up to a given set of heads. The returned
// Linearizer will reflect the DAG at the given point.

module.exports = async function replayLinearizer (store, indexers, heads, encryptionKey) {
  const writers = await replayWriterState(store, heads, encryptionKey)
  return loadLinearizer(writers, indexers)
}

async function replayWriterState (store, heads, encryptionKey) {
  const writerState = new Map()

  const stack = [...heads]
  const visited = new Set()

  while (stack.length) {
    const { key, length } = stack.pop()
    const hex = b4a.toString(key, 'hex')

    const ref = toRef({ key, length })
    visited.add(ref)

    let w = writerState.get(hex)

    if (!w) {
      const core = store.get({
        key,
        valueEncoding: OplogMessage,
        compat: false,
        encryptionKey
      })

      w = { core, start: length - 1, end: length }
      writerState.set(hex, w)
    }

    if (length > w.end) w.end = length
    if (length - 1 < w.start) w.start = length - 1

    const block = await w.core.get(length - 1, { wait: true })

    for (const { key, length } of block.node.heads) {
      if (visited.has(toRef({ key, length }))) continue
      stack.push({ key, length })
    }
  }

  return writerState
}

async function loadLinearizer (writerState, indexerKeys) {
  const writers = new Map()

  const base = new MockAutobase(writers)

  const ws = []
  for (const { core, start, end } of writerState.values()) {
    await core.ready()

    const writer = new Writer(base, core, start)
    writers.set(b4a.toString(core.key, 'hex'), writer)

    ws.push({ writer, missing: end - start })
  }

  const indexers = indexerKeys.map(key => writers.get(key))
  for (const idx of indexers) idx.isIndexer = true

  const linearizer = new Linearizer(indexers, { writers })

  base.linearizer = linearizer

  while (ws.length) {
    for (let i = 0; i < ws.length; i++) {
      const w = ws[i]

      try {
        await w.writer.update()
      } catch (e) {
        // will get removed below
      }

      const node = w.writer.advance()
      if (!node) continue
      if (w.writer.addedAt === undefined) {
        w.writer.addedAt = linearizer.size
      }

      linearizer.addHead(node)

      if (--w.missing > 0) continue

      const last = ws.pop()
      if (last !== w) ws[i--] = last
    }
  }

  return linearizer
}

function toRef (node) {
  return node.key.toString('hex') + ':' + node.length
}
