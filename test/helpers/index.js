const ram = require('random-access-memory')
const Corestore = require('corestore')
const helpers = require('autobase-test-helpers')
const same = require('same-data')
const b4a = require('b4a')

const Autobase = require('../..')
const argv = typeof global.Bare !== 'undefined' ? global.Bare.argv : process.argv
const encryptionKey = argv.includes('--encrypt-all') ? b4a.alloc(32).fill('autobase-encryption-test') : undefined

module.exports = {
  createStores,
  createBase,
  create,
  addWriter,
  addWriterAndSync,
  apply,
  confirm,
  printIndexerTip,
  printTip,
  compare,
  compareViews,
  encryptionKey,
  ...helpers
}

async function createStores (n, t, opts = {}) {
  const storage = opts.storage || (() => ram.reusable())
  const offset = opts.offset || 0

  const stores = []
  for (let i = offset; i < n + offset; i++) {
    const primaryKey = Buffer.alloc(32, i)
    stores.push(new Corestore(await storage(), { primaryKey, encryptionKey }))
  }

  t.teardown(() => Promise.all(stores.map(s => s.close())), { order: 2 })

  return stores
}

async function create (n, t, opts = {}) {
  const stores = await createStores(n, t, opts)
  const bases = [await createBase(stores[0], null, t, opts)]

  if (n === 1) return { stores, bases }

  for (let i = 1; i < n; i++) {
    const base = await createBase(stores[i], bases[0].local.key, t, opts)
    // await base.ready()

    bases.push(base)
  }

  return {
    stores,
    bases
  }
}

async function createBase (store, key, t, opts = {}) {
  const moreOpts = {
    apply,
    open,
    close: undefined,
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    fastForward: false,
    ...opts
  }

  const base = new Autobase(store.session(), key, moreOpts)
  await base.ready()

  // if (opts.maxSupportedVersion !== undefined) {
  //   base.maxSupportedVersion = opts.maxSupportedVersion
  // }

  t.teardown(async () => {
    await base.close()
    await base._viewStore.close()
  }, { order: 1 })

  return base
}

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function addWriter (base, add, indexer = true) {
  return base.append({ add: add.local.key.toString('hex'), indexer })
}

function printIndexerTip (tip, indexers) {
  let string = '```mermaid\n'
  string += 'graph TD;\n'

  const clock = new Map()
  const indexerTip = []

  // find lower bound for each writer
  for (const node of tip) {
    if (!indexers.includes(node.writer)) continue

    indexerTip.push(node)

    const hex = node.writer.core.key.toString('hex')
    if (clock.has(hex) && clock.get(hex) < node.length) continue

    clock.set(hex, node.length)
  }

  const state = new Map()

  // tip is already sorted
  for (const node of indexerTip) {
    const label = `${node.writer.core.key[0].toString(16)}:${node.length}`

    if (!state.has(node.writer)) state.set(node.writer, new Map())
    const last = state.get(node.writer)

    for (let [key, length] of node.clock) {
      const hex = key.toString('hex')

      if (hex === node.writer.core.key.toString('hex')) length--
      if (length < clock.get(hex) || length <= last.get(hex)) continue

      last.set(hex, length)

      const depLabel = `${key[0].toString(16)}:${length}`
      string += `  ${label}-->${depLabel};\n`
    }
  }

  string += '```'

  return string
}

function printTip (tip, indexers) {
  let string = '```mermaid\n'
  string += 'graph TD;\n'

  for (const node of tip) {
    for (const dep of node.dependencies) {
      if (!tip.includes(dep)) continue

      let label = node.ref
      let depLabel = dep.ref

      if (indexers) {
        const index = indexers.indexOf(node.writer)
        const depIndex = indexers.indexOf(dep.writer)

        if (index !== -1) {
          const char = String.fromCharCode(0x41 + index)
          label = char + ':' + node.length
        }

        if (depIndex !== -1) {
          const char = String.fromCharCode(0x41 + depIndex)
          depLabel = char + ':' + dep.length
        }
      }

      string += `  ${labelNonNull(label, node)}-->${labelNonNull(depLabel, dep)};\n`
    }
  }

  string += '```'
  return string

  function labelNonNull (label, node) {
    return label + (node.value !== null ? '*' : '')
  }
}

async function addWriterAndSync (base, add, indexer = true, bases = [base, add]) {
  await addWriter(base, add, indexer)
  await helpers.replicateAndSync(bases)
  await base.ack()
  await helpers.replicateAndSync(bases)
}

async function confirm (bases, options = {}) {
  console.log('init')
  const timeout = setTimeout(async () => console.log(await Promise.all(bases.map(b => b.heads()))), 10_000)
  await helpers.replicateAndSync(bases)
  clearTimeuot(timeout)

  for (let i = 0; i < 2; i++) {
    const writers = bases.filter(b => !!b.localWriter)
    const maj = options.majority || (Math.floor(writers.length / 2) + 1)
    for (let j = 0; j < maj; j++) {
      if (!writers[j].writable) continue

      console.log('-', i, j)
      await writers[j].append(null)
      console.log('--')
      await helpers.replicateAndSync(bases)
      console.log('---')
    }
  }

  await helpers.replicateAndSync(bases)
}

async function compare (a, b, full = false) {
  const alen = full ? a.view.length : a.view.indexedLength
  const blen = full ? b.view.length : b.view.indexedLength

  if (alen !== blen) throw new Error('Views are different lengths')

  for (let i = 0; i < alen; i++) {
    const left = await a.view.get(i)
    const right = await b.view.get(i)

    if (!same(left, right)) throw new Error('Views differ at block ' + i)
  }
}

async function apply (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      const key = Buffer.from(value.add, 'hex')
      await base.addWriter(key, { indexer: value.indexer })
      continue
    }

    if (view) await view.append(value)
  }
}

function compareViews (bases, t) {
  const missing = bases.slice()

  const a = missing.shift()

  for (const b of missing) {
    for (const [name, left] of a._viewStore.opened) {
      const right = b._viewStore.opened.get(name)
      if (!right) {
        t.fail(`missing view ${name}`)
        continue
      }

      if (!b4a.equals(left.key, right.key)) {
        t.fail(`view key: ${name}`)
        continue
      }

      if (left.core.indexedLength !== right.core.indexedLength) {
        t.fail(`view length: ${name}`)
        continue
      }

      if (!b4a.equals(left.core.treeHash(), right.core.treeHash())) {
        t.fail(`view treeHash: ${name}`)
        continue
      }

      t.pass(`consistent ${name}`)
    }
  }
}
