const ram = require('random-access-memory')
const Corestore = require('corestore')
const helpers = require('autobase-test-helpers')
const same = require('same-data')
const b4a = require('b4a')

const Autobase = require('../..')
const encryptionKey = process.env && process.env.ENCRYPT_ALL ? b4a.alloc(32).fill('autobase-encryption-test') : undefined

module.exports = {
  createStores,
  createBase,
  create,
  addWriter,
  addWriterAndSync,
  apply,
  confirm,
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
    bases.push(await createBase(stores[i], bases[0].local.key, t, opts))
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

  t.teardown(() => base.close(), { order: 1 })

  return base
}

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function addWriter (base, add, indexer = true) {
  return base.append({ add: add.local.key.toString('hex'), indexer })
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
  await helpers.replicateAndSync(bases)

  for (let i = 0; i < 2; i++) {
    const writers = bases.filter(b => !!b.localWriter)
    const maj = options.majority || (Math.floor(writers.length / 2) + 1)
    for (let j = 0; j < maj; j++) {
      if (!writers[j].writable) continue

      await writers[j].append(null)
      await helpers.replicateAndSync(bases)
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
