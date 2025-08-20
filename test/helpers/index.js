const tmpDir = require('test-tmp')
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
  applyNode,
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
  const storage = opts.storage || (() => tmpDir(t))
  const offset = opts.offset || 0

  const stores = []
  for (let i = offset; i < n + offset; i++) {
    const primaryKey = Buffer.alloc(32, i)
    const globalCache = opts.globalCache || null
    const dir = await storage()
    stores.push(new Corestore(dir, { primaryKey, encryptionKey, globalCache }))
  }

  t.teardown(() => Promise.all(stores.map(s => s.close())), { order: 2 })

  return stores
}

async function create (n, t, opts = {}) {
  const stores = await createStores(n, t, opts)
  const bases = [createBase(stores[0], null, t, opts)]
  await bases[0].ready()
  bases[0].name = 'a'
  if (n === 1) return { stores, bases }

  for (let i = 1; i < n; i++) {
    const base = createBase(stores[i], bases[0].local.key, t, opts)
    await base.ready()
    bases.push(base)
    // naming them makes debugging easier so why not
    base.name = String.fromCharCode('a'.charCodeAt(0) + i)
  }

  return {
    stores,
    bases
  }
}

function createBase (store, key, t, opts = {}) {
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

  if (opts.maxSupportedVersion !== undefined) {
    base.maxSupportedVersion = opts.maxSupportedVersion
  }

  t.teardown(async () => {
    return base.close()
    // const view = new Promise(resolve => {
    //   setImmediate(() => base._viewStore.close().then(resolve, resolve))
    // })

    // await Promise.all([
    //   view,
    //   base.close()
    // ])
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
  await helpers.replicateAndSync(bases, options)

  for (let i = 0; i < 2; i++) {
    const writers = bases.filter(b => !!b.localWriter)
    const maj = options.majority || (Math.floor(writers.length / 2) + 1)
    for (let j = 0; j < maj; j++) {
      if (!writers[j].ackable) continue

      await writers[j].append(null)
      await helpers.replicateAndSync(bases, options)
    }
  }

  await helpers.replicateAndSync(bases, options)
}

async function compare (a, b, full = false) {
  const alen = full ? a.view.length : a.view.signedLength
  const blen = full ? b.view.length : b.view.signedLength

  if (alen !== blen) throw new Error('Views are different lengths')

  for (let i = 0; i < alen; i++) {
    const left = await a.view.get(i)
    const right = await b.view.get(i)

    if (!same(left, right)) throw new Error('Views differ at block ' + i)
  }
}

async function applyNode (node, view, base) {
  if (node.value.add) {
    const key = Buffer.from(node.value.add, 'hex')
    await base.addWriter(key, { indexer: node.value.indexer })
    return
  }

  if (view) await view.append(node.value)
}

async function apply (batch, view, base) {
  for (const node of batch) {
    await applyNode(node, view, base)
  }
}

async function compareViews (bases, t) {
  const missing = bases.slice()

  const a = missing.shift()

  for (const b of missing) {
    const views = []
    const aAutoViews = a._viewStore.getViews()
    const bAutoViews = b._viewStore.getViews()
    // missing a sync mechanic for awaiting flushes here
    for (let i = 0; i < aAutoViews.length; i++) {
      const v = aAutoViews[i]
      const left = v.core
      const right = bAutoViews[i]?.core
      views.push({
        name: v.name,
        left,
        right
      })
    }

    for (const { name, left, right } of views) {
      if (!right) {
        t.fail(`missing view ${name}`)
        continue
      }

      if (!b4a.equals(left.key, right.key)) {
        t.fail(`view key: ${name}`)
        continue
      }

      const length = left.signedLength

      if (right.signedLength !== length) {
        t.fail(`view signedLength: ${name}`)
        continue
      }

      if (!b4a.equals(await left.treeHash(length), await right.treeHash(length))) {
        t.fail(`view treeHash: ${name}`)
        continue
      }

      t.pass(`consistent ${name} at signedLength ${length}`)
    }
  }
}
