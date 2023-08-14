const test = require('brittle')
const Hyperbee = require('hyperbee')

const {
  create,
  confirm,
  apply,
  addWriter,
  sync
} = require('./helpers')

const beeOpts = { extension: false, keyEncoding: 'binary', valueEncoding: 'binary' }

test('check snapshot of snapshot after rebase', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))
  const [base1, base2, base3] = bases

  await addWriter(base1, base2)
  await addWriter(base1, base3)
  await confirm(bases)

  await base1.append('1-1')
  await base1.append('1-2')
  await base2.append('2-1')
  await base2.append('2-2')

  const orig1 = base1.view.snapshot()
  const orig2 = base2.view.snapshot()

  const origValue1 = await orig1.get(1)
  const origValue2 = await orig2.get(1)

  await confirm(bases)

  const resnap1 = orig1.snapshot()
  const resnap2 = orig2.snapshot()

  const resnapValue1 = await resnap1.get(1)
  const resnapValue2 = await resnap2.get(1)

  const newValue1 = await orig1.get(1)
  const newValue2 = await orig2.get(1)

  t.alike(origValue1, newValue1)
  t.alike(origValue2, newValue2)

  t.alike(origValue1, resnapValue1)
  t.alike(origValue2, resnapValue2)
})

test('no inconsistent snapshot entries when truncated', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))
  const [base1, base2, base3] = bases

  await base1.append({ add: base2.local.key.toString('hex') })
  await base1.append({ add: base3.local.key.toString('hex') })

  await confirm(bases)

  await base1.append('1-1')
  await base1.append('1-2')
  await base2.append('2-1')
  await base2.append('2-2')

  const orig1 = base1.view.snapshot()
  const orig2 = base2.view.snapshot()

  const origValue1 = await orig1.get(1)
  const origValue2 = await orig2.get(1)

  await confirm(bases)

  const newValue1 = await orig1.get(1)
  const newValue2 = await orig2.get(1)

  t.alike(origValue1, newValue1)
  t.alike(origValue2, newValue2)
})

test('no inconsistent snapshot-of-snapshot entries when truncated', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))
  const [base1, base2, base3] = bases

  await base1.append({ add: base2.local.key.toString('hex') })
  await base1.append({ add: base3.local.key.toString('hex') })

  await confirm(bases)

  await base1.append('1-1')
  await base1.append('1-2')
  await base2.append('2-1')
  await base2.append('2-2')

  const orig1 = base1.view.snapshot().snapshot()
  const orig2 = base2.view.snapshot().snapshot()

  const origValues1 = [await orig1.get(0), await orig1.get(1)]
  const origValues2 = [await orig2.get(0), await orig2.get(1)]
  t.alike(origValues1, ['1-1', '1-2']) // Sanity check

  let hasTruncated = false
  base2.view.on('truncate', function () { hasTruncated = true })
  base1.view.on('truncate', function () { hasTruncated = true })
  await confirm(bases)
  t.is(hasTruncated, true) // Sanity check

  const newValues1 = [await orig1.get(0), await orig1.get(1)]
  const newValues2 = [await orig2.get(0), await orig2.get(1)]

  t.alike(origValues1, newValues1)
  t.alike(origValues2, newValues2)
})

test('no inconsistent entries when using snapshot core in bee (bee snapshot)', async t => {
  // Setup
  const bases = await create(3, (...args) => applyForBee(t, ...args), openForBee)
  const [base1, base2, base3] = bases

  await base1.append({ add: base2.local.key.toString('hex') })
  await base1.append({ add: base3.local.key.toString('hex') })
  await confirm(bases)

  // Add shared entry
  await base1.append({ entry: ['1-1', '1-entry1'] })
  await confirm(base1, base2)

  // Create 2 forks and snapshot both
  await Promise.all([
    base1.append({ entry: ['1-2', '1-entry2'] }),
    base2.append({ entry: ['2-1', '2-entry1'] }),
    base2.append({ entry: ['2-2', '2-entry2'] })
  ])

  const bee1 = base1.view.snapshot()
  t.is(bee1.core.indexedLength, 2) // Sanity check
  t.is(bee1.version, 3) // Sanity check

  const bee2 = base2.view.snapshot()
  t.is(bee2.core.indexedLength, 2) // Sanity check
  t.is(bee2.version, 4) // Sanity check

  let hasTruncated = false
  base2.view.core.on('truncate', function () { hasTruncated = true })
  base1.view.core.on('truncate', function () { hasTruncated = true })

  const keysPreMerge = await getBeeKeys(bee1)
  const keys2PreMerge = await getBeeKeys(bee2)
  t.alike(keysPreMerge, ['1-1', '1-2']) // Sanity check
  t.alike(keys2PreMerge, ['1-1', '2-1', '2-2']) // Sanity check

  // Merge the forks, which will result in truncates
  await confirm(base1, base2)

  const keysPostMerge = await getBeeKeys(bee1)
  const keys2PostMerge = await getBeeKeys(bee2)

  t.alike(keysPreMerge, keysPostMerge)
  t.alike(keys2PreMerge, keys2PostMerge)

  t.is(hasTruncated, true) // Sanity check
})

test('check cloning detached snapshot', async t => {
  const bases = await create(4, apply, store => store.get('test', { valueEncoding: 'json' }))
  const [base1, base2, base3, base4] = bases

  await addWriter(base1, base2)
  await addWriter(base1, base3)
  await confirm(bases)

  await base2.append('1-1')
  await base2.append('1-2')
  await base2.append('1-3')
  await base2.append('1-4')
  await base2.append('1-5')
  await base2.append('1-6')

  const orig = base2.view.snapshot()

  await base1.append('2-1')
  await base1.append('2-2')

  await confirm(base1, base3)
  await sync(base1, base4)

  const resnap = orig.snapshot()

  await base1.append('2-3')
  await resnap.close()
  await confirm(base1, base3)

  // todo: this test throws uncaught error
  await t.execution(confirm(bases))
})

async function applyForBee (t, batch, view, base) {
  for (const { value } of batch) {
    if (value === null) continue
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'), { isIndexer: true })
    } else {
      try {
        await view.put(...value.entry, { update: false })
      } catch (e) {
        console.error(e)
        t.fail()
      }
    }
  }
}

function openForBee (linStore) {
  const core = linStore.get('simple-bee', { valueEncoding: 'binary' })
  const view = new Hyperbee(core, beeOpts)
  return view
}

async function getBeeKeys (bee) {
  const res = []
  for await (const entry of bee.createReadStream()) res.push(entry.key.toString())
  return res
}
