const test = require('brittle')
const ram = require('random-access-memory')
const Corestore = require('corestore')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const Autobase = require('..')

const {
  create,
  createBase,
  createStores,
  replicateAndSync,
  addWriter,
  addWriterAndSync,
  confirm,
  replicate,
  compare,
  compareViews
} = require('./helpers')

test('basic - two writers', async t => {
  const { bases } = await create(3, t, { open: null })
  const [base1, base2, base3] = bases

  await addWriter(base1, base2)

  await confirm([base1, base2, base3])

  await addWriter(base2, base3)

  await confirm([base1, base2, base3])

  t.is(base2.system.members, 3)
  t.is(base2.system.members, base3.system.members)
  t.is(base2.system.members, base2.activeWriters.size)
  t.is(base3.system.members, base3.activeWriters.size)

  t.ok(base1.isIndexer)
  t.ok(base2.isIndexer)
  t.ok(base3.isIndexer)

  t.not(base1.system.core.manifest, null)
  // tests skipped: fix with linearizer update - batching

  // t.alike(await base1.system.checkpoint(), await base2.system.checkpoint())
  // t.alike(await base1.system.checkpoint(), await base3.system.checkpoint())
})

test('basic - no truncates when history is linear', async t => {
  const { bases } = await create(3, t)
  const [base1, base2, base3] = bases

  await addWriter(base1, base2, false)

  await confirm([base1, base2, base3])

  await addWriter(base2, base3, false)

  await confirm([base1, base2, base3])

  await base2.append('hello')

  await replicateAndSync([base1, base2, base3])

  await base1.append('world')

  await replicateAndSync([base1, base2, base3])

  await base3.append('hej')

  await replicateAndSync([base1, base2, base3])

  await base1.append('verden')

  const all = []
  for (let i = 0; i < base1.view.indexedLength; i++) {
    all.push(await base1.view.get(i))
  }

  t.alike(all, ['hello', 'world', 'hej', 'verden'])
  t.is(base1.view.fork, 0)
  t.is(base2.view.fork, 0)
  t.is(base3.view.fork, 0)
})

test('basic - writable event fires', async t => {
  t.plan(1)
  const { bases } = await create(2, t, { open: null })
  const [base1, base2] = bases

  base2.on('writable', () => {
    t.ok(base2.writable, 'Writable event fired when autobase writable')
  })

  await addWriter(base1, base2)

  await confirm([base1, base2])
})

test('basic - local key pair', async t => {
  const keyPair = crypto.keyPair()
  const [store] = await createStores(1, t)

  const base = await createBase(store, null, t, { keyPair })
  const key = base.bootstrap

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.view.indexedLength, 1)
  t.alike(await base.view.get(0), block)
  t.alike(base.local.manifest.signers[0].publicKey, keyPair.publicKey)

  await base.close()

  const base2 = await createBase(store, key, t)
  t.alike(base2.local.key, base.local.key)
  t.alike(await base2.view.get(0), block)
  t.alike(base2.local.manifest.signers[0].publicKey, keyPair.publicKey)
})

test('basic - view', async t => {
  const { bases } = await create(1, t)
  const [base] = bases

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.system.members, 1)
  t.is(base.view.indexedLength, 1)
  t.alike(await base.view.get(0), block)
})

test('basic - view with close', async t => {
  const { bases } = await create(1, t, { open, close })
  const [base] = bases

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.system.members, 1)
  t.is(base.view.core.indexedLength, 1)
  t.alike(await base.view.core.get(0), block)

  t.is(base.view.lastBlock, null)
  await base.close()
  t.is(base.view.lastBlock.message, 'hello, world!')

  function open (store) {
    const core = store.get('test', { valueEncoding: 'json' })
    return {
      core,
      lastBlock: null,
      append: v => core.append(v)
    }
  }

  async function close (view) {
    view.lastBlock = await view.core.get(view.core.length - 1)
    await view.core.close()
  }
})

test('basic - view/writer userdata is set', async t => {
  const { bases } = await create(2, t)
  const [base1, base2] = bases

  await addWriter(base1, base2)

  await confirm(bases)

  await verifyUserData(base1)
  await verifyUserData(base2)

  async function verifyUserData (base) {
    const systemData = await Autobase.getUserData(base.system.core)

    t.alike(systemData.referrer, base.bootstrap)

    t.is(base.activeWriters.size, 2)
    for (const writer of base.activeWriters) {
      const writerData = await Autobase.getUserData(writer.core)
      t.alike(writerData.referrer, base.bootstrap)
    }
  }
})

test('basic - compare views', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases
  await addWriter(a, b)

  await confirm(bases)

  for (let i = 0; i < 6; i++) await bases[i % 2].append('msg' + i)

  await confirm(bases)

  t.is(a.system.members, b.system.members)
  t.is(a.view.indexedLength, b.view.indexedLength)

  compareViews([a, b], t)
})

test('basic - online majority', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  await addWriter(a, b)
  await addWriter(a, c)

  await confirm(bases)

  await a.append({ message: 'a0' })
  await b.append({ message: 'b0' })
  await c.append({ message: 'c0' })

  await confirm(bases)

  const indexed = a.view.indexedLength

  await a.append({ message: 'a1' })
  await b.append({ message: 'b1' })
  await c.append({ message: 'c1' })
  await a.append({ message: 'a2' })
  await b.append({ message: 'b2' })
  await c.append({ message: 'c2' })

  await confirm([a, b])

  t.not(a.view.indexedLength, indexed)
  t.is(c.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)

  compareViews([a, b], t)

  await replicateAndSync([b, c])

  t.is(a.view.indexedLength, c.view.indexedLength)

  compareViews([a, b, c], t)
})

test('basic - rotating majority', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  await addWriter(a, b)
  await addWriter(a, c)

  await confirm(bases)

  await a.append('a0')
  await b.append('b0')
  await c.append('c0')

  await confirm(bases)

  let indexed = a.view.indexedLength

  await a.append('a1')
  await b.append('b1')
  await c.append('c1')
  await a.append('a2')
  await b.append('b2')
  await c.append('c2')

  await confirm([a, b])

  t.not(a.view.indexedLength, indexed)
  t.is(c.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)

  indexed = a.view.indexedLength

  await a.append('a3')
  await b.append('b3')
  await c.append('c3')
  await a.append('a4')
  await b.append('b4')
  await c.append('c4')

  await confirm([b, c])

  t.not(b.view.indexedLength, indexed)
  t.is(a.view.indexedLength, indexed)
  t.is(b.view.indexedLength, c.view.indexedLength)

  indexed = b.view.indexedLength

  await a.append('a5')
  await b.append('b5') // 8b:15
  await c.append('c5')
  await a.append('a6')
  await b.append('b6') // 8b:16
  await c.append('c6')

  await confirm([a, c])

  t.not(c.view.indexedLength, indexed)
  t.is(b.view.indexedLength, indexed)
  t.is(a.view.indexedLength, c.view.indexedLength)

  indexed = a.view.indexedLength

  await a.append('a7')
  await b.append('b7')
  await c.append('c7')
  await a.append('a8')
  await b.append('b8')
  await c.append('c8')

  b.debug = true
  await confirm(bases)

  t.not(a.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(a.view.indexedLength, c.view.indexedLength)

  compareViews([a, b, c], t)
})

test('basic - throws', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases

  await a.append('msg1')
  await a.append('msg2')
  await a.append('msg3')

  await confirm([a, b])

  await t.exception(b.append('not writable'))
  await t.exception(a.view.append('append outside apply'))
  await t.exception(() => a.addWriter(b.local.key))
})

test('basic - add 5 writers', async t => {
  const { bases } = await create(5, t)

  const [a, b, c, d, e] = bases

  let migrate = 0
  e.system.core.on('migrate', () => migrate++)

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await confirm(bases)

  t.is(a.activeWriters.size, 5)
  t.is(a.system.members, 5)

  t.is(a.activeWriters.size, b.activeWriters.size)
  t.is(a.system.members, b.system.members)

  t.is(a.activeWriters.size, c.activeWriters.size)
  t.is(a.system.members, c.system.members)

  t.is(a.activeWriters.size, d.activeWriters.size)
  t.is(a.system.members, d.system.members)

  t.is(a.activeWriters.size, e.activeWriters.size)
  t.is(a.system.members, e.system.members)

  t.ok(migrate > 0)
})

test('basic - online minorities', async t => {
  const { bases } = await create(5, t)

  const [a, b, c, d, e] = bases

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await confirm(bases)

  t.is(a.view.indexedLength, c.view.indexedLength)

  await a.append('msg0')
  await b.append('msg1')
  await c.append('msg2')
  await d.append('msg3')
  await e.append('msg4')
  await a.append('msg5')
  await b.append('msg6')
  await c.append('msg7')
  await d.append('msg8')
  await e.append('msg9')

  await a.append('msg10')
  await b.append('msg11')
  await a.append('msg12')
  await b.append('msg13')
  await a.append('msg14')

  await d.append('msg15')
  await c.append('msg16')
  await d.append('msg17')
  await c.append('msg18')
  await d.append('msg19')
  await c.append('msg20')
  await d.append('msg21')
  await c.append('msg22')

  await confirm([a, b])
  await confirm([c, d])

  t.not(a.view.length, a.view.indexedLength)
  t.is(a.view.length, b.view.length)
  t.not(c.view.length, a.view.length)
  t.is(c.view.length, d.view.length)

  compareViews([a, b], t)
  compareViews([c, d], t)

  await t.execution(compare(a, b, true))
  await t.execution(compare(c, d, true))

  await confirm(bases)

  t.is(a.view.length, c.view.length)
  t.is(a.view.indexedLength, c.view.indexedLength)

  compareViews(bases, t)

  await t.execution(compare(a, b, true))
  await t.execution(compare(a, c, true))
  await t.execution(compare(a, d, true))
  await t.execution(compare(a, e, true))
})

test('basic - restarting sets bootstrap correctly', async t => {
  const store = new Corestore(ram.reusable())

  let bootstrapKey = null
  let localKey = null

  {
    const ns = store.namespace('random-name')
    const base = new Autobase(ns, null, { ackInterval: 0, ackThreshold: 0 })
    await base.ready()

    bootstrapKey = base.bootstrap
    localKey = base.local.key

    await base.close()
  }

  {
    const ns = store.namespace(bootstrapKey)
    const base = new Autobase(ns, bootstrapKey, { ackInterval: 0, ackThreshold: 0 })
    await base.ready()

    t.alike(base.bootstrap, bootstrapKey)
    t.alike(base.local.key, base.bootstrap)
    t.alike(base.local.key, localKey)
  }
})

test('batch append', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases
  a.on('error', (e) => console.error(e))
  b.on('error', (e) => console.error(e))

  await addWriter(a, b)

  await confirm(bases)

  await a.append(['a0', 'a1'])
  await t.execution(confirm(bases))
})

test('undoing a batch', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases
  a.on('error', (e) => console.error(e))
  b.on('error', (e) => console.error(e))

  await addWriter(a, b)

  await confirm(bases)

  await a.append('a0')
  await confirm(bases)

  await Promise.all([
    a.append('a1'),
    b.append(['b0', 'b1'])
  ])

  await t.execution(confirm(bases))
})

test('append during reindex', async t => {
  const { bases } = await create(4, t)

  const [a, b, c, d] = bases

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await replicateAndSync(bases)

  t.ok(!!a.localWriter)
  t.ok(!!b.localWriter)
  t.ok(!!c.localWriter)
  t.ok(!!d.localWriter)

  await addWriter(b, d)

  const unreplicate = replicate([b, d])

  for (const w of d.activeWriters) {
    if (w === d.localWriter) continue

    const start = w.core.contiguousLength
    const end = w.core.length

    await w.core.download({ start, end }).done()
  }

  await d.append('hello')

  t.is(await d.view.get(d.view.length - 1), 'hello')

  await unreplicate()
})

test('closing an autobase', async t => {
  const { bases } = await create(1, t)
  const [base] = bases

  // Sanity check
  t.is(base.local.closed, false)

  await base.close()
  t.is(base.local.closed, true)
})

test('flush after reindex', async t => {
  const { bases } = await create(9, t)

  const root = bases[0]
  const adds = []
  let msg = 0

  adds.push(addWriter(root, bases[1]))
  adds.push(addWriter(root, bases[2]))
  adds.push(addWriter(root, bases[3]))
  adds.push(addWriter(root, bases[4]))
  adds.push(addWriter(root, bases[5]))
  adds.push(addWriter(root, bases[6]))
  adds.push(addWriter(root, bases[7]))
  adds.push(addWriter(root, bases[8]))

  await Promise.all(adds)
  await replicateAndSync(bases)

  await t.execution(bases[0].append('msg' + msg++))
  await t.execution(bases[1].append('msg' + msg++))
  await t.execution(bases[2].append('msg' + msg++))
  await t.execution(bases[3].append('msg' + msg++))
  await t.execution(bases[4].append('msg' + msg++))
  await t.execution(bases[5].append('msg' + msg++))
  await t.execution(bases[6].append('msg' + msg++))
  await t.execution(bases[7].append('msg' + msg++))
  await t.execution(bases[8].append('msg' + msg++))
})

test('reindex', async t => {
  const { bases } = await create(5, t)

  const [a, b, c, d, e] = bases

  let msg = 0

  await Promise.all([
    addWriter(a, b),
    addWriter(a, c)
  ])

  await confirm([a, b, c])

  // a sends message
  await a.append('a:' + msg++)

  // b and c add writer
  await addWriter(b, d)
  await confirm([b, c, d], { majority: 2 })

  t.is(b.system.members, 4)

  // trigger reindex for a
  await replicateAndSync([a, b, c, d])

  await a.append('a:' + msg++)
  await b.append('b:' + msg++)
  await c.append('c:' + msg++)
  await d.append('d:' + msg++)

  await replicateAndSync([a, b, c, d])

  // d sends message
  await d.append('d:' + msg++)

  // a, b and c add writer
  await addWriter(a, e)
  await confirm([a, b, c, e], { majority: 3 })

  t.is(b.system.members, 5)

  // trigger reindex for a
  await replicateAndSync([a, b, c, d, e])

  compareViews(bases, t)

  t.is((await a.system.getIndexedInfo()).heads.length, 1, 'only one indexed head')
  t.is(a.system.members, bases.length)

  t.not(a.view.indexedLength, a.view.length)

  for (let i = 1; i < bases.length; i++) {
    const [l, r] = [bases[0], bases[i]]

    t.is(l.view.indexedLength, r.view.indexedLength)
    t.is(l.view.length, r.view.length)
  }
})

test('sequential restarts', async t => {
  const { bases } = await create(9, t)

  const root = bases[0]
  const adds = []
  let msg = 0

  adds.push(addWriter(root, bases[1]))
  adds.push(addWriter(root, bases[2]))

  await Promise.all(adds)
  await confirm(bases.slice(0, 3))

  for (let i = 2; i < bases.length + 6; i++) {
    const appends = []

    const syncers = bases.slice(0, i + 1)
    const [isolated] = syncers.splice(i - 1, 1)

    // confirm over this node
    // include all dag except previous addWriter
    await replicateAndSync(syncers)
    await bases[0].append(null)
    await replicateAndSync(syncers)

    // everyone writes a message
    for (let j = 0; j < Math.min(i, bases.length); j++) {
      appends.push(bases[j].append('msg' + msg++))
    }

    await Promise.all(appends)
    await replicateAndSync(syncers)

    // unsynced writer adds a node
    if (i < bases.length) {
      const newguy = bases[i]
      await addWriter(isolated, newguy)

      await replicateAndSync([isolated, newguy])
    }

    if (i % 2 === 1) {
      if (i < bases.length) {
        t.is(
          bases[0].linearizer.indexers.length,
          bases[i - 1].linearizer.indexers.length
        )
      }
    }
  }

  const indexedHeads = (await bases[0].system.getIndexedInfo()).heads
  t.is(indexedHeads.length, 1)
  t.alike(indexedHeads[0].key, bases[0].local.key)

  t.is(bases[0].system.members, bases.length)

  t.not(bases[0].view.indexedLength, 0)
  t.not(bases[0].view.indexedLength, bases[0].view.length)

  await replicateAndSync(bases)

  compareViews(bases, t)
})

test('two writers write many messages, third writer joins', async t => {
  const { bases } = await create(3, t)
  const [base1, base2, base3] = bases

  await addWriter(base1, base2)

  for (let i = 0; i < 10000; i++) {
    base1.append({ value: `Message${i}` })
  }

  await confirm([base1, base2])

  await addWriter(base1, base3)

  await confirm([base1, base2, base3])
  t.pass('Confirming did not throw')

  compareViews([base1, base2, base3], t)
})

test('basic - gc indexed nodes', async t => {
  const { bases } = await create(1, t)
  const [base] = bases

  await base.append({ message: '0' })
  await base.append({ message: '1' })
  await base.append({ message: '2' })
  await base.append({ message: '3' })
  await base.append({ message: '4' })

  t.is(base.system.members, 1)
  t.is(base.view.indexedLength, 5)
  t.is(base.localWriter.nodes.size, 0)

  t.alike(await base.view.get(0), { message: '0' })
  t.alike(await base.view.get(1), { message: '1' })
  t.alike(await base.view.get(2), { message: '2' })
  t.alike(await base.view.get(3), { message: '3' })
  t.alike(await base.view.get(4), { message: '4' })
})

test('basic - isAutobase', async t => {
  const { bases } = await create(3, t, { open: null })
  const [base1, base2, base3] = bases

  await addWriter(base1, base2)

  await confirm([base1, base2, base3])

  await t.exception(Autobase.isAutobase(base3.local, { wait: false }))

  await addWriter(base2, base3)

  await confirm([base1, base2, base3])

  t.is(await Autobase.isAutobase(base1.local), true)
  t.is(await Autobase.isAutobase(base2.local), true)

  await base3.append('hello')

  t.is(await Autobase.isAutobase(base3.local), true)
})

test('basic - non-indexed writer', async t => {
  const { bases } = await create(2, t, { apply: applyWriter })
  const [a, b] = bases

  await a.append({ add: b.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b])

  await b.append('b0')
  await b.append('b1')

  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  t.is(a.view.length, 2)
  t.is(b.view.length, 2)

  await a.append('a0')

  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)

  t.is(a.view.length, 3)
  t.is(b.view.length, 3)

  const a0 = await a.view.get(0)
  const b0 = await b.view.get(0)

  const a1 = await a.view.get(1)
  const b1 = await b.view.get(1)

  const a2 = await a.view.get(2)
  const b2 = await b.view.get(2)

  t.is(a0, 'b0')
  t.is(b0, 'b0')

  t.is(a1, 'b1')
  t.is(b1, 'b1')

  t.is(a2, 'a0')
  t.is(b2, 'a0')

  t.ok(await Autobase.isAutobase(a.local))
  t.ok(await Autobase.isAutobase(b.local))

  compareViews([a, b], t)

  for await (const block of a.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of b.local.createReadStream()) {
    t.ok(block.checkpoint === null)
  }

  async function applyWriter (batch, view, base) {
    for (const node of batch) {
      if (node.value.add) {
        await base.addWriter(b4a.from(node.value.add, 'hex'), { isIndexer: !!node.value.indexer })
        continue
      }

      if (view) await view.append(node.value)
    }
  }
})

test('basic - non-indexed writers 3-of-5', async t => {
  const { bases } = await create(5, t, { apply: applyWriter })
  const [a, b, c, d, e] = bases

  await a.append({ add: b.local.key.toString('hex'), indexer: true })
  await a.append({ add: c.local.key.toString('hex'), indexer: true })
  await a.append({ add: d.local.key.toString('hex'), indexer: false })
  await a.append({ add: e.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b, c, d, e])

  // confirm indexers
  await b.append(null)
  await c.append(null)

  await confirm([a, b, c, d, e])

  t.is(a.linearizer.indexers.length, 3)
  t.is(b.linearizer.indexers.length, 3)
  t.is(c.linearizer.indexers.length, 3)
  t.is(d.linearizer.indexers.length, 3)
  t.is(e.linearizer.indexers.length, 3)

  t.ok(d.writable)
  t.ok(e.writable)

  await e.append('e0')
  await replicateAndSync([d, e])

  await d.append('d0')
  await replicateAndSync([a, d])

  await a.append('a0') // later will only index to here
  await replicateAndSync([a, e])

  await e.append('e1')
  await replicateAndSync([d, e])

  await e.append('d1')
  await replicateAndSync([a, d, e])

  // e and d do not count
  t.is(a.view.indexedLength, 0)
  t.is(d.view.indexedLength, 0)
  t.is(e.view.indexedLength, 0)

  // confirm with only b and c
  await replicateAndSync([a, b, c])
  await b.append('b0')

  await replicateAndSync([b, c])
  await c.append('c0')

  // should only index up to a0
  t.is(c.view.indexedLength, 3)

  await replicateAndSync([a, b, c, d, e])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)
  t.is(d.view.indexedLength, 3)
  t.is(e.view.indexedLength, 3)

  const a0 = await a.view.get(0)
  const a1 = await a.view.get(1)
  const a2 = await a.view.get(2)

  t.is(a0, 'e0')
  t.is(a1, 'd0')
  t.is(a2, 'a0')

  compareViews([a, b, c, d, e], t)

  t.ok(await Autobase.isAutobase(a.local))
  t.ok(await Autobase.isAutobase(b.local))
  t.ok(await Autobase.isAutobase(c.local))
  t.ok(await Autobase.isAutobase(d.local))
  t.ok(await Autobase.isAutobase(e.local))

  for await (const block of a.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of b.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of c.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of d.local.createReadStream()) {
    t.ok(block.checkpoint === null)
  }

  for await (const block of e.local.createReadStream()) {
    t.ok(block.checkpoint === null)
  }

  async function applyWriter (batch, view, base) {
    for (const node of batch) {
      if (node.value.add) {
        await base.addWriter(b4a.from(node.value.add, 'hex'), { isIndexer: !!node.value.indexer })
        continue
      }

      if (view) await view.append(node.value)
    }
  }
})

test('autobase should not detach the original store', async t => {
  const store = new Corestore(ram)
  const bootstrap = b4a.alloc(32)

  const base = new Autobase(store, bootstrap)
  t.ok(store !== base.store) // New session with the original attached to it

  await base.close()
  t.ok(store.closed)
  t.ok(base.store.closed)
})

test('basic - oplog digest', async t => {
  const { bases } = await create(2, t, { open: null })
  const [base1, base2] = bases

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await replicateAndSync([base1, base2])
  await base2.append(null)

  await replicateAndSync([base1, base2])
  await base1.append(null)
  await replicateAndSync([base1, base2])

  const last = await base1.local.get(1)

  t.is(last.digest.pointer, 0)
  t.is(base2.system.core.getBackingCore().manifest.signers.length, 2)
  t.alike(last.digest.key, base2.system.core.key)
})

// todo: use normal helper once we have hypercore session manager
test('basic - close during apply', async t => {
  t.plan(1)

  const [store] = await createStores(1, t)
  const a = new Autobase(store, null, {
    async apply (nodes, view, base) {
      for (const node of nodes) {
        if (node.value.add) {
          await base.addWriter(b4a.from(node.value.add, 'hex'))
          continue
        }

        const core = view._source.core.session
        await core.get(core.length) // can never resolve
      }
    },
    open: store => store.get('test'),
    valueEncoding: 'json'
  })

  const promise = a.append('trigger')
  setImmediate(() => a.close())

  // should not throw when closing
  await t.execution(promise)
})

test('basic - constructor throws', async t => {
  await t.exception(create(1, t, { apply: undefined, open }), /Synthetic./)

  function open () {
    throw new Error('Synthetic.')
  }
})

test('basic - never sign past pending migration', async t => {
  const { bases } = await create(5, t)

  const [a, b, c, d, e] = bases

  let migrate = 0
  e.system.core.on('migrate', () => migrate++)

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e)

  await confirm(bases)

  await a.append('trigger')

  const session = a.system.core.getBackingCore().session
  const info = await a.system.getIndexedInfo(session.length)
  const signers = session.manifest.signers

  // we should only ever sign a view's core up
  // to the point when it is replaced by a migration,
  // otherwise others may ff to a minority signed index
  t.ok(info.indexers.length - signers.length <= 1)
})

test('basic - remove writer', async t => {
  const { bases } = await create(3, t, { apply: applyWithRemove, open: null })
  const [a, b, c] = bases

  await addWriter(a, b, false)

  await confirm([a, b, c])

  await addWriter(b, c, false)

  await confirm([a, b, c])

  t.is(b.system.members, 3)
  t.is(b.system.members, c.system.members)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })

  await confirm([a, b, c])

  t.is(b.system.members, 2)
  t.is(b.system.members, c.system.members)
})

test('basic - non-indexer writer removes themselves', async t => {
  const { bases } = await create(2, t, { apply: applyWithRemove, open: null })
  const [a, b] = bases

  await addWriter(a, b, false)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await b.append({ remove: b4a.toString(b.local.key, 'hex') })

  await t.exception(b.append('fail'), /Not writable/)

  await confirm([a, b])

  t.is(a.system.members, 1)
  t.is(a.system.members, a.system.members)
})

test('basic - remove indexer', async t => {
  const { bases } = await create(3, t, { apply: applyWithRemove, open: null })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await b.append(null)
  await replicateAndSync([a, b, c])

  await confirm([a, b, c])

  await addWriterAndSync(b, c)
  await c.append(null)
  await replicateAndSync([a, b, c])

  await confirm([a, b, c])

  t.is(b.system.members, 3)
  t.is(b.system.members, c.system.members)

  t.is(b.system.indexers.length, 3)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })
  await confirm([a, b, c])

  t.is(c.writable, false)

  const info = await a.system.get(c.local.key)

  t.is(info.isIndexer, false)
  t.is(info.isRemoved, true)

  t.is(b.system.members, 2)
  t.is(b.system.indexers.length, 2)
  t.is(b.system.members, c.system.members)
})

test('basic - remove indexer and continue indexing', async t => {
  const { bases } = await create(3, t, { apply: applyWithRemove })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(b, c)

  await c.append('c1')

  await confirm([a, b, c])

  t.is(b.view.getBackingCore().session.manifest.signers.length, 3)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })
  await confirm([a, b, c])

  t.is(c.writable, false)
  await t.exception(c.append('fail'), /Not writable/)

  const length = a.view.indexedLength

  await t.execution(b.append('hello'))
  await t.execution(confirm([a, b, c]))

  t.not(a.view.indexedLength, length)

  t.is(b.linearizer.indexers.length, 2)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)
})

test('basic - remove indexer back to previously used indexer set', async t => {
  const { bases } = await create(3, t, { apply: applyWithRemove })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)

  await b.append('b1')

  await confirm([a, b, c])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  await addWriterAndSync(b, c)

  await c.append('c1')

  await confirm([a, b, c])

  t.is(b.view.indexedLength, 2)

  const manifest1 = b.system.core.getBackingCore().session.manifest
  t.is(manifest1.signers.length, 3)
  t.is(b.system.indexers.length, 3)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })
  await confirm([a, b, c])

  t.is(c.writable, false)
  await t.exception(c.append('fail'), /Not writable/)

  await t.execution(b.append('hello'))
  await t.execution(confirm([a, b, c]))

  t.is(b.linearizer.indexers.length, 2)
  t.is(b.view.indexedLength, 3)
  t.is(c.view.indexedLength, 3)

  const manifest2 = b.system.core.getBackingCore().session.manifest
  t.is(manifest2.signers.length, 2)

  t.not(manifest1.prologue.length, manifest2.prologue.length)
  t.unlike(manifest1.prologue.hash, manifest2.prologue.hash)
})

test('basic - remove an indexer when 2-of-2', async t => {
  const { bases } = await create(2, t, { apply: applyWithRemove })
  const [a, b] = bases

  await addWriterAndSync(a, b)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  const manifest = b.system.core.getBackingCore().session.manifest

  t.is(manifest.signers.length, 2)
  t.is(b.system.indexers.length, 2)
  t.is(b.linearizer.indexers.length, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await confirm([a, b])

  t.is(b.writable, false)
  await t.exception(b.append('fail'), /Not writable/)

  await t.execution(a.append('hello'))
  await t.execution(confirm([a, b]))

  t.is(a.linearizer.indexers.length, 1)
  t.is(a.view.indexedLength, 2)

  t.is(b.linearizer.indexers.length, 1)
  t.is(b.view.indexedLength, 2)

  const finalManifest = b.system.core.getBackingCore().session.manifest

  t.is(finalManifest.signers.length, 1)
  t.not(finalManifest.prologue.length, 0)
})

test('basic - remove multiple indexers concurrently', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(b, c)

  await c.append('c1')

  await confirm([a, b, c])

  t.is(b.view.getBackingCore().session.manifest.signers.length, 3)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await a.append({ remove: b4a.toString(c.local.key, 'hex') })

  await confirm([a, b, c])

  t.is(b.writable, false)
  t.is(c.writable, false)

  await t.exception(b.append('fail'), /Not writable/)
  await t.exception(c.append('fail'), /Not writable/)

  const length = a.view.indexedLength
  await t.execution(a.append('hello'))

  t.not(a.view.indexedLength, length) // 1 indexer

  t.is(b.linearizer.indexers.length, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 1)

  async function apply (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        await base.addWriter(b4a.from(value.add, 'hex'))
        continue
      }

      if (value.remove) {
        await base.removeWriter(b4a.from(value.remove, 'hex'))
        continue
      }

      await view.append(value)
    }
  }
})

test('basic - indexer removes themselves', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(b, c)

  await a.append('a1')

  await confirm([a, b, c])

  t.is(b.view.getBackingCore().session.manifest.signers.length, 3)

  await a.append({ remove: b4a.toString(a.local.key, 'hex') })

  await confirm([a, b, c])

  t.is(a.writable, false)
  t.is(a.view.getBackingCore().session.manifest.signers.length, 2)

  await t.exception(a.append('fail'), /Not writable/)

  const length = a.view.length
  const indexedLength = a.view.indexedLength

  await t.execution(b.append('b1'))
  await t.execution(c.append('c1'))

  await replicateAndSync([a, b, c])

  t.not(a.view.length, length) // can still read

  await confirm([a, b, c])

  t.not(a.view.indexedLength, indexedLength) // b,c can still index

  async function apply (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        await base.addWriter(b4a.from(value.add, 'hex'))
        continue
      }

      if (value.remove) {
        await base.removeWriter(b4a.from(value.remove, 'hex'))
        continue
      }

      await view.append(value)
    }
  }
})

// todo: version 0 static ff guard

test('basic - all indexers removed', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await a.append('a1')

  await replicateAndSync([a, b])

  t.is(a.view.length, 1)
  t.is(b.view.length, 1)

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  t.is(a.view.getBackingCore().session.manifest.signers.length, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 1)

  await a.append({ remove: b4a.toString(a.local.key, 'hex') })

  await replicateAndSync([a, b])

  t.is(a.writable, false)

  t.is(a.view.getBackingCore().session.manifest.signers.length, 0)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 0)

  t.is(a.view.length, 1)
  t.is(b.view.length, 1)

  await t.exception(a.append('fail'), /Not writable/)
  await t.exception(b.append('fail'), /Not writable/)

  async function apply (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        await base.addWriter(b4a.from(value.add, 'hex'))
        continue
      }

      if (value.remove) {
        await base.removeWriter(b4a.from(value.remove, 'hex'))
        continue
      }

      await view.append(value)
    }
  }
})

test('basic - add new indexer after removing', async t => {
  const { bases } = await create(3, t, { apply: applyWithRemove })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await confirm([a, b])

  t.is(b.writable, false)
  t.is(b.localWriter, null)

  t.is(a.linearizer.indexers.length, 1)
  t.is(b.linearizer.indexers.length, 1)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 1)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 1)

  await t.execution(a.append('hello'))
  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 2)
  t.is(b.view.indexedLength, 2)

  await addWriterAndSync(a, c)

  await c.append('c1')

  b.debug = true
  t.is(b.writable, false)
  t.is(b.localWriter, null)

  await confirm([a, b, c])
  await replicateAndSync([a, b, c])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)
  t.is(c.view.indexedLength, 3)

  t.is(a.linearizer.indexers.length, 2)
  t.is(b.linearizer.indexers.length, 2)
  t.is(c.linearizer.indexers.length, 2)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 2)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 2)
  t.is(c.system.core.getBackingCore().session.manifest.signers.length, 2)
})

test('basic - readd removed indexer', async t => {
  const { bases } = await create(2, t, { apply: applyWithRemove })
  const [a, b] = bases

  await addWriterAndSync(a, b)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await confirm([a, b])

  t.is(b.writable, false)
  await t.exception(b.append('fail'), /Not writable/)

  t.is(a.linearizer.indexers.length, 1)
  t.is(b.linearizer.indexers.length, 1)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 1)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 1)

  await t.execution(a.append('hello'))
  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 2)
  t.is(b.view.indexedLength, 2)

  await addWriterAndSync(a, b)
  t.is(b.writable, true)

  await b.append('b1')

  await confirm([a, b])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)

  t.is(a.linearizer.indexers.length, 2)
  t.is(b.linearizer.indexers.length, 2)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 2)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 2)
})

async function applyWithRemove (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(b4a.from(value.add, 'hex'), { indexer: value.indexer !== false })
      continue
    }

    if (value.remove) {
      await base.removeWriter(b4a.from(value.remove, 'hex'))
      continue
    }

    await view.append(value)
  }
}
