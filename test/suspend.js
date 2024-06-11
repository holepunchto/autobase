const test = require('brittle')
const tmpDir = require('test-tmp')
const c = require('compact-encoding')
const b4a = require('b4a')

const { BootRecord } = require('../lib/messages')

const {
  create,
  createBase,
  createStores,
  replicate,
  replicateAndSync,
  addWriter,
  addWriterAndSync,
  confirm,
  eventFlush
} = require('./helpers')

test('suspend - pass exisiting store', async t => {
  const { stores, bases } = await create(2, t)

  const [base1, base2] = bases

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await base1.append({
    value: 'base1'
  })

  await confirm([base1, base2])

  await base2.append({
    value: 'base2'
  })

  await confirm([base1, base2])

  t.is(base2.activeWriters.size, 2)

  await base2.close()

  const base3 = createBase(stores[1], base1.local.key, t)
  await base3.ready()

  t.is(base3.activeWriters.size, 2)

  await base3.append('final')

  await t.execution(replicateAndSync([base3, base1]))
})

test('suspend - pass exisiting fs store', async t => {
  const { bases } = await create(1, t, { open: null })
  const [base1] = bases

  const [store] = await createStores(1, t, {
    offset: 1,
    storage: () => tmpDir(t)
  })

  const base2 = createBase(store, base1.local.key, t, { open: null })
  await base2.ready()

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await base1.append({
    value: 'base1'
  })

  await confirm([base1, base2])

  await base2.append({
    value: 'base2'
  })

  await confirm([base1, base2])

  t.is(base2.activeWriters.size, 2)

  await base2.close()

  const base3 = createBase(store, base1.local.key, t, { open: null })
  await base3.ready()

  t.is(base3.activeWriters.size, 2)

  await base3.append('final')

  await t.execution(replicateAndSync([base3, base1]))
})

test('suspend - 2 exisiting fs stores', async t => {
  const { bases, stores } = await create(2, t, { storage: () => tmpDir(t) })

  const [base1, base2] = bases

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await base1.append({
    value: 'base1'
  })

  await confirm([base1, base2])

  await base2.append({
    value: 'base2'
  })

  await confirm([base1, base2])

  t.is(base2.activeWriters.size, 2)

  await base2.close()

  const base3 = createBase(stores[1], base1.local.key, t)
  await base3.ready()

  t.is(base3.activeWriters.size, 2)

  await base3.append('final')

  await t.execution(replicateAndSync([base3, base1]))
})

test('suspend - reopen after index', async t => {
  const { bases, stores } = await create(2, t, { storage: () => tmpDir(t) })

  const [a, b] = bases

  await addWriter(a, b)

  await replicateAndSync([a, b])

  await a.append('a0')

  await confirm([a, b])

  await b.append('b0')
  await a.append('a1')

  t.is(b.activeWriters.size, 2)
  t.is(b.view.length, 2)

  const order = []
  for (let i = 0; i < b.view.length; i++) {
    order.push(await b.view.get(i))
  }

  await b.close()

  const b2 = createBase(stores[1], a.local.key, t)
  await b2.ready()

  await b2.update()

  t.is(b2.view.length, order.length)

  for (let i = 0; i < b2.view.length; i++) {
    t.alike(await b2.view.get(i), order[i])
  }

  t.is(b2.activeWriters.size, 2)

  await b2.append('final')

  await t.execution(replicateAndSync([a, b2]))

  t.is(b.view.indexedLength, 1)
  t.is(b2.view.indexedLength, 1)
  t.is(b2.view.length, b.view.length + 2)
})

test('suspend - reopen with sync in middle', async t => {
  const { bases, stores } = await create(2, t, { storage: () => tmpDir(t) })

  const [a, b] = bases

  await addWriter(a, b)

  await replicateAndSync([a, b])

  await a.append('a0')

  await confirm([a, b])

  await b.append('b0')
  await a.append('a1')

  t.is(b.activeWriters.size, 2)
  t.is(b.view.length, 2)

  await b.close()

  const bstore = stores[1]
  const unreplicate = replicate([a.store, bstore])

  await a.update()

  // sync current views
  for (const { key } of [b.system.core, b.view]) {
    if (!key) continue

    const core = bstore.get({ key, compat: false })
    const remote = a.store.get({ key, compat: false })

    await core.ready()
    await remote.ready()
    await core.download({ start: 0, end: remote.length }).done()
  }

  // sync next views
  for (const ac of [a.system.core, a.view]) {
    const remote = ac.getBackingCore().session
    const local = bstore.get({ key: remote.key, compat: false })
    await local.ready()
    await local.download({ start: 0, end: remote.length }).done()
  }

  // sync writers
  for (const core of [a.local]) {
    const remote = bstore.get({ key: core.key, compat: false })
    await remote.ready()
    await remote.download({ start: 0, end: core.length }).done()
  }

  await unreplicate()

  const b2 = createBase(bstore, a.local.key, t)
  await b2.ready()

  await b2.update()

  t.is(b2.activeWriters.size, 2)
  t.is(b2.view.length, b.view.length + 1)

  await b2.append('final')

  await t.execution(replicateAndSync([b2, a]))

  t.is(b.view.indexedLength, 1)
  t.is(b2.view.indexedLength, 1)
  t.is(b2.view.length, b.view.length + 2)
})

test('suspend - reopen with indexing in middle', async t => {
  const { bases, stores } = await create(3, t, { storage: () => tmpDir(t) })

  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm([a, b, c])

  t.is(c.activeWriters.size, 3)
  t.is(c.view.length, 0)

  await c.append('c0')

  const order = []
  for (let i = 0; i < c.view.length; i++) {
    order.push(await c.view.get(i))
  }

  await c.close()

  // majority continues

  await a.append('a0')
  await replicateAndSync([a, b])
  await b.append('b0')
  await replicateAndSync([a, b])
  await a.append('a1')

  await confirm([a, b])

  const c2 = createBase(stores[2], a.local.key, t)
  await c2.ready()

  await c2.update()

  t.is(c2.view.length, order.length)

  for (let i = 0; i < c2.view.length; i++) {
    t.alike(await c2.view.get(i), order[i])
  }

  t.is(c2.activeWriters.size, 3)
  t.is(c2.view.indexedLength, 0)

  await c2.append('final')

  await t.execution(replicateAndSync([c2, b]))

  t.is(b.view.indexedLength, 3)
  t.is(c2.view.indexedLength, 3)
  t.is(c2.view.length, 5)
})

test.skip('suspend - reopen with indexing + sync in middle', async t => {
  const { bases, stores } = await create(2, t, { storage: () => tmpDir(t) })

  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm([a, b, c])

  t.is(c.activeWriters.size, 3)
  t.is(c.view.length, 0)

  await c.append('c0')

  const order = []
  for (let i = 0; i < c.view.length; i++) {
    order.push(await c.view.get(i))
  }

  await c.close()
  const cstore = stores[2]

  // majority continues

  await a.append('a0')
  await replicateAndSync([a, b])
  await b.append('b0')
  await replicateAndSync([a, b])
  await a.append('a1')

  await confirm([a, b])

  const unreplicate = replicate([a.store, cstore])

  await a.update()

  // sync current views
  for (const { key } of [c.system.core, c.view]) {
    if (!key) continue

    const core = cstore.get({ key, compat: false })
    const remote = a.store.get({ key, compat: false })

    await core.ready()
    await remote.ready()
    await core.download({ start: 0, end: remote.length }).done()
  }

  // sync next views
  for (const ac of [a.system.core, a.view]) {
    const remote = ac.getBackingCore().session
    const local = cstore.get({ key: remote.key, compat: false })
    await local.ready()
    await local.download({ start: 0, end: remote.length }).done()
  }

  // sync writers
  for (const core of [a.local, a.store.get({ key: b.local.key, compat: false })]) {
    await core.ready()
    const remote = cstore.get({ key: core.key, compat: false })
    await remote.ready()
    await remote.download({ start: 0, end: core.length }).done()
  }

  await unreplicate()

  const c2 = createBase(cstore, a.local.key, t)
  await c2.ready()

  t.is(c2.view.length, order.length)

  for (let i = 0; i < c2.view.length; i++) {
    t.alike(await c2.view.get(i), order[i])
  }

  t.is(c2.activeWriters.size, 3)
  t.is(c2.view.length, order.length)
  t.is(c2.view.indexedLength, 0)

  await c2.append('final')

  await t.execution(replicateAndSync([c2, b]))

  t.is(b.view.indexedLength, 3)
  t.is(c2.view.indexedLength, 3)
  t.is(c2.view.length, 5)
})

test('suspend - non-indexed writer', async t => {
  const { bases, stores } = await create(2, t, {
    apply: applyWriter,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  await b.view.ready()

  await a.append({ add: b.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b])

  await b.append('b0')
  await b.append('b1')

  await replicateAndSync([a, b])

  await a.append('a0')

  await confirm([a, b])

  const unreplicate = replicate([a.store, stores[1]])

  await eventFlush()
  await a.update()
  await b.update()

  await unreplicate()

  await b.close()

  const b2 = createBase(stores[1], a.local.key, t)
  await b2.ready()

  t.is(b2.view.indexedLength, a.view.indexedLength)
  t.is(b2.view.length, a.view.length)

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

test('suspend - open new index after reopen', async t => {
  const { bases, stores } = await create(2, t, {
    apply: applyMultiple,
    open: openMultiple,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  await addWriterAndSync(a, b)

  await replicateAndSync([a, b])

  await a.append({ index: 1, data: 'a0' })

  await confirm([a, b])

  await b.append({ index: 2, data: 'b0' })
  await a.append({ index: 1, data: 'a1' })

  t.is(b.activeWriters.size, 2)

  const order = []
  for (let i = 0; i < b.view.first.length; i++) {
    order.push(await b.view.first.get(i))
  }

  for (let i = 0; i < b.view.second.length; i++) {
    order.push(await b.view.second.get(i))
  }

  await b.close()

  const b2 = createBase(stores[1], a.local.key, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  await b2.ready()

  for (let i = 0; i < b2.view.first.length; i++) {
    t.alike(await b2.view.first.get(i), order[i])
  }

  for (let i = 0; i < b2.view.second.length; i++) {
    t.alike(await b2.view.second.get(i), order[i + b2.view.first.length])
  }

  t.is(b2.activeWriters.size, 2)

  await b2.append({ index: 1, data: 'final' })

  t.is(b2.view.first.length + b2.view.second.length, order.length + 1)

  await t.execution(replicateAndSync([a, b2]))

  t.is(b.view.first.indexedLength, 1)
  t.is(b2.view.first.indexedLength, 1)
  t.is(b2.view.first.length, b.view.first.length + 2)

  await t.execution(confirm([a, b2]))

  const an = await a.local.get(a.local.length - 1)
  const bn = await b2.local.get(b2.local.length - 1)

  t.is(an.checkpoint.length, 3)
  t.is(bn.checkpoint.length, 3)

  const acp1 = await a.localWriter.getCheckpoint(1)
  // const acp2 = await a.localWriter.getCheckpoint(2)

  const bcp1 = await b2.localWriter.getCheckpoint(1)
  // const bcp2 = await b2.localWriter.getCheckpoint(2)

  t.is(acp1.length, 3)
  // t.is(acp2.length, 1)

  t.alike(acp1.length, bcp1.length)
  // t.alike(acp2.length, bcp2.length)

  t.alike(acp1, await a.view.first._source._checkpoint())
  // t.alike(acp2, await a.view.second._source._checkpoint())
})

test('suspend - reopen multiple indexes', async t => {
  const { bases, stores } = await create(2, t, {
    apply: applyMultiple,
    open: openMultiple,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  await addWriterAndSync(a, b)

  await replicateAndSync([a, b])

  await a.append({ index: 1, data: 'a0' })
  await a.append({ index: 2, data: 'a1' })

  await confirm([a, b])

  await b.append({ index: 2, data: 'b0' })
  await b.append({ index: 1, data: 'b1' })

  await a.append({ index: 1, data: 'a2' })

  t.is(b.activeWriters.size, 2)

  const order = []
  for (let i = 0; i < b.view.first.length; i++) {
    order.push(await b.view.first.get(i))
  }

  for (let i = 0; i < b.view.second.length; i++) {
    order.push(await b.view.second.get(i))
  }

  await b.close()

  const b2 = createBase(stores[1], a.local.key, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  await b2.ready()

  await b2.update()

  for (let i = 0; i < b2.view.first.length; i++) {
    t.alike(await b2.view.first.get(i), order[i])
  }

  for (let i = 0; i < b2.view.second.length; i++) {
    t.alike(await b2.view.second.get(i), order[i + b2.view.first.length])
  }

  t.is(b2.activeWriters.size, 2)

  await b2.append({ index: 1, data: 'final' })

  t.is(b2.view.first.length + b2.view.second.length, order.length + 1)

  await t.execution(replicateAndSync([a, b2]))

  t.is(b.view.first.indexedLength, 1)
  t.is(b2.view.first.indexedLength, 1)
  t.is(b2.view.first.length, b.view.first.length + 2)

  await t.execution(confirm([a, b2]))

  const an = await a.local.get(a.local.length - 1)
  const b2n = await b2.local.get(b2.local.length - 1)

  t.is(an.checkpoint.length, 3)
  t.is(b2n.checkpoint.length, 3)

  const acp1 = await a.localWriter.getCheckpoint(1)
  const acp2 = await a.localWriter.getCheckpoint(2)

  const b2cp1 = await b2.localWriter.getCheckpoint(1)
  const b2cp2 = await b2.localWriter.getCheckpoint(2)

  t.alike(acp1.length, 4)
  t.alike(acp2.length, 2)

  t.alike(acp1.length, b2cp1.length)
  t.alike(acp2.length, b2cp2.length)

  t.alike(acp1, await a.view.first._source._checkpoint())
  t.alike(acp2, await a.view.second._source._checkpoint())
})

test('restart non writer', async t => {
  const [storeA, storeB] = await createStores(2, t)

  const base = createBase(storeA, null, t)
  await base.append({ hello: 'world' })

  const other = createBase(storeB, base.key, t)

  await other.ready()

  await replicateAndSync([base, other])

  await other.close()
  await base.close()

  const other2 = createBase(storeB, base.key, t)
  await t.execution(other2.ready(), 'should be able to start')
  await other2.close()
})

test('suspend - non-indexed writer catches up', async t => {
  const { bases, stores } = await create(2, t, {
    apply: applyWriter,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  await b.view.ready()

  await a.append({ add: b.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b])

  await b.append('b0')
  await b.append('b1')

  await replicateAndSync([a, b])

  await a.append('a0')

  await confirm([a, b])

  const unreplicate = replicate([a.store, stores[1]])

  await eventFlush()
  await a.update()
  await b.update()

  for (let i = 0; i < 999; i++) a.append('a')
  await a.append('final')

  await unreplicate()

  await b.close()

  await t.execution(createBase(stores[1], a.local.key, t))

  t.pass('did not fail on open')

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

test.skip('suspend - append but not indexed then reopen', async t => {
  const { bases, stores } = await create(3, t, {
    apply: applyMultiple,
    open: openMultiple,
    storage: () => tmpDir(t)
  })

  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm([a, b, c])

  await a.append({ index: 1, data: 'a0' })
  await confirm([a, b])

  await b.append({ index: 2, data: 'b0' })
  await confirm([a, b])

  t.is(a._viewStore.getByKey(a.system.views[1].key).name, 'first')
  t.is(a._viewStore.getByKey(a.system.views[2].key).name, 'second')

  await c.append({ index: 2, data: 'c0' })

  // c hasn't seen any appends to first
  t.is(c._viewStore.getByKey(c.system.views[1].key).name, 'second')

  t.is(b.activeWriters.size, 3)
  t.is(c.activeWriters.size, 3)

  await c.close()

  const c2 = createBase(stores[2], a.local.key, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  await c2.ready()

  // c hasn't seen any appends to first
  // t.is(c2._viewStore.getByKey(c2.system.views[1].key).name, 'second')

  t.absent(await c2.localWriter.getCheckpoint(1))
  t.absent(await c2.localWriter.getCheckpoint(2))

  await confirm([a, b, c2])

  // t.alike((await c2.localWriter.getCheckpoint(1)).length, (await a.localWriter.getCheckpoint(1)).length)
  // t.alike((await c2.localWriter.getCheckpoint(2)).length, (await a.localWriter.getCheckpoint(2)).length)

  await c2.append({ index: 2, data: 'c1' })

  // t.is(c2._viewStore.getByKey(c2.system.views[1].key).name, 'first')
  // t.is(c2._viewStore.getByKey(c2.system.views[2].key).name, 'second')

  await c2.append({ index: 1, data: 'final' })

  await t.execution(confirm([a, c2]))

  const an = await a.local.get(a.local.length - 1)
  const c2n = await c2.local.get(c2.local.length - 1)

  t.is(an.checkpoint.length, 3)
  t.is(c2n.checkpoint.length, 3)

  const acp1 = await a.localWriter.getCheckpoint(1)
  // const acp2 = await a.localWriter.getCheckpoint(2)

  const c2cp1 = await c2.localWriter.getCheckpoint(1)
  // const c2cp2 = await c2.localWriter.getCheckpoint(2)

  t.alike(acp1.length, 2)
  // t.alike(acp2.length, 3)

  t.alike(acp1.length, c2cp1.length)
  // t.alike(acp2.length, c2cp2.length)

  t.alike(acp1, await a.view.first._source._checkpoint())
  // t.alike(acp2, await a.view.second._source._checkpoint())
})

test('suspend - migrations', async t => {
  const { bases, stores } = await create(3, t, { storage: () => tmpDir(t) })

  const [a, b] = bases

  await a.append('a0')
  await a.append('a1')

  t.is(a.view.indexedLength, 2)
  t.is(a.view.signedLength, 2)

  await b.ready()

  await addWriter(a, b)

  await replicateAndSync([a, b])

  await b.append('b0')
  await confirm([a, b])

  await a.append('a1')
  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 3)
  t.is(a.view.signedLength, 3)

  t.is(b.activeWriters.size, 2)
  t.is(b.view.indexedLength, 3)
  t.is(b.view.signedLength, 3)

  const order = []
  for (let i = 0; i < b.view.length; i++) {
    order.push(await b.view.get(i))
  }

  await b.close()

  const b2 = createBase(stores[1], a.local.key, t)
  await b2.ready()

  t.is(b2.view.indexedLength, 3)
  t.is(b2.view.signedLength, 3)
  t.is(b2.view.getBackingCore().indexedLength, 3)

  await b2.update()

  t.is(b2.view.length, order.length)
  for (let i = 0; i < b2.view.length; i++) {
    t.alike(await b2.view.get(i), order[i])
  }

  t.is(b2.activeWriters.size, 2)

  await b2.append('final') // this indexes a1 (all indexer ack)

  await t.execution(replicateAndSync([a, b2]))

  t.is(b.view.indexedLength, 3)
  t.is(b2.view.indexedLength, 4)
  t.is(b2.view.length, b.view.length + 1)
})

test('suspend - append waits for drain after boot', async t => {
  const { bases, stores } = await create(3, t, {
    apply: applyMultiple,
    open: openMultiple,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  await addWriterAndSync(a, b)

  await replicateAndSync([a, b])

  for (let i = 0; i < 100; i++) await b.append({ tick: true })

  await b.close()

  const b2 = createBase(stores[1], a.local.key, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  await b2.ready()

  await b2.append({ last: true })

  const { node } = await b2.localWriter.core.get(b2.localWriter.core.length - 1)
  t.is(node.heads.length, 1)
  t.is(node.heads[0].length, 101) // links the last node
})

test('suspend - incomplete migrate', async t => {
  const { bases, stores } = await create(3, t, { storage: () => tmpDir(t) })

  const [a, b] = bases

  await a.append('a0')
  await a.append('a1')

  t.is(a.view.indexedLength, 2)
  t.is(a.view.signedLength, 2)

  await addWriter(a, b)

  await replicateAndSync([a, b])

  await b.append('b0')

  await replicateAndSync([a, b])

  await a.append('a1') // this indexes b0
  await replicateAndSync([a, b])

  await b.append('b1') // this indexes a1

  t.is(b.view.indexedLength, 4)
  t.is(b.view.signedLength, 2)

  await b.close()

  const b2 = createBase(stores[1], a.local.key, t)

  await b2.ready()
  await b2.update() // needed cause we arent persisting the tip

  t.is(a.view.indexedLength, 3)
  t.is(a.view.signedLength, 2)
  t.is(a.view.getBackingCore().indexedLength, 2)

  t.is(b2.view.indexedLength, 4)
  t.is(b2.view.signedLength, 2)
  t.is(b2.view.getBackingCore().indexedLength, 2)

  await b2.update()

  t.is(b2.activeWriters.size, 2)

  await t.execution(replicateAndSync([a, b2]))
})

test('suspend - recover from bad sys core', async t => {
  const { bases, stores } = await create(2, t, { storage: () => tmpDir(t) })

  const [a, b] = bases

  await a.append('a0')
  await a.append('a1')
  await a.append('a2')
  await a.append('a3')

  await replicateAndSync([a, b])

  const len = b.system.core.length

  await b.close()

  const raw = await b.local.getUserData('autobase/boot')
  const record = c.decode(BootRecord, raw)

  const core = stores[1].get(record.indexed.key)
  await core.ready()

  for (let i = 6; i < record.indexed.length; i++) {
    core.core.bitfield.set(i, false)
  }

  const b1 = createBase(stores[1], null, t)
  await b1.ready()

  t.not(b1.system.core.length, len)
  t.is(b1.system.core.length, 0)

  await replicateAndSync([a, b1])

  t.is(b1.system.core.length, len)
})

test('suspend - restart with unindexed local nodes', async t => {
  const { bases, stores } = await create(3, t, { storage: () => tmpDir(t) })

  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await replicateAndSync([a, b, c])

  await addWriterAndSync(a, c, false)
  await confirm([a, b, c])

  // bigger than autobase max batch size
  for (let i = 0; i < 100; i++) await b.append('b' + i)

  await replicateAndSync([b, c])

  await c.close()

  const c1 = createBase(stores[2], null, t)
  c1.debug = true

  await c1.ready()

  await c1.append('c0')

  await replicateAndSync([a, c1])

  const exp = { key: b.local.key, length: b.local.length }

  const last = await c1.local.get(0)
  t.alike(last.node.heads, [exp])

  t.is(await a.view.get(a.view.length - 1), 'c0')
})

function openMultiple (store) {
  return {
    first: store.get('first', { valueEncoding: 'json' }),
    second: store.get('second', { valueEncoding: 'json' })
  }
}

async function applyMultiple (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'))
      continue
    }

    if (value.index === 1) {
      await view.first.append(value.data)
    } else if (value.index === 2) {
      await view.second.append(value.data)
    }
  }
}
