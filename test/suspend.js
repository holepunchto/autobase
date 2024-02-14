const test = require('brittle')
const ram = require('random-access-memory')
const tmpDir = require('test-tmp')
const Corestore = require('corestore')
const b4a = require('b4a')

const Autobase = require('..')

const {
  create,
  replicate,
  replicateAndSync,
  apply,
  addWriter,
  addWriterAndSync,
  confirm,
  eventFlush
} = require('./helpers')

test('suspend - pass exisiting store', async t => {
  const [base1] = await create(1, apply)

  const store = new Corestore(ram.reusable(), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session2 = store.session()
  const base2 = new Autobase(session2, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
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

  const session3 = store.session()
  const base3 = new Autobase(session3, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, fastForward: false })
  await base3.ready()

  t.is(base3.activeWriters.size, 2)

  await base3.append('final')

  await t.execution(replicateAndSync([base3, base1]))
})

test('suspend - pass exisiting fs store', async t => {
  const [base1] = await create(1, apply)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session2 = store.session()
  const base2 = new Autobase(session2, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, fastForward: false })
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

  const session3 = store.session()
  const base3 = new Autobase(session3, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, fastForward: false })
  await base3.ready()

  t.teardown(() => Promise.all([
    base1.close(),
    base3.close()
  ]))

  t.is(base3.activeWriters.size, 2)

  await base3.append('final')

  await t.execution(replicateAndSync([base3, base1]))
})

test('suspend - 2 exisiting fs stores', async t => {
  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(0)
  })

  const base1 = new Autobase(store, null, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, fastForward: false })
  await base1.ready()

  const store2 = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session2 = store2.session()
  const base2 = new Autobase(session2, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, fastForward: false })
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

  const session3 = store2.session()
  const base3 = new Autobase(session3, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, fastForward: false })
  await base3.ready()

  t.is(base3.activeWriters.size, 2)

  await base3.append('final')

  await t.execution(replicateAndSync([base3, base1]))

  await base1.close()
  await base2.close()
  await base3.close()
})

test('suspend - reopen after index', async t => {
  const [a] = await create(1, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()

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

  const session2 = store.session()
  const c = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await c.ready()
  await c.update()

  t.is(c.view.length, order.length)

  for (let i = 0; i < c.view.length; i++) {
    t.alike(await c.view.get(i), order[i])
  }

  t.is(c.activeWriters.size, 2)

  await c.append('final')

  await t.execution(replicateAndSync([a, c]))

  t.is(b.view.indexedLength, 1)
  t.is(c.view.indexedLength, 1)
  t.is(c.view.length, b.view.length + 2)

  await a.close()
  await b.close()
  await c.close()
})

test('suspend - reopen with sync in middle', async t => {
  const [a] = await create(1, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()

  await addWriter(a, b)

  await replicateAndSync([a, b])

  await a.append('a0')

  await confirm([a, b])

  await b.append('b0')
  await a.append('a1')

  t.is(b.activeWriters.size, 2)
  t.is(b.view.length, 2)

  await b.close()

  const unreplicate = replicate([a.store, store])

  await a.update()

  // sync current views
  for (const { key } of [b.system.core, b.view]) {
    if (!key) continue

    const core = store.get({ key, compat: false })
    const remote = a.store.get({ key, compat: false })

    await core.ready()
    await remote.ready()
    await core.download({ start: 0, end: remote.length }).done()
  }

  // sync next views
  for (const ac of [a.system.core, a.view]) {
    const remote = ac.getBackingCore().session
    const local = store.get({ key: remote.key, compat: false })
    await local.ready()
    await local.download({ start: 0, end: remote.length }).done()
  }

  // sync writers
  for (const core of [a.local]) {
    const remote = store.get({ key: core.key, compat: false })
    await remote.ready()
    await remote.download({ start: 0, end: core.length }).done()
  }

  await unreplicate()

  const session2 = store.session()
  await session2.ready()

  const b2 = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b2.ready()
  await b2.update()

  t.is(b2.activeWriters.size, 2)
  t.is(b2.view.length, b.view.length + 1)

  await b2.append('final')

  await t.execution(replicateAndSync([b2, a]))

  t.is(b.view.indexedLength, 1)
  t.is(b2.view.indexedLength, 1)
  t.is(b2.view.length, b.view.length + 2)

  await a.close()
  await b.close()
  await b2.close()
})

test('suspend - reopen with indexing in middle', async t => {
  const [a, b] = await create(2, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(2)
  })

  const session1 = store.session()
  const c = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await c.ready()

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

  const session2 = store.session()
  await session2.ready()

  const c2 = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

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

  await a.close()
  await b.close()
  await c2.close()
})

test.skip('suspend - reopen with indexing + sync in middle', async t => {
  const [a, b] = await create(2, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(2)
  })

  const session1 = store.session()
  const c = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await c.ready()

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

  const unreplicate = replicate([a.store, store])

  await a.update()

  // sync current views
  for (const { key } of [c.system.core, c.view]) {
    if (!key) continue

    const core = store.get({ key, compat: false })
    const remote = a.store.get({ key, compat: false })

    await core.ready()
    await remote.ready()
    await core.download({ start: 0, end: remote.length }).done()
  }

  // sync next views
  for (const ac of [a.system.core, a.view]) {
    const remote = ac.getBackingCore().session
    const local = store.get({ key: remote.key, compat: false })
    await local.ready()
    await local.download({ start: 0, end: remote.length }).done()
  }

  // sync writers
  for (const core of [a.local, a.store.get({ key: b.local.key, compat: false })]) {
    await core.ready()
    const remote = store.get({ key: core.key, compat: false })
    await remote.ready()
    await remote.download({ start: 0, end: core.length }).done()
  }

  await unreplicate()

  const session2 = store.session()
  await session2.ready()

  const c2 = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

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

  await a.close()
  await b.close()
  await c2.close()
})

test('suspend - non-indexed writer', async t => {
  const [a] = await create(1, applyWriter, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: b4a.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply: applyWriter,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()
  await b.view.ready()

  await a.append({ add: b.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b])

  await b.append('b0')
  await b.append('b1')

  await replicateAndSync([a, b])

  await a.append('a0')

  await confirm([a, b])

  const unreplicate = replicate([a.store, store])

  await eventFlush()
  await a.update()
  await b.update()

  await unreplicate()

  await b.close()

  const session2 = store.session()
  await session2.ready()

  const c = new Autobase(session2, a.local.key, {
    apply: applyWriter,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  c.debug = true

  await c.ready()

  t.is(c.view.indexedLength, a.view.indexedLength)
  t.is(c.view.length, a.view.length)

  await a.close()
  await c.close()

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
  const [a] = await create(1, applyMultiple, openMultiple)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()

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

  const session2 = store.session()
  const b2 = new Autobase(session2, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
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

  await a.close()
  await b2.close()
})

test('suspend - reopen multiple indexes', async t => {
  const [a] = await create(1, applyMultiple, openMultiple)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()

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

  const session2 = store.session()
  const c = new Autobase(session2, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await c.ready()
  await c.update()

  for (let i = 0; i < c.view.first.length; i++) {
    t.alike(await c.view.first.get(i), order[i])
  }

  for (let i = 0; i < c.view.second.length; i++) {
    t.alike(await c.view.second.get(i), order[i + c.view.first.length])
  }

  t.is(c.activeWriters.size, 2)

  await c.append({ index: 1, data: 'final' })

  t.is(c.view.first.length + c.view.second.length, order.length + 1)

  await t.execution(replicateAndSync([a, c]))

  t.is(b.view.first.indexedLength, 1)
  t.is(c.view.first.indexedLength, 1)
  t.is(c.view.first.length, b.view.first.length + 2)

  await t.execution(confirm([a, c]))

  const an = await a.local.get(a.local.length - 1)
  const cn = await c.local.get(c.local.length - 1)

  t.is(an.checkpoint.length, 3)
  t.is(cn.checkpoint.length, 3)

  const acp1 = await a.localWriter.getCheckpoint(1)
  const acp2 = await a.localWriter.getCheckpoint(2)

  const ccp1 = await c.localWriter.getCheckpoint(1)
  const ccp2 = await c.localWriter.getCheckpoint(2)

  t.alike(acp1.length, 4)
  t.alike(acp2.length, 2)

  t.alike(acp1.length, ccp1.length)
  t.alike(acp2.length, ccp2.length)

  t.alike(acp1, await a.view.first._source._checkpoint())
  t.alike(acp2, await a.view.second._source._checkpoint())

  await a.close()
  await b.close()
  await c.close()
})

test('restart non writer', async t => {
  const storeA = new Corestore(ram.reusable())
  const storeB = new Corestore(ram.reusable())

  const base = new Autobase(storeA, { apply, valueEncoding: 'json', fastForward: false })
  await base.append({ hello: 'world' })

  const other = new Autobase(storeB.session(), base.key, { apply, valueEncoding: 'json' })

  await other.ready()

  await replicateAndSync([base, other])

  await other.close()
  await base.close()

  const other2 = new Autobase(storeB.session(), base.key, { apply, valueEncoding: 'json' })
  await t.execution(other2.ready(), 'should be able to start')
  await other2.close()
})

test('suspend - non-indexed writer catches up', async t => {
  const [a] = await create(1, applyWriter, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: b4a.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply: applyWriter,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()
  await b.view.ready()

  await a.append({ add: b.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b])

  await b.append('b0')
  await b.append('b1')

  await replicateAndSync([a, b])

  await a.append('a0')

  await confirm([a, b])

  const unreplicate = replicate([a.store, store])

  await eventFlush()
  await a.update()
  await b.update()

  for (let i = 0; i < 999; i++) a.append('a')
  await a.append('final')

  await unreplicate()

  await b.close()

  const session2 = store.session()
  await session2.ready()

  const c = new Autobase(session2, a.local.key, {
    applyWriter,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await c.ready()

  t.pass('did not fail on open')

  await a.close()
  await c.close()

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
  const [a, b] = await create(2, applyMultiple, openMultiple)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(2)
  })

  const session1 = store.session()
  const c = new Autobase(session1, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()
  await c.ready()

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

  const session2 = store.session()
  const c2 = new Autobase(session2, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
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

  await a.close()
  await b.close()
  await c2.close()
})

test('suspend - migrations', async t => {
  const [a] = await create(1, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  await a.append('a0')
  await a.append('a1')

  t.is(a.view.indexedLength, 2)
  t.is(a.view.signedLength, 2)

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

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

  const session2 = store.session()
  const c = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await c.ready()

  t.is(c.view.indexedLength, 3)
  t.is(c.view.signedLength, 3)
  t.is(c.view.getBackingCore().indexedLength, 3)

  await c.update()

  t.is(c.view.length, order.length)
  for (let i = 0; i < c.view.length; i++) {
    t.alike(await c.view.get(i), order[i])
  }

  t.is(c.activeWriters.size, 2)

  await c.append('final')

  await t.execution(replicateAndSync([a, c]))

  t.is(b.view.indexedLength, 3)
  t.is(c.view.indexedLength, 3)
  t.is(c.view.length, b.view.length + 1)

  await a.close()
  await b.close()
  await c.close()
})

test('suspend - append waits for drain after boot', async t => {
  const [a] = await create(1, applyMultiple, openMultiple)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const b = new Autobase(store.session(), a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()

  await addWriterAndSync(a, b)

  await replicateAndSync([a, b])

  for (let i = 0; i < 100; i++) await b.append({ tick: true })

  await b.close()

  const b2 = new Autobase(store.session(), a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b2.append({ last: true })

  const { node } = await b2.localWriter.core.get(b2.localWriter.core.length - 1)
  t.is(node.heads.length, 1)
  t.is(node.heads[0].length, 101) // links the last node

  await store.close()
})

test('suspend - incomplete migrate', async t => {
  const [a] = await create(1, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  await a.append('a0')
  await a.append('a1')

  t.is(a.view.indexedLength, 2)
  t.is(a.view.signedLength, 2)

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b.ready()

  await addWriter(a, b)

  await replicateAndSync([a, b])

  await b.append('b0')

  await replicateAndSync([a, b])

  await a.append('a1')
  await replicateAndSync([a, b])

  await b.append('b1')

  t.is(b.view.indexedLength, 3)
  t.is(b.view.signedLength, 2)

  await b.close()

  const session2 = store.session()
  const b2 = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: false
  })

  await b2.ready()

  t.is(a.view.indexedLength, 2)
  t.is(a.view.signedLength, 2)
  t.is(a.view.getBackingCore().indexedLength, 2)

  t.is(b2.view.indexedLength, 3)
  t.is(b2.view.signedLength, 2)
  t.is(b2.view.getBackingCore().indexedLength, 2)

  await b2.update()

  t.is(b2.activeWriters.size, 2)

  await t.execution(replicateAndSync([a, b2]))

  await a.close()
  await b2.close()
})

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

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
