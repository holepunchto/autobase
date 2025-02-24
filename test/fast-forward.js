const { on } = require('events')
const test = require('brittle')
const tmpDir = require('test-tmp')
const b4a = require('b4a')

const {
  addWriter,
  addWriterAndSync,
  replicateAndSync,
  sync,
  replicate,
  eventFlush,
  confirm,
  create,
  createStores,
  createBase
} = require('./helpers')

test('fast-forward - simple', async t => {
  t.plan(1)

  const { bases } = await create(2, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  for (let i = 0; i < 1000; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])

  const sparse = await isSparse(b.view)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / b.view.length * 100).toFixed(2) + '%')
})

test('fast-forward - migrate', async t => {
  t.plan(3)

  const { bases } = await create(3, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c] = bases

  for (let i = 0; i < 400; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)

  t.is(a.linearizer.indexers.length, 2)

  await replicateAndSync([a, c])

  const core = c.view
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 2)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - fast forward after migrate', async t => {
  t.plan(3)

  const { bases } = await create(3, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c] = bases

  for (let i = 0; i < 400; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)

  t.is(a.linearizer.indexers.length, 2)

  await a.append('lets index some nodes')
  await confirm([a, b])

  for (let i = 0; i < 5; i++) {
    const unreplicate = replicate([a, b])
    await eventFlush()

    for (let i = 0; i < 60; i++) {
      b.append('b' + i)
      a.append('a' + i)

      if (i % 2 === 0) await eventFlush()
    }

    await unreplicate()
    await confirm([a, b])
  }

  await replicateAndSync([a, b, c])

  const core = c.view
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 2)

  t.ok(sparse > 400)

  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - multiple writers added', async t => {
  t.plan(2)

  const MESSAGES_PER_ROUND = 40

  const { bases } = await create(4, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await b.append('b')
  await c.append('c')

  await confirm([a, b, c])

  const online = [a, b, c]

  for (let i = 0; i < 10; i++) {
    const unreplicate = replicate(online)
    await eventFlush()

    const as = Math.random() * MESSAGES_PER_ROUND
    const bs = Math.random() * MESSAGES_PER_ROUND
    const cs = Math.random() * MESSAGES_PER_ROUND

    for (let j = 0; j < Math.max(as, bs, cs); j++) {
      if (j < as) a.append('a' + j)
      if (j < bs) b.append('b' + j)
      if (j < cs) c.append('c' + j)

      if (j % 2 === 0) await eventFlush()
    }

    await unreplicate()
    await confirm(online)

    if (i === 8) online.push(d)
  }

  const core = d.view
  const sparse = await isSparse(core)

  t.is(d.linearizer.indexers.length, 3)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - multiple queues', async t => {
  const { bases } = await create(4, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d] = bases

  // this value should be long enough that 2 fast-forwards are
  // queued (ie. we don't just replicate the last state), but short
  // enough that the second is queued before the first has completed
  const DELAY = 20

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c, false)

  await b.append('b')
  await c.append('c')

  await confirm([a, b, c])
  await replicateAndSync([a, b, c, d])

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await confirm([a, b, c])

  const midLength = a.core.signedLength

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await confirm([a, b])

  // trigger 1st fast-forward
  const s1 = c.replicate(true)
  const s2 = d.replicate(false)

  s1.pipe(s2).pipe(s1)

  await new Promise(resolve => s2.on('open', resolve))

  // trigger 2nd fast-forward
  setTimeout(() => t.teardown(replicate([a, b, d])), DELAY)

  const to = await new Promise(resolve => d.on('fast-forward', resolve))
  const next = new Promise(resolve => d.on('fast-forward', resolve))

  if (to > midLength) {
    // vary value of DELAY, but make sure the first fast-forward
    // has not completed when the second is queued
    // (check fastForwardTo !== null in queueFastForward)
    t.fail('test failed due to timing')
    return
  }

  await next

  t.pass()
})

// tests needs updating, old apis, internals etc
test.skip('fast-forward - open with no remote io', async t => {
  const { bases, stores } = await create(2, t, {
    apply: applyOldState,
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  await b.ready()

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)
  const unreplicate = replicate([a, b])

  const core = b.view
  const sparse = await isSparse(core)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)

  await b.append('b1')
  await b.append('b2')
  await b.append('b3')

  await unreplicate()

  await a.append('a1001')

  await b.close()

  const local = a.local
  const remote = stores[1].get({ key: local.key })

  const s1 = local.replicate(true)
  const s2 = remote.replicate(false)

  s1.pipe(s2).pipe(s1)

  await remote.download({ end: local.length }).downloaded()

  s1.destroy()
  await new Promise(resolve => s2.on('close', resolve))

  const b2 = createBase(stores[1].session(), a.local.key, t, { apply: applyOldState })
  await t.execution(b2.ready())

  async function applyOldState (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        const key = Buffer.from(value.add, 'hex')
        await base.addWriter(key, { indexer: value.indexer })
        continue
      }

      if (view) await view.append(value)
      const core = base._viewStore.getSystemCore().originalCore

      // get well distributed unique index
      const index = (view.length * 67 + view.length * 89) % core.length
      if (core.length) await core.get(index)
    }
  }
})

// todo: rocks will never use this pathway, rewrite for failure scenario
test.skip('fast-forward - force reset then ff', async t => {
  t.plan(8)

  const { bases } = await create(3, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await confirm([a, b, c])

  t.is(a.core.manifest.signers.length, 3)

  for (let i = 0; i < 400; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])
  await b.append(null)
  await replicateAndSync([a, b])
  await a.append(null)
  await replicateAndSync([a, b])

  for (let i = 0; i < 400; i++) {
    await a.append('a' + i)
  }

  t.ok(b.core.signedLength < 40)

  await confirm([a, c])

  t.ok(a.core.signedLength > 800)

  const truncate = new Promise(resolve => b.core.on('truncate', resolve))

  t.not(b.core.signedLength, a.core.signedLength)

  await b.forceResetViews()

  await replicateAndSync([a, b, c])

  await t.execution(truncate)

  t.is(b.core.signedLength, a.core.signedLength)

  await replicateAndSync([a, c])

  const core = b.core
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 3)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - initial fast forward', async t => {
  t.plan(3)

  const { bases } = await create(2, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)

  t.is(a.linearizer.indexers.length, 2)

  await a.append('lets index some nodes')
  await confirm([a, b])

  for (let i = 0; i < 200; i++) {
    await b.append('b' + i)
  }

  await confirm([a, b])

  const fastForward = { key: a.core.key }

  const [store] = await createStores(1, t, { offset: 2, storage: () => tmpDir(t) })

  const c = createBase(store.session(), a.key, t, { fastForward })
  await c.ready()

  await replicateAndSync([a, b, c])
  const core = c.core
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 2)
  t.ok(sparse > 0)

  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - initial ff after multiple migrate', async t => {
  t.plan(3)

  const { bases } = await create(5, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d, e] = bases

  for (let i = 0; i < 60; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await b.append('b' + i)
  }

  await addWriterAndSync(b, c)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await c.append('c' + i)
  }

  await addWriterAndSync(c, d)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await d.append('d' + i)
  }

  await addWriterAndSync(d, e)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await e.append('e' + i)
  }

  await confirm(bases)

  const sys = a.core
  t.is(sys.manifest.signers.length, 5)

  const fastForward = { key: sys.key }

  const [store] = await createStores(1, t, { offset: 5, storage: () => tmpDir(t) })

  const latecomer = createBase(store.session(), a.key, t, { fastForward })
  await latecomer.ready()

  await replicateAndSync([...bases, latecomer])
  const core = latecomer.core
  const sparse = await isSparse(core)

  t.is(latecomer.linearizer.indexers.length, 5)
  t.ok(sparse > 0)

  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - ignore bogus initial ff', async t => {
  t.plan(3)

  const { bases } = await create(2, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)
  await confirm(bases)

  for (let i = 0; i < 200; i++) {
    await b.append('b' + i)
  }

  const sys = a.core
  t.is(sys.manifest.signers.length, 2)

  const key = Buffer.from(sys.key)
  key[0] ^= 0xff

  const fastForward = {
    key,
    timeout: 1500
  }

  const [store] = await createStores(1, t, { offset: 2, storage: () => tmpDir(t) })

  const latecomer = createBase(store.session(), a.key, t, { fastForward })
  await latecomer.ready()

  await replicateAndSync([...bases, latecomer])
  const core = latecomer.core
  const sparse = await isSparse(core)

  t.is(latecomer.linearizer.indexers.length, 2)

  t.absent(latecomer.fastForwardTo) // fastForward was cleared
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

// very timing dependent this test, so skipping. the idea is good enough tho
// just needs to make less assumptions about when to ff
test.skip('fast-forward - double ff', async t => {
  const { bases } = await create(5, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d, e] = bases

  const migrations = []

  for (let i = 0; i < 60; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await b.append('b' + i)
  }

  migrations.push(a.core.manifest.prologue.length)

  await addWriterAndSync(b, c)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await c.append('c' + i)
  }

  migrations.push(a.core.manifest.prologue.length)

  await addWriterAndSync(c, d)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await d.append('d' + i)
  }

  migrations.push(a.core.manifest.prologue.length)

  await addWriterAndSync(d, e)
  await confirm(bases)

  for (let i = 0; i < 60; i++) {
    await e.append('e' + i)
  }

  migrations.push(a.core.manifest.prologue.length)

  await confirm(bases)

  const sys = a.core
  t.is(sys.manifest.signers.length, 5)

  const [store] = await createStores(1, t, { offset: 5, storage: () => tmpDir(t) })

  const latecomer = createBase(store.session(), a.key, t, { fastForward: true })
  await latecomer.ready()

  const p = replicateAndSync([...bases, latecomer])

  // check that the migration happened from start to end
  for await (const [to, from] of on(latecomer, 'fast-forward')) {
    t.ok(from < migrations.shift())
    t.ok(migrations.length > 1)

    if (!migrations.length || to > migrations[migrations.length - 1]) break
  }

  await p

  const core = latecomer.core
  const sparse = await isSparse(core)

  t.is(latecomer.linearizer.indexers.length, 5)
  t.ok(sparse > 0)

  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - unindexed cores should migrate', async t => {
  const { bases } = await create(4, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c, false)

  await b.append('b')
  await c.append('c')

  await confirm([a, b, c])
  await replicateAndSync([a, b, c, d])

  t.is(a.core.signedLength, c.core.signedLength)
  t.alike(a.core.key, c.core.key)
})

test('fast-forward - initial fast forward with in between writer', async t => {
  t.plan(3)

  const { bases } = await create(2, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b, false)

  await replicateAndSync([a, b])
  await b.append('in between')
  await replicateAndSync([a, b])

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i + 200)
  }

  await replicateAndSync([a, b])

  t.is(a.linearizer.indexers.length, 1)

  const fastForward = { key: a.core.key }

  const [store] = await createStores(1, t, { offset: 2, storage: () => tmpDir(t) })

  const c = createBase(store.session(), a.key, t, { fastForward })
  await c.ready()

  t.teardown(replicate([a, c]))

  // wait some time so c's initial wakeup is not up to date
  await new Promise(resolve => setTimeout(resolve, 1000))

  t.teardown(replicate([a, b]))
  await b.append('c no see')

  await t.execution(sync([a, b, c]))

  t.pass()
})

test('fast-forward - writer removed', async t => {
  t.plan(3)

  const { bases } = await create(2, t, {
    fastForward: true,
    apply: applyWithRemove,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  for (let i = 0; i < 400; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b, false)

  t.is(b.writable, true)

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])

  const core = b.view
  const sparse = await isSparse(core)

  t.is(b.writable, false)

  t.ok(sparse > 0)

  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - is indexer set correctly', async t => {
  t.plan(11)

  const { bases } = await create(4, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d] = bases

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  // add writer
  await addWriterAndSync(a, b)
  await replicateAndSync([a, b, c, d])

  await addWriter(a, c, false)
  await replicateAndSync([a, c])

  await c.append(null)
  await replicateAndSync([a, b, c, d])

  // promote writer
  await addWriter(a, c, true)
  await confirm([a, b])

  t.is(a.linearizer.indexers.length, 3)
  t.is(b.linearizer.indexers.length, 3)
  t.is(c.linearizer.indexers.length, 2)

  t.absent(c.isIndexer)
  t.absent(c.localWriter.isActiveIndexer)

  for (let i = 200; i < 400; i++) {
    await a.append('a' + i)
  }

  // c has ff'd past addWriter
  await confirm([a, b])

  await replicateAndSync([a, d])

  t.is(a.view.signedLength, 400)

  t.is(c.linearizer.indexers.length, 2)

  const event = new Promise(resolve => c.on('is-indexer', resolve))

  await replicateAndSync([c, d])

  t.is(c.linearizer.indexers.length, 3)
  t.ok(c.isIndexer)
  t.ok(c.localWriter.isActiveIndexer)

  await t.execution(event)
})

test('fast-forward - multiple views reordered', async t => {
  t.plan(3)

  const { bases } = await create(2, t, {
    fastForward: true,
    open: openMultiple,
    apply: applyMultiple,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  await addWriterAndSync(a, b, false)

  await b.append({ index: 1, data: 'b0' })
  await a.append({ index: 2, data: 'a0' })

  for (let i = 0; i < 1000; i++) {
    await a.append(null)
  }

  await a.append({ index: 1, data: 'a1' })

  t.is(a.core.signedLength, 2008)

  await addWriter(a, b, true)
  await replicateAndSync([a, b])

  const core = b.core
  const sparse = await isSparse(core)

  t.alike(b.view.key, a.view.key)

  await b.append({ index: 2, data: 'a2' })

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - static fast-forward', async t => {
  const { bases } = await create(4, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c, false)

  await b.append('b')
  await c.append('c')

  await confirm([a, b, c])

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, c)

  for (let i = 0; i < 200; i++) {
    await a.append('a' + i)
  }

  await confirm([a, b])

  let count = 0
  d.on('fast-forward', () => count++)

  const ff = new Promise(resolve => d.once('fast-forward', resolve))

  // trigger fast-forward
  t.teardown(replicate([a, b, d]))

  t.is(await ff, a.core.signedLength)
  t.alike(d.core.key, a.core.key)
  t.is(count, 1)
  t.pass()
})

test('fast-forward - initial ff with zero length view', async t => {
  t.plan(4)

  const { bases } = await create(2, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b] = bases

  for (let i = 0; i < 1000; i++) {
    await a.append(null)
  }

  await addWriterAndSync(a, b)

  t.is(a.linearizer.indexers.length, 2)

  const fastForward = { key: a.core.key }

  const [store] = await createStores(1, t, { offset: 2, storage: () => tmpDir(t) })

  const c = createBase(store.session(), a.key, t, { fastForward })
  await c.ready()

  await replicateAndSync([a, b, c])

  const core = c.core
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 2)
  t.alike(c.view.key, a.view.key)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

async function isSparse (core) {
  let n = 0
  for (let i = 0; i < core.length; i++) {
    if (!await core.has(i)) n++
  }
  return n
}

function openMultiple (store) {
  return {
    first: store.get('first', { valueEncoding: 'json' }),
    second: store.get('second', { valueEncoding: 'json' })
  }
}

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

async function applyMultiple (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'), { indexer: value.indexer !== false })
      continue
    }

    if (value.index === 1) {
      await view.first.append(value.data)
    } else if (value.index === 2) {
      await view.second.append(value.data)
    }
  }
}
