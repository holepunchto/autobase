const test = require('brittle')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')
const cenc = require('compact-encoding')

const Autobase = require('..')
const { SystemPointer } = require('../lib/messages')

const {
  addWriterAndSync,
  replicateAndSync,
  replicate,
  eventFlush,
  confirm,
  apply,
  create
} = require('./helpers')

test('fast-forward - simple', async t => {
  t.plan(1)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(0)
  })

  const store2 = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const a = new Autobase(store.session(), null, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0
  })

  await a.ready()

  const b = new Autobase(store2.session(), a.local.key, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0
  })

  await b.ready()

  t.teardown(() => Promise.all([
    a.close(),
    b.close()
  ]))

  for (let i = 0; i < 1000; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])

  const core = b.view.getBackingCore()
  const sparse = await isSparse(core)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
})

test('fast-forward - migrate', async t => {
  t.plan(3)

  const open = store => store.get('view', { valueEncoding: 'json' })
  const [a, b, c] = await create(3, apply, open, null, {
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: true,
    storage: () => tmpDir(t)
  })

  t.teardown(() => Promise.all([
    a.close(),
    b.close(),
    c.close()
  ]))

  for (let i = 0; i < 2000; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)

  t.is(a.linearizer.indexers.length, 2)

  await replicateAndSync([a, c])

  const core = c.view.getBackingCore()
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 2)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
})

test('fast-forward - fast forward after migrate', async t => {
  t.plan(3)

  const open = store => store.get('view', { valueEncoding: 'json' })
  const [a, b, c] = await create(3, apply, open, null, {
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: true,
    storage: () => tmpDir(t)
  })

  t.teardown(() => Promise.all([
    a.close(),
    b.close(),
    c.close()
  ]))

  for (let i = 0; i < 2000; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)

  t.is(a.linearizer.indexers.length, 2)

  await a.append('lets index some nodes')
  await confirm([a, b])

  for (let i = 0; i < 5; i++) {
    const unreplicate = replicate([a, b])
    await eventFlush()

    for (let i = 0; i < 300; i++) {
      b.append('b' + i)
      a.append('a' + i)

      if (i % 2 === 0) await eventFlush()
    }

    await unreplicate()
    await confirm([a, b])
  }

  await replicateAndSync([a, b, c])

  const core = c.view.getBackingCore()
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 2)

  t.ok(sparse > 2000)

  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - multiple writers added', async t => {
  t.plan(2)

  const MESSAGES_PER_ROUND = 200

  const open = store => store.get('view', { valueEncoding: 'json' })
  const [a, b, c, d] = await create(4, apply, open, null, {
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: true,
    storage: () => tmpDir(t)
  })

  t.teardown(() => Promise.all([
    a.close(),
    b.close(),
    c.close(),
    d.close()
  ]))

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

  const core = d.view.getBackingCore()
  const sparse = await isSparse(core)

  t.is(d.linearizer.indexers.length, 3)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
  t.comment('percentage: ' + (sparse / core.length * 100).toFixed(2) + '%')
})

test('fast-forward - multiple queues', async t => {
  const open = store => store.get('view', { valueEncoding: 'json' })
  const [a, b, c, d] = await create(4, apply, open, null, {
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: true,
    storage: () => tmpDir(t)
  })

  t.teardown(() => Promise.all([
    a.close(),
    b.close(),
    c.close(),
    d.close()
  ]))

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

  for (let i = 0; i < 1000; i++) {
    await a.append('a' + i)
  }

  await confirm([a, b, c])

  const midLength = a.system.core.indexedLength

  for (let i = 0; i < 1000; i++) {
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
  let done = false

  {
    const pointer = await d.local.getUserData('autobase/system')
    const { indexed } = cenc.decode(SystemPointer, pointer)
    t.is(indexed.length, to)

    done = to > midLength
  }

  if (done) {
    // vary value of DELAY, but make sure the first fast-forward
    // has not completed when the second is queued
    // (check fastForwardTo !== null in queueFastForward)
    t.fail('test failed due to timing')
    return
  }

  const final = await next

  {
    const pointer = await d.local.getUserData('autobase/system')
    const { indexed } = cenc.decode(SystemPointer, pointer)
    t.is(indexed.length, final)
  }
})

test('fast-forward - open with no remote io', async t => {
  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(0)
  })

  const store2 = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const a = new Autobase(store.session(), null, {
    apply: applyOldState,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0
  })

  await a.ready()

  const b = new Autobase(store2.session(), a.local.key, {
    apply: applyOldState,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0
  })

  await b.ready()

  t.teardown(() => Promise.all([
    a.close(),
    b.close()
  ]))

  for (let i = 0; i < 1000; i++) {
    await a.append('a' + i)
  }

  await addWriterAndSync(a, b)
  const unreplicate = replicate([a, b])

  const core = b.view.getBackingCore()
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
  const remote = store2.get({ key: local.key })

  const s1 = local.replicate(true)
  const s2 = remote.replicate(false)

  s1.pipe(s2).pipe(s1)

  await remote.download({ end: local.length }).downloaded()
  s1.destroy()
  await new Promise(resolve => s2.on('close', resolve))

  const b2 = new Autobase(store2.session(), a.local.key, {
    apply: applyOldState,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0
  })

  await b2.ready()
  await t.execution(b2.ready())

  async function applyOldState (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        const key = Buffer.from(value.add, 'hex')
        await base.addWriter(key, { indexer: value.indexer })
        continue
      }

      if (view) await view.append(value)
      const core = view._source.core.session

      // get well distributed unique index
      const index = (view.length * 67 + view.length * 89) % core.length
      if (core.length) await core.get(index)
    }
  }
})

test('fast-forward - force reset then ff', async t => {
  t.plan(9)

  const open = store => store.get('view', { valueEncoding: 'json' })
  const [a, b, c] = await create(3, apply, open, null, {
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0,
    fastForward: true,
    storage: () => tmpDir(t)
  })

  t.teardown(() => Promise.all([
    a.close(),
    b.close(),
    c.close()
  ]))

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await confirm([a, b, c])

  t.is(a.system.core.getBackingCore().manifest.signers.length, 3)

  for (let i = 0; i < 2000; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])
  await b.append(null)
  await replicateAndSync([a, b])
  await a.append(null)
  await replicateAndSync([a, b])

  for (let i = 0; i < 2000; i++) {
    await a.append('a' + i)
  }

  t.ok(b.system.core.getBackingCore().flushedLength > 2000)
  t.ok(b.system.core.getBackingCore().indexedLength < 40)

  await confirm([a, c])

  t.ok(a.system.core.getBackingCore().indexedLength > 4000)

  const truncate = new Promise(resolve => b.system.core.on('truncate', resolve))

  t.not(b.system.core.getBackingCore().indexedLength, a.system.core.getBackingCore().indexedLength)

  await b.forceResetViews()

  await replicateAndSync([a, b, c])

  await t.execution(truncate)

  t.is(b.system.core.getBackingCore().indexedLength, a.system.core.getBackingCore().indexedLength)

  await replicateAndSync([a, c])

  const core = b.system.core.getBackingCore()
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 3)

  t.ok(sparse > 0)
  t.comment('sparse blocks: ' + sparse)
})

async function isSparse (core) {
  let n = 0
  for (let i = 0; i < core.length; i++) {
    if (!await core.has(i)) n++
  }
  return n
}
