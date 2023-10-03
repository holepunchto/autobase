const test = require('brittle')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')

const Autobase = require('..')

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

async function isSparse (core) {
  let n = 0
  for (let i = 0; i < core.length; i++) {
    if (!await core.has(i)) n++
  }
  return n
}
