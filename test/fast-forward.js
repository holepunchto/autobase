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

  for (let i = 0; i < 1000; i++) {
    await b.append('b' + i)
  }

  await confirm([a, b])

  await replicateAndSync([a, b, c])

  const core = c.view.getBackingCore()
  const sparse = await isSparse(core)

  t.is(c.linearizer.indexers.length, 2)

  t.ok(sparse > 2000)
  t.comment('sparse blocks: ' + sparse)
})

test('fast-forward - multiple writers added', async t => {
  t.plan(2)

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

  let unreplicate = replicate([a, b, c])

  d.debug = true
  for (let i = 0; i < 50; i++) {
    const appends = []

    for (let i = 0; i < Math.random() * 10; i++) {
      appends.push(a.append('a' + i))
    }

    for (let i = 0; i < Math.random() * 10; i++) {
      appends.push(b.append('b' + i))
    }

    for (let i = 0; i < Math.random() * 10; i++) {
      appends.push(c.append('c' + i))
    }

    await Promise.all(appends)
    if (i % 20 === 0) {
      await unreplicate()
      await confirm([a, b, c])
      unreplicate = replicate([a, b, c, d])
      await eventFlush()
    }
  }

  await unreplicate()
  const core = d.view.getBackingCore()
  const sparse = await isSparse(core)

  t.is(d.linearizer.indexers.length, 3)

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
