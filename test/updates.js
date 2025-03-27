const test = require('brittle')

const {
  create,
  createStores,
  createBase,
  confirm,
  addWriter,
  replicateAndSync
} = require('./helpers')

test('updates - simple', async t => {
  const { bases } = await create(1, t, { update })
  const [a] = bases

  await a.append('a1')

  t.is(a.system.members, 1)
  t.is(a.view.length, 1)
  t.is(a.view.signedLength, 1)

  await a.append('a2')

  t.is(a.view.length, 2)
  t.is(a.view.signedLength, 2)

  async function update (view, changes) {
    const update = changes.get('view')

    // always appending one
    t.is(update.to - update.from, 1)
    t.is(update.shared, update.from)
  }
})

test('updates - truncate', async t => {
  const { bases } = await create(2, t, { update })
  const [a, b] = bases

  let truncations = 0

  await addWriter(a, b)
  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.view.length, 0)
  t.is(a.view.signedLength, 0)

  await a.append('a1')
  await b.append('a1')

  await replicateAndSync([a, b])

  t.is(truncations, 1)

  async function update (view, changes) {
    const update = changes.get('view')
    if (update.from === update.shared) return

    truncations++

    t.is(update.shared, 0)
    t.is(update.from, 1)
    t.is(update.to, 2)
  }
})

test('updates - fast-forward', async t => {
  let updates = 0

  const stores = await createStores(2, t)

  const a = createBase(stores[0], null, t, { fastForward: true })
  await a.ready()

  const b = createBase(stores[1], a.local.key, t, {
    fastForward: true,
    update
  })

  for (let i = 0; i < 1000; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])

  const sparse = await isSparse(b.view)

  t.ok(sparse > 0)
  t.is(updates, 1)

  async function update (view, changes) {
    const update = changes.get('view')
    if (update.to !== 1000) return

    updates++

    t.is(update.shared, update.from) // no truncations
    t.is(update.to, 1000)
  }
})

test('updates - fast-forward with truncation', async t => {
  let truncations = 0

  const stores = await createStores(4, t)

  const a = createBase(stores[0], null, t, { fastForward: true })
  await a.ready()

  const b = createBase(stores[1], a.local.key, t, { fastForward: true })
  const c = createBase(stores[2], a.local.key, t, { fastForward: true, update })
  const d = createBase(stores[3], a.local.key, t, { fastForward: true }) // just to sync

  await b.ready()
  await c.ready()

  await addWriter(a, b)
  await addWriter(a, c)

  await a.append('confirmed')
  await a.append('data')

  await confirm([a, b, c])

  const shared = c.view.length

  t.is(a.linearizer.indexers.length, 3)

  await c.append('some')
  await c.append('data')
  await c.append('to')
  await c.append('truncate')

  const from = c.view.length

  for (let i = 2; i < 1000; i++) {
    await a.append('a' + i)
  }

  await confirm([a, b, d])

  t.is(d.view.signedLength, 1000)

  await replicateAndSync([c, d])

  const sparse = await isSparse(c.view)

  t.ok(sparse > 0)
  t.is(truncations, 1)

  async function update (view, changes) {
    const update = changes.get('view')
    if (update.to !== 1000) return

    truncations++

    t.ok(update.from >= from)
    t.is(update.shared, shared)
    t.is(update.to, 1000)
  }
})

async function isSparse (core) {
  let n = 0
  for (let i = 0; i < core.length; i++) {
    if (!await core.has(i)) n++
  }
  return n
}
