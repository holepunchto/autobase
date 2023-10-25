const RAM = require('random-access-memory')
const b4a = require('b4a')
const test = require('brittle')
const { Base } = require('./')

test('framework', async t => {
  const base = new Base(RAM.reusable())

  await t.execution(base.ready())
  await t.execution(base.append('msg'))

  const { view } = base.getState()

  for (let i = 0; i < view.length; i++) {
    t.is(b4a.toString(await view.get(i)), 'msg')
  }
})

test('framework - sync', async t => {
  const root = new Base(RAM.reusable())
  await root.ready()

  await root.append('msg')

  const base = new Base(RAM.reusable(), { root })
  await base.ready()

  base.replicate(root)
  await t.execution(base.sync())

  const { view } = base.getState()

  for (let i = 0; i < view.length; i++) {
    t.is(b4a.toString(await view.get(i)), 'msg')
  }
})

test('framework - unreplicate', async t => {
  const root = new Base(RAM.reusable())
  await root.ready()

  await root.append('msg1')

  const base = new Base(RAM.reusable(), { root })
  await base.ready()

  base.replicate(root)
  await base.sync()

  await base.unreplicate()

  await root.append('msg2')

  const r = root.getState()
  const b = base.getState()

  t.is(r.view.length, 2)
  t.is(b.view.length, 1)
})

test('framework - add writer', async t => {
  const root = new Base(RAM.reusable())
  await root.ready()

  const base = new Base(RAM.reusable(), { root })
  await base.ready()

  base.replicate(root)

  await base.join()

  await base.append('msg')
  await root.sync()

  t.is(root.base.view.length, 1)
  t.is(base.base.view.length, 1)
})

test('framework - 3 indexers', async t => {
  const root = new Base(RAM.reusable())
  await root.ready()

  const a = new Base(RAM.reusable(), { root })
  const b = new Base(RAM.reusable(), { root })

  await a.ready()
  await b.ready()

  a.replicate([root, b])
  b.replicate([root, a])

  await a.sync()
  await a.join({ indexer: true })

  await b.sync()
  await b.join({ indexer: true })

  // confirm
  await a.sync()
  await a.append(null)
  await root.sync()
  await root.append(null)

  // offline
  await root.offline()

  t.is(b.getState().indexers.length, 3)

  await a.append('msg')

  // confirm
  await b.sync()
  await b.append(null)
  await a.sync()
  await a.append(null)
  await b.sync()
  await b.append(null)

  t.is(a.base.view.indexedLength, 1)
  t.is(b.base.view.indexedLength, 1)

  t.is(root.base.view.length, 0)
  t.is(root.base.view.indexedLength, 0)
})
