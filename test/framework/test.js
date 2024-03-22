const RAM = require('random-access-memory')
const b4a = require('b4a')
const test = require('brittle')

const { Base, Room } = require('./')

test('framework - base', async t => {
  const base = new Base(RAM.reusable())

  await t.execution(base.ready())
  await t.execution(base.append('msg'))

  const { view } = base.getState()

  for (let i = 0; i < view.length; i++) {
    t.is(b4a.toString(await view.get(i)), 'msg')
  }

  await base.close()
})

test('framework - base - sync', async t => {
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

  await root.close()
  await base.close()
})

test('framework - base - unreplicate', async t => {
  const root = new Base(RAM.reusable())
  await root.ready()

  await root.append('msg1')

  const base = new Base(RAM.reusable(), { root })
  await base.ready()

  const unreplicate = base.replicate(root)
  await base.sync(root)

  await unreplicate()

  await root.append('msg2')

  const r = root.getState()
  const b = base.getState()

  t.is(r.view.length, 2)
  t.is(b.view.length, 1)

  await root.close()
  await base.close()
})

test('framework - base - add writer', async t => {
  const root = new Base(RAM.reusable())
  await root.ready()

  const base = new Base(RAM.reusable(), { root })
  await base.ready()

  base.replicate(root)

  await base.join()

  await base.append('msg')
  await root.sync(base)

  t.is(root.base.view.length, 1)
  t.is(base.base.view.length, 1)

  await root.close()
  await base.close()
})

test('framework - base - 3 indexers', async t => {
  const root = new Base(RAM.reusable())
  await root.ready()

  const a = new Base(RAM.reusable(), { root })
  const b = new Base(RAM.reusable(), { root })

  await a.ready()
  await b.ready()

  const rootADown = root.replicate(a)
  const rootBDown = root.replicate(b)
  a.replicate(b)

  await root.sync([a, b])
  await a.join({ indexer: true })

  await root.sync([a, b])
  await b.join({ indexer: true })

  // confirm
  await root.sync([a, b])
  await a.append(null)
  await root.sync([a, b])
  await root.append(null)

  // root offline
  await Promise.all([
    rootADown(),
    rootBDown()
  ])

  t.is(b.getState().indexers.length, 3)

  await a.append('msg')

  // confirm
  await a.sync(b)
  await b.append(null)
  await a.sync(b)
  await a.append(null)
  await a.sync(b)
  await b.append(null)

  t.is(a.base.view.indexedLength, 1)
  t.is(b.base.view.indexedLength, 1)

  t.is(root.base.view.length, 0)
  t.is(root.base.view.indexedLength, 0)

  await root.close()
  await a.close()
  await b.close()
})

test('framework - room', async t => {
  const room = new Room(() => RAM.reusable(), { size: 2 })
  await room.ready()

  t.teardown(() => room.close())

  t.alike(room.key, room.root.base.bootstrap)

  room.replicate()

  await room.root.spam(100)

  await room.sync()

  for (const member of room) {
    t.is(member.base.view.length, 100)
    t.is(member.base.view.indexedLength, 100)
  }
})

test('framework - room - add writers', async t => {
  const room = new Room(() => RAM.reusable())
  await room.ready()

  t.teardown(() => room.close())

  const net = room.replicate()

  const members = await room.createMembers(2)
  await room.addWriters(members, { indexer: true })

  t.is(room.indexers.length, room.root.base.linearizer.indexers.length)
  t.is(room.indexers.length, 3)

  await room.root.spam(100)
  await room.sync()

  for (const member of room) {
    t.is(member.base.view.length, 100)
    t.is(member.base.view.indexedLength, 0)
  }

  await room.confirm()

  for (const member of room) {
    t.is(member.base.view.indexedLength, 100)
  }
})

test('framework - room - spam', async t => {
  const room = new Room(() => RAM.reusable())
  await room.ready()

  t.teardown(() => room.close())

  room.replicate()

  const members = await room.createMembers(2)
  await room.addWriters(members, { indexer: true })

  t.is(room.indexers.length, room.root.base.linearizer.indexers.length)
  t.is(room.indexers.length, 3)

  await room.spam(members, [100, 50])
  await room.sync()

  for (const member of room) {
    t.is(member.base.view.length, 150)
  }

  await room.confirm()

  for (const member of room) {
    t.is(member.base.view.indexedLength, 150)
  }
})

test('framework - room - netsplit', async t => {
  const room = new Room(() => RAM.reusable())
  await room.ready()

  const members = await room.createMembers(4)
  await room.addWriters(members, { indexer: true })

  const replicated = room.replicate()

  t.teardown(() => room.close())

  t.is(room.indexers.length, room.root.base.linearizer.indexers.length)
  t.is(room.indexers.length, 5)

  t.is(replicated.size, 5)

  const [left, right] = await replicated.split(2)

  await room.spam(left.members(), [100, 50])
  await room.spam(right.members(), [90, 200, 10])

  t.is(left.size, 2)
  t.is(right.size, 3)

  await room.sync(left.members())

  for (const member of left) {
    t.is(member.base.view.length, 150)
  }

  await room.sync(right.members())

  for (const member of right) {
    t.is(member.base.view.length, 300)
  }

  await room.confirm(right.members())

  for (const member of left) {
    t.is(member.base.view.indexedLength, 0)
  }

  for (const member of right) {
    t.is(member.base.view.indexedLength, 300)
  }

  room.replicate()
  await room.sync()

  for (const member of room) {
    t.is(member.base.view.indexedLength, 300)
  }

  // trigger autoack
  await new Promise(resolve => setTimeout(resolve, 1000))

  for (const member of room) {
    t.is(member.base.view.indexedLength, 450)
  }
})
