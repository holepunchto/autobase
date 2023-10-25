const RAM = require('random-access-memory')
const b4a = require('b4a')
const test = require('brittle')
const { Base, Room } = require('./')

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

test('framework - room', async t => {
  const room = new Room(() => RAM.reusable(), { size: 2 })
  await room.ready()

  room.replicate()

  await room.root.spam(100)

  await room.sync()

  for (const member of room) {
    t.is(member.base.view.length, 100)
    t.is(member.base.view.indexedLength, 100)
  }
})

test('framework - add writers', async t => {
  const room = new Room(() => RAM.reusable())
  await room.ready()

  room.replicate()

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

test('framework - add writers', async t => {
  const room = new Room(() => RAM.reusable())
  await room.ready()

  room.replicate()

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

test('framework - room spam', async t => {
  const room = new Room(() => RAM.reusable())
  await room.ready()

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

test('framework - room netsplit', async t => {
  const room = new Room(() => RAM.reusable())
  await room.ready()

  room.replicate()

  const members = await room.createMembers(4)
  await room.addWriters(members, { indexer: true })

  t.is(room.indexers.length, room.root.base.linearizer.indexers.length)
  t.is(room.indexers.length, 5)

  const left = room.indexers.slice(0, 2) // minority
  const right = room.indexers.slice(2) // majority

  await room.netsplit(left, right)

  await room.spam(left, [100, 50])
  await room.spam(right, [90, 200])

  await room.sync(left)

  for (const member of left) {
    t.is(member.base.view.length, 150)
  }

  await room.sync(right)

  for (const member of right) {
    t.is(member.base.view.length, 290)
  }

  await room.confirm(right)

  for (const member of left) {
    t.is(member.base.view.indexedLength, 0)
  }

  for (const member of right) {
    t.is(member.base.view.indexedLength, 290)
  }

  room.replicate()
  await room.sync()

  for (const member of room) {
    t.is(member.base.view.indexedLength, 290)
  }

  // trigger autoack
  await new Promise(resolve => setTimeout(resolve, 2000))

  for (const member of room) {
    t.is(member.base.view.indexedLength, 440)
  }
})
