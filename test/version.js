const test = require('brittle')
const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('..')

const {
  apply,
  addWriter,
  confirm,
  replicate,
  sync
} = require('./helpers')

test('basic - two writers', async t => {
  const s1 = new Corestore(ram.reusable(), { primaryKey: Buffer.alloc(32).fill(0) })
  const s2 = new Corestore(ram.reusable(), { primaryKey: Buffer.alloc(32).fill(1) })

  const opts = {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json',
    fastForward: false
  }

  const a = new Autobase(s1.session(), null, opts)
  await a.ready()

  const b = new Autobase(s2.session(), a.bootstrap, opts)
  await b.ready()

  await addWriter(a, b)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(b.system.members, a.system.members)

  const unreplicate = replicate([a, b])

  for (let i = 0; i < 10; i++) {
    await a.append('a' + i)
  }

  for (let i = 0; i < 10; i++) {
    await b.append('b' + i)
  }

  await confirm([a, b])
  await sync([a, b])

  t.is(a.view.indexedLength, 20)
  t.is(a.view.indexedLength, b.view.indexedLength)

  await unreplicate()
  await b.close()

  const b2 = new Autobase(s2.session(), a.bootstrap, opts)
  b2.version = 1
  await b2.ready()

  a.once('warning', () => t.pass())

  t.teardown(replicate([a, b2]))

  for (let i = 10; i < 20; i++) {
    await b2.append('b' + i)
  }

  t.is(a.view.length, 20)
  t.absent(a.closing)

  await a.append('a11')

  t.is(a.view.length, 21)
  t.is(b2.view.length, 30)
})
