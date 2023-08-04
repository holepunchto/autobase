const test = require('brittle')
const ram = require('random-access-memory')
const Hypercore = require('hypercore')

const {
  create,
  apply,
  addWriter,
  sync
} = require('./helpers')

test('core -  no new session if closed (hypercore compat)', async t => {
  const [base] = await create(1, apply, store => store.get('test'))
  const normalCore = new Hypercore(ram)
  const linearizedSessionCore = base.view
  const snapshotSession = linearizedSessionCore.snapshot()

  await Promise.all([
    linearizedSessionCore.close(),
    normalCore.close(),
    snapshotSession.close()
  ])

  // LinearisedCore.session handles snapshots differently than hypercore.session,
  // so just testing that explicitly too

  t.exception(() => normalCore.session(), /SESSION_CLOSED/)
  t.exception(() => normalCore.snapshot(), /SESSION_CLOSED/)

  t.exception(() => linearizedSessionCore.session(), /SESSION_CLOSED/)
  t.exception(() => linearizedSessionCore.snapshot(), /SESSION_CLOSED/)

  t.exception(() => snapshotSession.session(), /SESSION_CLOSED/)
  t.exception(() => snapshotSession.snapshot(), /SESSION_CLOSED/)
})

test('core - seek', async t => {
  const [base] = await create(1, apply, store => store.get('test'))

  const b1 = base.view.byteLength
  await base.append('hello')

  const b2 = base.view.byteLength
  await base.append('and')

  const b3 = base.view.byteLength
  await base.append('goodbye')

  t.alike(await base.view.seek(b1), [0, 0])
  t.alike(await base.view.seek(b2), [1, 0])
  t.alike(await base.view.seek(b3), [2, 0])

  t.alike(await base.view.seek(b1 + 1), [0, 1])
  t.alike(await base.view.seek(b2 + 1), [1, 1])
  t.alike(await base.view.seek(b3 + 1), [2, 1])

  t.alike(await base.view.seek(b3 + 10), null)

  // expected behaviour?
  // t.alike(await base.view.seek(b1 - 10), null)
})

test('core - seek multi writer', async t => {
  const [a, b] = await create(2, scopedApply, store => store.get('test'))

  await addWriter(a, b)

  await a.append('hello')
  await sync(a, b)

  await b.append('and')
  await sync(a, b)

  await a.append('goodbye')
  await sync(a, b)

  t.is(a.view.length, 3)
  t.is(a.view.byteLength, 15)
  t.is(a.view.indexedLength, 1)
  t.is(a.view.indexedByteLength, 5)

  t.alike(a.view.length, b.view.length)
  t.alike(a.view.byteLength, b.view.byteLength)
  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedByteLength, b.view.indexedByteLength)

  let i = 0
  while (i++ < a.view.byteLength) {
    t.alike(a.view.seek(i), b.view.seek(i))
  }

  // synced so views should be same
  while (i++ < a.view.byteLength) {
    t.alike(a.view.seek(i), b.view.seek(i))
  }

  // byteLength tests are apply dependent
  async function scopedApply (batch, view, base) {
    for (const { value } of batch) {
      if (value === null) continue
      if (value.add) {
        base.system.addWriter(Buffer.from(value.add, 'hex'))
        continue
      }

      if (view) await view.append(value)
    }
  }
})

test('core - userData', async t => {
  const [base] = await create(1, apply, store => store.get('test'))

  await base.view.setUserData('first', Buffer.from('hello'))
  await base.view.setUserData('second', Buffer.from('goodbye'))

  t.alike(await base.view.getUserData('first'), Buffer.from('hello'))
  t.alike(await base.view.getUserData('second'), Buffer.from('goodbye'))
  t.alike(await base.view.getUserData('third'), null)

  const session = base.view.session()
  t.alike(await session.getUserData('first'), Buffer.from('hello'))

  await session.setUserData('first', Buffer.from('change'))

  t.alike(await base.view.getUserData('first'), Buffer.from('change'))
  t.alike(await session.getUserData('first'), Buffer.from('change'))
})
