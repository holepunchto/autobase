const test = require('brittle')
const ram = require('random-access-memory')
const Hypercore = require('hypercore')

const {
  create,
  apply
} = require('./helpers')
const LinearizedCore = require('../lib/core')

test('core - isAutobase property', async t => {
  const [base] = await create(1, apply, store => store.get('test'))
  const normalCore = new Hypercore(ram)
  const linearizedSessionCore = base.view
  const linearizedCore = new LinearizedCore(base, normalCore, 'name')

  t.is(linearizedSessionCore.isAutobase, true)
  t.is(linearizedCore.isAutobase, true)
  t.is(!normalCore.isAutobase, true)
})

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
