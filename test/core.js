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
