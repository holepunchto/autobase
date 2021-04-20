const test = require('tape')
const Hypercore = require('hypercore-x')
const ram = require('random-access-memory')

const MemoryView = require('../lib/views/memory')

test('simple memory view', async t => {
  t.plan(9)

  const core = new Hypercore(ram, { valueEncoding: 'utf-8' })
  await core.append(['a', 'b', 'c'])

  const view = MemoryView.from(core)
  await view.append('d')

  t.same(view.length, 4)
  t.same(view.byteLength, 4)
  t.same(core.length, 3)
  console.log('view here:', view)

  t.same(await view.get(3), 'd')
  console.log('after view get')
  try {
    console.log('before core get, core:', core)
    await core.get(3)
    console.log('after core get')
    t.fail('core get should have thrown')
  } catch {
    t.pass('core get threw correctly')
  }

  await view.truncate(2)
  t.same(view.length, 2)
  t.same(core.length, 3)
  t.same(await view.get(1), 'b')
  try {
    await view.get(2)
    t.fail('view should have thrown')
  } catch {
    t.pass('view threw correctly')
  }
})
