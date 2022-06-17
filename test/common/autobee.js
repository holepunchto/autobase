const test = require('tape')
const ram = require('random-access-memory')
const Hypercore = require('hypercore')

const Autobase = require('../..')
const SimpleAutobee = require('../../examples/autobee-simple')
const AutobeeWithResolution = require('../../examples/autobee-with-resolution')

test('simple autobee', async t => {
  const firstUser = new Hypercore(ram)
  const firstOutput = new Hypercore(ram)
  const secondUser = new Hypercore(ram)
  const secondOutput = new Hypercore(ram)

  const inputs = [firstUser, secondUser]

  const base1 = new Autobase({
    inputs,
    localOutput: firstOutput,
    localInput: firstUser
  })
  const base2 = new Autobase({
    inputs,
    localOutput: secondOutput,
    localInput: secondUser
  })
  const base3 = new Autobase({
    inputs,
    outputs: [firstOutput, secondOutput]
  })

  const writer1 = new SimpleAutobee(base1, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  const writer2 = new SimpleAutobee(base2, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  // Simulates a remote reader (not part of the group).
  const reader = new SimpleAutobee(base3, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })

  await writer1.put('a', 'a')
  await writer2.put('b', 'b')

  t.same(firstUser.length, 1)
  t.same(secondUser.length, 1)

  {
    const node = await writer2.get('a')
    t.true(node)
    t.same(node.value, 'a')
  }

  {
    const node = await writer1.get('b')
    t.true(node)
    t.same(node.value, 'b')
  }

  {
    const node = await reader.get('a')
    t.true(node)
    t.same(node.value, 'a')
  }

  // Both indexes should have processed two writes.
  t.same(firstOutput.length, 3)
  t.same(secondOutput.length, 3)

  t.end()
})

test('autobee with basic conflict resolution (only handles puts)', async t => {
  const firstUser = new Hypercore(ram)
  const firstOutput = new Hypercore(ram)
  const secondUser = new Hypercore(ram)
  const secondOutput = new Hypercore(ram)

  const inputs = [firstUser, secondUser]

  const base1 = new Autobase({
    inputs,
    localOutput: firstOutput,
    localInput: firstUser
  })
  const base2 = new Autobase({
    inputs,
    localOutput: secondOutput,
    localInput: secondUser
  })

  const writer1 = new AutobeeWithResolution(base1, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  const writer2 = new AutobeeWithResolution(base2, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })

  // Create two forking writes to 'a'
  await writer1.put('a', 'a', []) // [] means empty clock
  await writer1.put('b', 'b', []) // Two appends will shift writer1 to the back of the rebased index.
  await writer1.put('c', 'c', []) // Two appends will shift writer1 to the back of the rebased index.
  await writer2.put('a', 'a*', [])

  {
    const node = await writer2.get('a')
    t.true(node)
    t.same(node.value, 'a*') // Last one wins
  }

  // There should be one conflict for 'a'
  {
    const conflict = await writer2.get('_conflict/a')
    t.true(conflict)
  }

  // Fix the conflict with another write that causally references both previous writes.
  await writer2.put('a', 'resolved')

  {
    const node = await writer1.get('a')
    t.true(node)
    t.same(node.value, 'resolved')
  }

  // The conflict should be resolved
  {
    const conflict = await writer2.get('_conflict/a')
    t.false(conflict)
  }

  t.end()
})
