const test = require('tape')
const Hypercore = require('hypercore-x')
const ram = require('random-access-memory')

const { causalValues } = require('../helpers')
const AutobaseCore = require('../../core')

test('linearizes short branches on long branches', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerC, `c${i}`)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
  }

  t.end()
})

test('causal writes', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerC, `c${i}`)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), ['b1', 'b0', 'a0', 'c2', 'c1', 'c0'])
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
  }

  t.end()
})

test('manually specifying links', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB])
  await base.ready()

  await base.append(writerA, 'a0')
  await base.append(writerA, 'a1')
  await base.append(writerB, 'b0', [
    [writerA.key.toString('hex'), 2] // Links to a1
  ])
  await base.append(writerB, 'b1')
  await base.append(writerB, 'b2')

  const output = await causalValues(base)
  t.same(output.map(v => v.value), ['b2', 'b1', 'b0', 'a1', 'a0'])

  t.end()
})

// TODO: Add a test case that links directly to the links of a previous input node.
