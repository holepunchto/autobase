const test = require('tape')
const Omega = require('omega')
const ram = require('random-access-memory')

const { causalValues } = require('./helpers')
const Autobase = require('..')

test('linearizes short branches on long branches', async t => {
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
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
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest([writerA, writerB]))
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
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
