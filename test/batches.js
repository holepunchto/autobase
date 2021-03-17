const test = require('tape')
const Omega = require('omega')
const ram = require('random-access-memory')

const { causalValues } = require('./helpers')
const Autobase = require('..')

test('batches array-valued appends using partial input nodes', async t => {
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  await base.append(writerA, ['a0'], await base.latest(writerA))
  await base.append(writerB, ['b0', 'b1'], await base.latest(writerB))
  await base.append(writerC, ['c0', 'c1', 'c2'], await base.latest(writerC))

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

test('batches are always grouped in the causal stream', async t => {

})
