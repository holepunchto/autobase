const test = require('tape')
const Hypercore = require('hypercore-x')
const ram = require('random-access-memory')

const { causalValues } = require('../helpers')
const AutobaseCore = require('../../core')

test('batches array-valued appends using partial input nodes', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])
  await base.ready()

  // Create three dependent forks
  await base.append(writerA, ['a0'])
  await base.append(writerB, ['b0', 'b1'], await base.latest(writerA))
  await base.append(writerC, ['c0', 'c1', 'c2'], await base.latest(writerA))

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), ['b1', 'b0', 'c2', 'c1', 'c0', 'a0'])
  }

  // Add 4 more records to A -- should switch fork ordering
  for (let i = 1; i < 5; i++) {
    await base.append(writerA, `a${i}`)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), ['b1', 'b0', 'c2', 'c1', 'c0', 'a4', 'a3', 'a2', 'a1', 'a0'])
  }

  t.end()
})
