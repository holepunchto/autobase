const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, causalValues } = require('./helpers')
const Autobase = require('../')

test('batches array-valued appends using partial input nodes', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC]
  })

  // Create three dependent forks
  await base.append(['a0'], await base.latest(writerA), writerA)
  await base.append(['b0', 'b1'], await base.latest(writerA), writerB)
  await base.append(['c0', 'c1', 'c2'], await base.latest(writerA), writerC)

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a0']))
  }

  // Add 4 more records to A -- should switch fork ordering
  for (let i = 1; i < 5; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a4', 'a3', 'a2', 'a1', 'a0']))
  }

  t.end()
})

test('batched appends produce correct operation values', async t => {
  const writerA = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA],
    localInput: writerA
  })

  await base.append('a0')
  await base.append(['a1', 'a2'])
  await base.append(['a3', 'a4', 'a5'])

  const expected = [1, 2, 3, 4, 5, 6]

  for await (const node of base.createCausalStream()) {
    t.same(node.operations, expected.pop())
  }
  t.same(expected.length, 0)

  t.end()
})
