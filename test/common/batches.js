const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, causalValues } = require('../helpers')
const Autobase = require('../..')

test('batches - array-valued appends using partial input nodes', async t => {
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

test('batches - appends produce correct operation values', async t => {
  const writerA = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA],
    localInput: writerA
  })

  await base.append('a0')
  await base.append(['a1', 'a2'])
  await base.append(['a3', 'a4', 'a5'])

  const expected = [1, 3, 3, 6, 6, 6]
  const actual = []

  for await (const node of base.createCausalStream()) {
    actual.push(node.operations)
    t.same(node.operations, expected.pop())
  }
  t.same(expected.length, 0)

  t.end()
})

test('batches - autobee-style batching gives correct operations', async t => {
  const writerA = new Hypercore(ram)
  const output = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA],
    localInput: writerA,
    localOutput: output
  })
  base.start({
    apply: async (view, batch) => {
      // Two batched nodes regardless of the input batch size
      await view.append(Buffer.from('a'))
      await view.append(Buffer.from('b'))
    }
  })

  // Should trigger 4 output blocks for the 3 input operations
  await base.append('a0')
  await base.view.update()

  await base.append(['a1', 'a2'])
  await base.view.update()

  t.same(output.length, 4)
  t.same(base.view.length, 4)

  const expected = [1, 1, 3, 3]
  for (let i = 0; i < base.view.length; i++) {
    const node = await base.view.get(i)
    t.same(node.operations, expected[i])
  }

  t.end()
})

test('batches - batches store compressed clocks correctly', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const output = new Hypercore(ram)

  let base = new Autobase({
    inputs: [writerA, writerB],
    localOutput: output,
    autostart: true
  })

  await base.append(['a0', 'a1'], await base.latest(), writerA)
  await base.append(['b0', 'b1', 'b2'], await base.latest(), writerB)
  await base.view.update()

  // Recreate the Autobase so that the key compressors are uninitialized
  base = new Autobase({
    inputs: [writerA, writerB],
    localOutput: output,
    autostart: true
  })
  await base.ready()

  // Can independently load the first block of the second batch
  const b0 = await base.localOutput.get(2)

  t.same(b0.change, writerB.key)
  t.same(b0.clock.size, 2) // The clock is the full batch clock

  t.end()
})
