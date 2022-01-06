const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, linearizedValues } = require('./helpers')
const Autobase = require('../')

test('linearizing - three independent forks', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  {
    const view = base.linearize(output)
    const outputNodes = await linearizedValues(view)

    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 6)
    t.same(view.status.removed, 0)
    t.same(output.length, 6)
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const view = base.linearize(output)
    const outputNodes = await linearizedValues(view)

    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
    t.same(view.status.added, 9)
    t.same(view.status.removed, 6)
    t.same(output.length, 9)
  }

  t.end()
})

test('linearizing - default output', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC], {
    outputs: output
  })
  const view = base.linearize()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 6)
    t.same(view.status.removed, 0)
    t.same(output.length, 6)
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, [], writerA)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
    t.same(view.status.added, 9)
    t.same(view.status.removed, 6)
    t.same(output.length, 9)
  }

  t.end()
})

test('linearizing - causal writes preserve clock', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])

  // Create three causally-linked forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, writerC)
  }

  const view = base.linearize(output)
  const outputNodes = await linearizedValues(view)

  t.same(outputNodes.map(v => v.value), bufferize(['c2', 'c1', 'c0', 'b1', 'b0', 'a0']))
  t.same(view.status.added, 6)
  t.same(view.status.removed, 0)
  t.same(output.length, 6)

  for (let i = 1; i < view.length; i++) {
    const prev = await view.get(i - 1)
    const node = await view.get(i)
    t.true(prev.lte(node))
  }

  t.end()
})

test('linearizing - does not over-truncate', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC], { outputs: output })
  const view = base.linearize()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 5; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 8)
    t.same(view.status.removed, 0)
    t.same(output.length, 8)
  }

  // Add 3 more records to A -- should switch fork ordering (A after C)
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, [], writerA)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 6)
    t.same(view.status.removed, 3)
    t.same(output.length, 11)
  }

  // Add 1 more record to B -- should not cause any reordering
  await base.append('b2', [], writerB)

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['b2', 'b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 1)
    t.same(view.status.removed, 0)
    t.same(output.length, 12)
  }

  t.end()
})

test('linearizing - can cut out a writer', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC], { outputs: output })
  const view = base.linearize()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 5; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 8)
    t.same(view.status.removed, 0)
    t.same(output.length, 8)
  }

  // Cut out writer B. Should truncate 3
  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 1) // a0 is reindexed
    t.same(view.status.removed, 3) // a0 is popped and reindexed
    t.same(output.length, 6)
  }

  t.end()
})

test('linearizing - can cut out a writer from the back', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC], { outputs: output })
  const view = base.linearize()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 5; i++) {
    await base.append(`b${i}`, [], writerB)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b4', 'b3', 'b2', 'b1', 'b0']))
    t.same(view.status.added, 6)
    t.same(view.status.removed, 0)
    t.same(output.length, 6)
  }

  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0']))
    t.same(view.status.added, 1) // a0 is reindexed
    t.same(view.status.removed, 6) // a0 is popped and reindexed
    t.same(output.length, 1)
  }

  t.end()
})

test('linearizing - can cut out a writer from the front', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC], { outputs: output })
  const view = base.linearize()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 5; i++) {
    await base.append(`b${i}`, [], writerB)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b4', 'b3', 'b2', 'b1', 'b0']))
    t.same(view.status.added, 6)
    t.same(view.status.removed, 0)
    t.same(output.length, 6)
  }

  await base.removeInput(writerA)

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['b4', 'b3', 'b2', 'b1', 'b0']))
    t.same(view.status.added, 0) // a0 is removed
    t.same(view.status.removed, 1) // a0 is removed
    t.same(output.length, 5)
  }

  t.end()
})

test('linearizing - can cut out a writer, causal writes', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC], { outputs: output })
  const view = base.linearize()

  // Create three causally-linked forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest([writerB, writerA]), writerB)
  }
  for (let i = 0; i < 5; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 8)
    t.same(view.status.removed, 0)
    t.same(output.length, 8)
  }

  // Cut out writer B. Should truncate 3
  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 0) // b1 and b0 are removed
    t.same(view.status.removed, 2) // b1 and b0 are removed
    t.same(output.length, 6)
  }

  t.end()
})

test('linearizing - can cut out a writer, causal writes interleaved', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new Autobase([writerA, writerB], { outputs: output })
  const view = base.linearize()

  for (let i = 0; i < 6; i++) {
    if (i % 2) {
      await base.append(`a${i}`, await base.latest(), writerA)
    } else {
      await base.append(`b${i}`, await base.latest(), writerB)
    }
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a5', 'b4', 'a3', 'b2', 'a1', 'b0']))
    t.same(view.status.added, 6)
    t.same(view.status.removed, 0)
    t.same(output.length, 6)
  }

  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a5', 'a3', 'a1']))
    t.same(view.status.added, 3)
    t.same(view.status.removed, 6)
    t.same(output.length, 3)
  }

  t.end()
})

test('linearizing - many writers, no causal writes', async t => {
  const NUM_WRITERS = 10
  const NUM_APPENDS = 11

  const output = new Hypercore(ram)
  const writers = []

  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = new Hypercore(ram)
    writers.push(writer)
  }

  const base = new Autobase(writers)
  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = writers[i - 1]
    for (let j = 0; j < i; j++) {
      await base.append(`w${i}-${j}`, [], writer)
    }
  }

  {
    const view = base.linearize(output)
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.length, (NUM_WRITERS * (NUM_WRITERS + 1)) / 2)
  }

  const middleWriter = writers[Math.floor(writers.length / 2)]
  const decodedMiddleWriter = base._decodeInput(middleWriter)

  // Appending to the middle writer NUM_APPEND times should shift it to the back of the index.
  for (let i = 0; i < NUM_APPENDS; i++) {
    await base.append(`new entry ${i}`, [], middleWriter)
  }

  const view = base.linearize(output, {
    unwrap: true
  })
  await view.update()

  for (let i = 0; i < NUM_APPENDS + Math.floor(writers.length / 2); i++) {
    const latestNode = await view.get(i)
    const val = latestNode.toString('utf-8')
    t.same(val, (await decodedMiddleWriter.get(i)).value.toString('utf-8'))
  }

  t.end()
})

test('linearizing - double-linearizing is a no-op', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC], { outputs: output })
  const view = base.linearize()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 6)
    t.same(view.status.removed, 0)
    t.same(output.length, 6)
  }

  {
    const outputNodes = await linearizedValues(view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(view.status.added, 0)
    t.same(view.status.removed, 0)
    t.same(output.length, 6)
  }

  t.end()
})

test('linearizing - selects longest remote output', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const output1 = new Hypercore(ram)
  const output2 = new Hypercore(ram)
  const output3 = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 3; i++) {
    await base.append(`a${i}`, [], writerA)
  }

  {
    const view = base.linearize(output1)
    await view.update()
  }

  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }

  {
    const view = base.linearize(output2)
    await view.update()
  }

  for (let i = 0; i < 1; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  {
    const view = base.linearize(output3)
    await view.update()
  }

  {
    // Should not have to modify output3
    const view = base.linearize([output3], { autocommit: false })
    await view.update()
    t.same(view.status.added, 0)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  {
    // Should not have to add B and C
    const view = base.linearize([output1], { autocommit: false })
    await view.update()
    t.same(view.status.added, 3)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  {
    // Should select output2
    const view = base.linearize([output1, output2], { autocommit: false })
    await view.update()
    t.same(view.status.added, 1)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  {
    // Should select output3
    const view = base.linearize([output1, output2, output3], { autocommit: false })
    await view.update()
    t.same(view.status.added, 0)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  t.end()
})

test('linearizing - can dynamically add/remove default outputs', async function (t) {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const output1 = new Hypercore(ram)
  const output2 = new Hypercore(ram)
  const output3 = new Hypercore(ram)

  const base1 = new Autobase([writerA, writerB, writerC])
  await base1.ready()

  // Create three independent forks, and rebase them into separate indexes
  for (let i = 0; i < 3; i++) {
    await base1.append(`a${i}`, [], writerA)
  }

  {
    const view = base1.linearize(output1)
    await view.update()
  }

  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, [], writerB)
  }

  {
    const view = base1.linearize(output2)
    await view.update()
  }

  for (let i = 0; i < 1; i++) {
    await base1.append(`c${i}`, [], writerC)
  }

  {
    const view = base1.linearize(output3)
    await view.update()
  }

  const base2 = new Autobase([writerA, writerB, writerC])

  {
    const view = base2.linearize({ autocommit: false })
    await view.update()
    t.same(view.status.added, 6)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  await base2.addDefaultOutput(output1)

  {
    const view = base2.linearize({ autocommit: false })
    await view.update()
    t.same(view.status.added, 3)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  await base2.addDefaultOutput(output2)

  {
    const view = base2.linearize({ autocommit: false })
    await view.update()
    t.same(view.status.added, 1)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  await base2.addDefaultOutput(output3)

  {
    // Should select output3
    const view = base2.linearize({ autocommit: false })
    await view.update()
    t.same(view.status.added, 0)
    t.same(view.status.removed, 0)
    t.same(view.length, 6)
  }

  t.end()
})

test('linearizing - linearize operations are debounced', async function (t) {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  const view = base.linearize(output)
  await Promise.all([
    view.update(),
    view.update(),
    view.update(),
    view.update()
  ])

  const outputNodes = []
  for (let i = 0; i < view.length; i++) {
    outputNodes.push(await view.get(i))
  }
  outputNodes.reverse()

  t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
  t.same(output.length, 6)

  t.end()
})
