const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, linearizedValues } = require('../helpers')
const { decodeKeys } = require('../../lib/nodes/messages')
const Autobase = require('../..')

test('local linearizing - three independent forks', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
    t.same(base.view.status.appended, 9)
    t.same(base.view.status.truncated, 6)
    t.same(output.length, 9)
  }

  t.end()
})

test('local linearizing - three independent forks, two truncations', async t => {
  t.plan(14)

  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })
  const expectedTruncations = [5, 0]
  base.view.on('truncate', length => {
    t.same(length, expectedTruncations.pop())
  })

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
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  // Add 4 more records to A -- should switch fork ordering
  for (let i = 1; i < 5; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a4', 'a3', 'a2', 'a1', 'a0']))
    t.same(base.view.status.appended, 10)
    t.same(base.view.status.truncated, 6)
    t.same(output.length, 10)
  }

  // Add 2 more records to B -- should switch fork ordering
  for (let i = 2; i < 4; i++) {
    await base.append(`b${i}`, await base.latest(writerB), writerB)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['c2', 'c1', 'c0', 'b3', 'b2', 'b1', 'b0', 'a4', 'a3', 'a2', 'a1', 'a0']))
    t.same(base.view.status.appended, 7)
    t.same(base.view.status.truncated, 5)
    t.same(output.length, 12)
  }

  t.end()
})

test('local linearizing - causal writes preserve clock', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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

  const outputNodes = await linearizedValues(base.view)

  t.same(outputNodes.map(v => v.value), bufferize(['c2', 'c1', 'c0', 'b1', 'b0', 'a0']))
  t.same(base.view.status.appended, 6)
  t.same(base.view.status.truncated, 0)
  t.same(output.length, 6)

  for (let i = 1; i < base.view.length; i++) {
    const prev = await base.view.get(i - 1)
    const node = await base.view.get(i)
    t.false(node.lt(prev))
  }

  t.end()
})

test('local linearizing - does not over-truncate', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 8)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 8)
  }

  // Add 3 more records to A -- should switch fork ordering (A after C)
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, [], writerA)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 3)
    t.same(output.length, 11)
  }

  // Add 1 more record to B -- should not cause any reordering
  await base.append('b2', [], writerB)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b2', 'b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 1)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 12)
  }

  t.end()
})

test('local linearizing - can purge', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 8)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 8)
  }

  // Cut out writer B. Should truncate 3
  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 1) // a0 is reindexed
    t.same(base.view.status.truncated, 3) // a0 is popped and reindexed
    t.same(output.length, 6)
  }

  t.end()
})

test('local linearizing - can purge from the back', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 5; i++) {
    await base.append(`b${i}`, [], writerB)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b4', 'b3', 'b2', 'b1', 'b0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0']))
    t.same(base.view.status.appended, 1) // a0 is reindexed
    t.same(base.view.status.truncated, 6) // a0 is popped and reindexed
    t.same(output.length, 1)
  }

  t.end()
})

test('local linearizing - can purge from the front', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 5; i++) {
    await base.append(`b${i}`, [], writerB)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b4', 'b3', 'b2', 'b1', 'b0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  await base.removeInput(writerA)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b4', 'b3', 'b2', 'b1', 'b0']))
    t.same(base.view.status.appended, 0) // a0 is removed
    t.same(base.view.status.truncated, 1) // a0 is removed
    t.same(output.length, 5)
  }

  t.end()
})

test('local linearizing - can purge, causal writes', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 8)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 8)
  }

  // Cut out writer B. Should truncate 3
  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'c4', 'c3', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 0) // b1 and b0 are removed
    t.same(base.view.status.truncated, 2) // b1 and b0 are removed
    t.same(output.length, 6)
  }

  t.end()
})

// TODO: When causal writes are interleaved, nodes with unsatisfied links should
// not be yielded after the writer is removed, unless that removal is clear at
// autobase creation time. Requires a fix.
test.skip('local linearizing - can purge, causal writes interleaved', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

  for (let i = 0; i < 6; i++) {
    if (i % 2) {
      await base.append(`a${i}`, await base.latest(), writerA)
    } else {
      await base.append(`b${i}`, await base.latest(), writerB)
    }
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a5', 'b4', 'a3', 'b2', 'a1', 'b0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  await base.removeInput(writerB)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a5', 'a3', 'a1']))
    t.same(base.view.status.appended, 3)
    t.same(base.view.status.truncated, 6)
    t.same(output.length, 3)
  }

  t.end()
})

test('local linearizing - many writers, no causal writes', async t => {
  const NUM_WRITERS = 10
  const NUM_APPENDS = 11

  const output = new Hypercore(ram)
  const writers = []

  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = new Hypercore(ram)
    writers.push(writer)
  }
  const middleWriter = writers[Math.floor(writers.length / 2)]

  const base = new Autobase({
    inputs: writers,
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })
  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = writers[i - 1]
    for (let j = 0; j < i; j++) {
      await base.append(`w${i}-${j}`, [], writer)
    }
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.length, (NUM_WRITERS * (NUM_WRITERS + 1)) / 2)
  }

  // Appending to the middle writer NUM_APPEND times should shift it to the back of the index.
  for (let i = 0; i < NUM_APPENDS; i++) {
    await base.append(`new entry ${i}`, [], middleWriter)
  }

  await base.view.update()

  for (let i = 0; i < NUM_APPENDS + Math.floor(writers.length / 2); i++) {
    const latestNode = await base.view.get(i)
    const val = latestNode.value.toString()
    t.same(val, (await base._getInputNode(middleWriter, i)).value.toString())
  }

  t.end()
})

test('local linearizing - double-linearizing is a no-op', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 0)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  t.end()
})

test('local linearizing - can dynamically add/remove default outputs', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const output1 = new Hypercore(ram)
  const output2 = new Hypercore(ram)
  const output3 = new Hypercore(ram)

  const inputs = [writerA, writerB, writerC]
  const base1 = new Autobase({
    inputs,
    localOutput: output1,
    eagerUpdate: false
  })
  const base2 = new Autobase({
    inputs,
    localOutput: output2,
    eagerUpdate: false
  })
  const base3 = new Autobase({
    inputs,
    localOutput: output3,
    eagerUpdate: false
  })
  base1.start()
  base2.start()
  base3.start()

  // Create three independent forks, and linearize them into separate outputs
  for (let i = 0; i < 3; i++) {
    await base1.append(`a${i}`, [], writerA)
  }

  await base1.view.update()

  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, [], writerB)
  }

  await base2.view.update()

  for (let i = 0; i < 1; i++) {
    await base1.append(`c${i}`, [], writerC)
  }

  await base3.view.update()

  const base4 = new Autobase({
    inputs,
    outputs: [output1],
    eagerUpdate: false
  })
  base4.start()

  await base4.view.update()
  t.same(base4.view.status.appended, 3)
  t.same(base4.view.status.truncated, 0)
  t.same(base4.view.length, 6)

  await base4.addOutput(output2)

  await base4.view.update()
  t.same(base4.view.status.appended, 0)
  t.same(base4.view.status.truncated, 0)
  t.same(base4.view.length, 6)

  await base4.addOutput(output3)

  await base4.view.update()
  t.same(base4.view._core.nodes.length, 0) // Should switch to output3
  t.same(base4.view.status.appended, 0)
  t.same(base4.view.status.truncated, 0)
  t.same(base4.view.length, 6)

  t.end()
})

test('local linearizing - can dynamically add a default output', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    autostart: true,
    eagerUpdate: false
  })
  base.localOutput = output

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
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  t.end()
})

test('local linearizing - truncation does not break key compression', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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
    // A's key initially should be stored in the 6th block
    await base.view.update()
    const keys = decodeKeys(await output.get(5))
    t.same(keys.length, 1)
    t.same(keys[0], writerA.key)
  }

  // Add 3 more records to A -- should switch fork ordering
  // A's key should be re-recorded into the 0th block after truncation
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    // A's key should now be stored in the 0th block
    await base.view.update()
    const keys = decodeKeys(await output.get(0))
    t.same(keys.length, 1)
    t.same(keys[0], writerA.key)
  }

  t.end()
})

test('local linearizing - creating two branch snapshots with a common update clones core snapshots', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

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

  const snapshot1 = base.view.snapshot()
  await base.view.update()
  const snapshot2 = base.view.snapshot()
  const snapshot3 = base.view.snapshot()

  {
    const outputNodes = await linearizedValues(snapshot1, { update: false })
    t.same(outputNodes.map(v => v.value), bufferize([]))
    t.same(output.length, 6)
  }

  await snapshot1.update()

  {
    const outputNodes = await linearizedValues(snapshot1, { update: false })
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(output.length, 6)
  }

  {
    const outputNodes = await linearizedValues(snapshot2, { update: false })
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(output.length, 6)
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  await snapshot3.update()

  {
    const outputNodes = await linearizedValues(snapshot3, { update: false })
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
    t.same(output.length, 9)
  }

  {
    const outputNodes = await linearizedValues(snapshot2, { update: false })
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(output.length, 9)
  }

  t.end()
})

test('local linearizing - consistent reads with a pre-update snapshot', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

  await base.append('a0', await base.latest(), writerA)
  await base.append('b0', await base.latest(), writerB)
  await base.view.update()

  const snapshot = base.view.snapshot().unwrap()
  t.same(snapshot.length, 2)

  const nodes = []
  for (let i = 0; i < snapshot.length; i++) {
    nodes.push(await snapshot.get(i))
  }
  t.same(nodes, bufferize(['a0', 'b0']))

  t.end()
})
