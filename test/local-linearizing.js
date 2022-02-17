const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, linearizedValues } = require('./helpers')
const Autobase = require('../')

test('local linearizing - three independent forks', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
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

test('local linearizing - causal writes preserve clock', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
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
    autostart: true
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

test('local linearizing - can cut out a writer', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
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

test('local linearizing - can cut out a writer from the back', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
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

test('local linearizing - can cut out a writer from the front', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
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

test('local linearizing - can cut out a writer, causal writes', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
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

test('local linearizing - can cut out a writer, causal writes interleaved', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB],
    localOutput: output,
    autostart: true
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
    autostart: true
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
    autostart: true
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
    localOutput: output1
  })
  const base2 = new Autobase({
    inputs,
    localOutput: output2
  })
  const base3 = new Autobase({
    inputs,
    localOutput: output3
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
    outputs: [output1]
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
  t.same(base4.view.status.nodes.length, 0) // Should switch to output3
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
    autostart: true
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
