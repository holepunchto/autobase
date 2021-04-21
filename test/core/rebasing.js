const test = require('tape')
const Hypercore = require('hypercore-x')
const ram = require('random-access-memory')

const { causalValues, indexedValues } = require('../helpers')
const AutobaseCore = require('../../core')

test('simple rebase', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])

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
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 7)
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
    t.same(result.added, 9)
    t.same(result.removed, 6)
    t.same(output.length, 10)
  }

  t.end()
})

test('rebasing with causal writes preserves links', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])

  // Create three causally-linked forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest())
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest())
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerC, `c${i}`, await base.latest())
  }

  const result = await base.rebaseInto(output)
  const indexed = await indexedValues(result.index)
  t.same(indexed.map(v => v.value), ['c2', 'c1', 'c0', 'b1', 'b0', 'a0'])
  t.same(result.added, 6)
  t.same(result.removed, 0)
  t.same(output.length, 7)

  for (let i = 2; i < result.index.length; i++) {
    const prev = await result.index.get(i - 1)
    const node = await result.index.get(i)
    t.true(prev.node.lte(node.node))
  }

  t.end()
})

test('does not over-truncate', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  for (let i = 0; i < 5; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 9)
  }

  // Add 3 more records to A -- should switch fork ordering (A after C)
  for (let i = 1; i < 4; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 3)
    t.same(output.length, 12)
  }

  // Add 1 more record to B -- should not cause any reordering
  await base.append(writerB, 'b2', await base.latest(writerB))

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['b2', 'b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 1)
    t.same(result.removed, 0)
    t.same(output.length, 13)
  }

  t.end()
})

test('can cut out a writer', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  for (let i = 0; i < 5; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 9)
  }

  // Cut out writer B. Should truncate 3
  const base2 = new AutobaseCore([writerA, writerC])

  {
    const result = await base2.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 2) // a0 and c4 are reindexed
    t.same(result.removed, 4) // a0 and c4 are both popped and reindexed
    t.same(output.length, 7)
  }

  t.end()
})

test('can cut out a writer, causal writes', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest([writerB, writerA]))
  }
  for (let i = 0; i < 5; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 9)
  }

  // Cut out writer B. Should truncate 3
  const base2 = new AutobaseCore([writerA, writerC])

  {
    const result = await base2.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 1) // a0 is reindexed
    t.same(result.removed, 3) // a0, b1, and b0 are popped, a0 is reindexed
    t.same(output.length, 7)
  }

  t.end()
})

test('can cut out a writer, causal writes interleaved', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB])

  for (let i = 0; i < 6; i++) {
    if (i % 2) {
      await base.append(writerA, `a${i}`, await base.latest([writerA, writerB]))
    } else {
      await base.append(writerB, `b${i}`, await base.latest([writerA, writerB]))
    }
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a5', 'b4', 'a3', 'b2', 'a1', 'b0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 7)
  }

  const base2 = new AutobaseCore([writerA])

  {
    const output = await causalValues(base2)
    t.same(output.map(v => v.value), ['a5', 'a3', 'a1'])
  }

  {
    const result = await base2.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a5', 'a3', 'a1'])
    t.same(result.added, 3)
    t.same(result.removed, 6)
    t.same(output.length, 4)
  }

  t.end()
})

test('many writers, no causal writes', async t => {
  const NUM_WRITERS = 10
  const NUM_APPENDS = 11

  const output = new Hypercore(ram)
  const writers = []

  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = new Hypercore(ram)
    writers.push(writer)
  }

  const base = new AutobaseCore(writers)
  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = writers[i - 1]
    for (let j = 0; j < i; j++) {
      await base.append(writer, `w${i}-${j}`, await base.latest(writer))
    }
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.length, (NUM_WRITERS * (NUM_WRITERS + 1)) / 2)
  }

  const middleWriter = writers[Math.floor(writers.length / 2)]
  const decodedMiddleWriter = base.decodeInput(middleWriter)

  // Appending to the middle writer NUM_APPEND times should shift it to the front of the index.
  for (let i = 0; i < NUM_APPENDS; i++) {
    await base.append(middleWriter, `new entry ${i}`, await base.latest(middleWriter))
  }

  const { index } = await base.rebaseInto(output, {
    unwrap: true
  })

  for (let i = 1; i < NUM_APPENDS + Math.floor(writers.length / 2) + 1; i++) {
    const latestNode = await index.get(i)
    const val = latestNode.toString('utf-8')
    t.same(val, (await decodedMiddleWriter.get(i)).value.toString('utf-8'))
  }

  t.end()
})

test('double-rebasing is a no-op', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])

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
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 7)
  }

  {
    const result = await base.rebaseInto(output)
    const indexed = await indexedValues(result.index)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 0)
    t.same(result.removed, 0)
    t.same(output.length, 7)
  }

  t.end()
})

test('remote rebasing selects longest index', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const output1 = new Hypercore(ram)
  const output2 = new Hypercore(ram)
  const output3 = new Hypercore(ram)

  const base = new AutobaseCore([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 3; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  await base.rebaseInto(output1)

  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  await base.rebaseInto(output2)

  for (let i = 0; i < 1; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }
  await base.rebaseInto(output3)

  {
    // Should not have to modify output3
    const reader = await base.rebasedView([output3])
    t.same(reader.added, 0)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  {
    // Should not have to add B and C
    const reader = await base.rebasedView([output1])
    t.same(reader.added, 3)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  {
    // Should select output2
    const reader = await base.rebasedView([output1, output2])
    t.same(reader.added, 1)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  {
    // Should select output3
    const reader = await base.rebasedView([output1, output2, output3])
    t.same(reader.added, 0)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  t.end()
})
