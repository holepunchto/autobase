const test = require('tape')
const Omega = require('omega')
const ram = require('random-access-memory')

const { causalValues, indexedValues } = require('./helpers')
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

test('simple rebase', async t => {
  const output = new Omega(ram)
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])

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
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
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
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
    t.same(result.added, 9)
    t.same(result.removed, 6)
    t.same(output.length, 10)
  }

  t.end()
})

test('does not over-truncate', async t => {
  const output = new Omega(ram)
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])

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
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
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
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 3)
    t.same(output.length, 12)
  }

  // Add 1 more record to B -- should not cause any reordering
  await base.append(writerB, 'b2', await base.latest(writerB))

  {
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b2', 'b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 1)
    t.same(result.removed, 0)
    t.same(output.length, 13)
  }

  t.end()
})

test('can cut out a writer', async t => {
  const output = new Omega(ram)
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
  for (let i = 0; i < 5; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  {
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 9)
  }

  // Cut out writer B. Should truncate 3
  const base2 = new Autobase([writerA, writerC])

  {
    const result = await base2.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 2) // a0 and c4 are reindexed
    t.same(result.removed, 4) // a0 and c4 are both popped and reindexed
    t.same(output.length, 7)
  }

  t.end()
})

test('can cut out a writer, causal writes', async t => {
  const output = new Omega(ram)
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
    await base.append(writerB, `b${i}`, await base.latest([writerB, writerA]))
  }
  for (let i = 0; i < 5; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  {
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 9)
  }

  // Cut out writer B. Should truncate 3
  const base2 = new Autobase([writerA, writerC])

  {
    const result = await base2.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 1) // a0 is reindexed
    t.same(result.removed, 3) // a0, b1, and b0 are popped, a0 is reindexed
    t.same(output.length, 7)
  }

  t.end()
})

test('can cut out a writer, causal writes interleaved', async t => {
  const output = new Omega(ram)
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)

  const base = new Autobase([writerA, writerB])

  for (let i = 0; i < 6; i++) {
    if (i % 2) {
      await base.append(writerA, `a${i}`, await base.latest([writerA, writerB]))
    } else {
      await base.append(writerB, `b${i}`, await base.latest([writerA, writerB]))
    }
  }

  {
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a5', 'b4', 'a3', 'b2', 'a1', 'b0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 7)
  }

  const base2 = new Autobase([writerA])

  {
    const output = await causalValues(base2)
    t.same(output.map(v => v.value), ['a5', 'a3', 'a1'])
  }

  {
    const result = await base2.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a5', 'a3', 'a1'])
    t.same(result.added, 3)
    t.same(result.removed, 6)
    t.same(output.length, 4)
  }

  t.end()
})

test('many writers, no causal writes', async t => {
  const NUM_WRITERS = 30
  const NUM_APPENDS = 10

  const output = new Omega(ram)
  const base = new Autobase()
  const writers = []

  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = new Omega(ram)
    await base.addInput(writer)
    writers.push(writer)
    for (let j = 0; j < i; j++) {
      await base.append(writer, `w${i}-${j}`, await base.latest())
    }
  }

  {
    await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.length, (NUM_WRITERS * (NUM_WRITERS + 1)) / 2)
  }

  for (let i = 0; i < NUM_APPENDS; i++) {
    const writer = writers[Math.floor(writers.length / 2)]
    await base.append(writer, `new entry ${i}`, await base.latest(writer))
    const result = await base.localRebase(output)
    console.log('result:', result)
  }

  t.end()
})

test('rebase with mapper', async t => {
  const output = new Omega(ram)
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])

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
    await base.localRebase(output, {
      map: function (indexNode) {
        return Buffer.from(indexNode.node.value.toString('utf-8').toUpperCase(), 'utf-8')
      }
    })
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['A0', 'B1', 'B0', 'C2', 'C1', 'C0'])
  }

  t.end()
})

test('double-rebasing is a no-op', async t => {
  const output = new Omega(ram)
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])

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
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 7)
  }

  {
    const result = await base.localRebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 0)
    t.same(result.removed, 0)
    t.same(output.length, 7)
  }

  t.end()
})

test('remote rebasing selects longest index', async t => {
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

  const output1 = new Omega(ram)
  const output2 = new Omega(ram)
  const output3 = new Omega(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 3; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  await base.localRebase(output1)

  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  await base.localRebase(output2)

  for (let i = 0; i < 1; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }
  await base.localRebase(output3)

  {
    // Should not have to modify output3
    const reader = await base.remoteRebase([output3])
    t.same(reader.added, 0)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  {
    // Should not have to add B and C
    const reader = await base.remoteRebase([output1])
    t.same(reader.added, 3)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  {
    // Should select output2
    const reader = await base.remoteRebase([output1, output2])
    t.same(reader.added, 1)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  {
    // Should select output3
    const reader = await base.remoteRebase([output1, output2, output3])
    t.same(reader.added, 0)
    t.same(reader.removed, 0)
    t.same(reader.index.length, 7)
  }

  t.end()
})
