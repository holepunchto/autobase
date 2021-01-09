const test = require('tape')
const Corestore = require('corestore')
const Omega = require('omega')
const ram = require('random-access-memory')
const { toPromises } = require('hypercore-promisifier')

const { causalValues, indexedValues } = require('./helpers')
const Autobase = require('..')

test('linearizes short branches on long branches', async t => {
  const store = new Corestore(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
  const store = new Corestore(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 6)
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }

  {
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
    t.same(result.added, 9)
    t.same(result.removed, 6)
    t.same(output.length, 9)
  }

  t.end()
})

test('does not over-truncate', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 8)
  }

  // Add 3 more records to A -- should switch fork ordering (A after C)
  for (let i = 1; i < 4; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }

  {
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 3)
    t.same(output.length, 11)
  }

  // Add 1 more record to B -- should not cause any reordering
  await base.append(writerB, 'b2', await base.latest(writerB))

  {
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b2', 'b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 1)
    t.same(result.removed, 0)
    t.same(output.length, 12)
  }

  t.end()
})

test('can cut out a writer', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 8)
  }

  // Cut out writer B. Should truncate 3
  const base2 = new Autobase([writerA, writerC])

  {
    const result = await base2.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 2) // a0 and c4 are reindexed
    t.same(result.removed, 4) // a0 and c4 are both popped and reindexed
    t.same(output.length, 6)
  }

  t.end()
})

test('can cut out a writer, causal writes', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['b1', 'b0', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 8)
    t.same(result.removed, 0)
    t.same(output.length, 8)
  }

  // Cut out writer B. Should truncate 3
  const base2 = new Autobase([writerA, writerC])

  {
    const result = await base2.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
    t.same(result.added, 1) // a0 is reindexed
    t.same(result.removed, 3) // a0, b1, and b0 are popped, a0 is reindexed
    t.same(output.length, 6)
  }

  t.end()
})

test('can cut out a writer, causal writes interleaved', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))

  const base = new Autobase([writerA, writerB])

  for (let i = 0; i < 6; i++) {
    if (i % 2) {
      await base.append(writerA, `a${i}`, await base.latest([writerA, writerB]))
    } else {
      await base.append(writerB, `b${i}`, await base.latest([writerA, writerB]))
    }
  }

  {
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a5', 'b4', 'a3', 'b2', 'a1', 'b0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 6)
  }

  const base2 = new Autobase([writerA])

  {
    const output = await causalValues(base2)
    t.same(output.map(v => v.value), ['a5', 'a3', 'a1'])
  }

  {
    const result = await base2.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a5', 'a3', 'a1'])
    t.same(result.added, 3)
    t.same(result.removed, 6)
    t.same(output.length, 3)
  }

  t.end()
})

test('many writers, no causal writes', async t => {
  const NUM_WRITERS = 30
  const NUM_APPENDS = 10

  const store = new Corestore(ram)
  const output = new Omega(ram)
  const base = new Autobase()
  const writers = []

  for (let i = 1; i < NUM_WRITERS + 1; i++) {
    const writer = toPromises(store.get({ name: `writer-${i}` }))
    await base.addInput(writer)
    writers.push(writer)
    for (let j = 0; j < i; j++) {
      await base.append(writer, `w${i}-${j}`, await base.latest())
    }
  }

  {
    await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.length, (NUM_WRITERS * (NUM_WRITERS + 1)) / 2)
  }

  for (let i = 0; i < NUM_APPENDS; i++) {
    const writer = writers[Math.floor(writers.length / 2)]
    await base.append(writer, `new entry ${i}`, await base.latest(writer))
    const result = await base.rebase(output)
    console.log('result:', result)
  }

  t.end()
})

test('rebase with mapper', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
    await base.rebase(output, {
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
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

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
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 6)
    t.same(result.removed, 0)
    t.same(output.length, 6)
  }

  {
    const result = await base.rebase(output)
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
    t.same(result.added, 0)
    t.same(result.removed, 0)
    t.same(output.length, 6)
  }

  t.end()
})
