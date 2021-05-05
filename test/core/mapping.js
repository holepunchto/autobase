const test = require('tape')
const Hypercore = require('hypercore-x')
const ram = require('random-access-memory')

const { bufferize, indexedValues } = require('../helpers')
const Autobase = require('../..')

test('map with stateless mapper', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerB), writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  const rebased = base.createRebasedIndex(output, {
    apply (batch) {
      batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
      return rebased.append(batch)
    }
  })
  const indexed = await indexedValues(rebased)
  t.same(indexed.map(v => v.value), bufferize(['A0', 'B1', 'B0', 'C2', 'C1', 'C0']))

  t.end()
})

test('mapping into batches yields the correct clock on reads', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  await base.append(['a0'], [], writerA)
  await base.append(['b0', 'b1'], [], writerB)
  await base.append(['c0', 'c1', 'c2'], [], writerC)

  const rebased = base.createRebasedIndex(output, {
    apply (batch) {
      batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
      return rebased.append(batch)
    }
  })
  const indexed = await indexedValues(rebased)
  t.same(indexed.map(v => v.value), bufferize(['A0', 'B1', 'B0', 'C2', 'C1', 'C0']))

  t.end()
})

// TODO: Do we still need a state reinitialization test with the StableIndexView?
test.skip('rebase with stateful mapper, reinitializes state correctly', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  for (let i = 0; i < 5; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  let reinitialized = 0
  const reducer = {
    init: function () {
      reinitialized++
    },
    reduce: function (indexNode) {
      return Buffer.from(indexNode.node.value.toString('utf-8').toUpperCase(), 'utf-8')
    }
  }

  {
    await base.rebaseInto(output, {
      init: reducer.init,
      reduce: reducer.reduce
    })
    const indexed = await indexedValues(base, output)
    t.same(indexed.map(v => v.value), ['A0', 'B2', 'B1', 'B0', 'C4', 'C3', 'C2', 'C1', 'C0'])
    t.same(reinitialized, 1)
  }

  // This should not trigger any reordering, so init should not be called again.
  await base.append(writerA, 'a1', await base.latest(writerA))

  {
    await base.rebaseInto(output, {
      init: reducer.init,
      reduce: reducer.reduce
    })
    const indexed = await indexedValues(base, output)
    t.same(indexed.map(v => v.value), ['A1', 'A0', 'B2', 'B1', 'B0', 'C4', 'C3', 'C2', 'C1', 'C0'])
    t.same(reinitialized, 1)
  }

  // This should reorder A to come before B, triggering a reordering, so init should be called again.
  await base.append(writerA, 'a2', await base.latest(writerA))
  await base.append(writerA, 'a3', await base.latest(writerA))

  {
    await base.rebaseInto(output, {
      init: reducer.init,
      reduce: reducer.reduce
    })
    const indexed = await indexedValues(base, output)
    t.same(indexed.map(v => v.value), ['B2', 'B1', 'B0', 'A3', 'A2', 'A1', 'A0', 'C4', 'C3', 'C2', 'C1', 'C0'])
    t.same(reinitialized, 2)
  }

  t.end()
})

test.skip('stateful mapper', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  for (let i = 0; i < 5; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  let state = 0
  function statefulMapper (indexNode) {
    return Buffer.from(indexNode.node.value.toString('utf-8').toUpperCase() + '-' + state++, 'utf-8')
  }

  await base.rebaseInto(output, {
    map: statefulMapper
  })
  const indexed = await indexedValues(base, output)
  console.log('indexed:', indexed)

  t.end()
})
