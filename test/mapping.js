const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, linearizedValues } = require('./helpers')
const Autobase = require('../')

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

  const view = base.linearize(output, {
    apply (batch) {
      batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
      return view.append(batch)
    }
  })
  const outputNodes = await linearizedValues(view)
  t.same(outputNodes.map(v => v.value), bufferize(['A0', 'B1', 'B0', 'C2', 'C1', 'C0']))

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

  const view = base.linearize(output, {
    apply (batch) {
      batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
      return view.append(batch)
    }
  })
  const outputNodes = await linearizedValues(view)
  t.same(outputNodes.map(v => v.value), bufferize(['A0', 'B1', 'B0', 'C2', 'C1', 'C0']))

  t.end()
})
