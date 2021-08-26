const test = require('tape')
const Hypercore = require('hypercore-x')
const ram = require('random-access-memory')

const { bufferize, causalValues } = require('../helpers')
const Autobase = require('../..')

test('linearizes short branches on long branches', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

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

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
  }

  t.end()
})

test('causal writes', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerA), writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'a0', 'c2', 'c1', 'c0']))
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
  }

  t.end()
})

test('manually specifying clocks', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new Autobase([writerA, writerB])
  await base.ready()

  await base.append('a0', await base.latest(writerA), writerA)
  await base.append('a1', await base.latest(writerA), writerA)
  await base.append('b0', [
    [writerA.key.toString('hex'), 2] // Links to a1
  ], writerB)
  await base.append('b1', await base.latest(writerB), writerB)
  await base.append('b2', await base.latest(writerB), writerB)

  const output = await causalValues(base)
  t.same(output.map(v => v.value), bufferize(['b2', 'b1', 'b0', 'a1', 'a0']))

  t.end()
})

test('supports a default writer and default latest clocks', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase([writerA, writerB], { defaultWriter: writerA })
  const base2 = new Autobase([writerA, writerB], { defaultWriter: writerB })
  await base1.ready()
  await base2.ready()

  await base1.append('a0')
  await base1.append('a1')
  await base2.append('b0')
  await base1.append('a2')
  await base2.append('b1')
  await base1.append('a3')

  const output = await causalValues(base1)
  t.same(output.map(v => v.value), bufferize(['a3', 'b1', 'a2', 'b0', 'a1', 'a0']))

  t.end()
})

test('dynamically adding/removing inputs', async t => {
  const writerA = new Hypercore(ram)

  const base = new Autobase([writerA])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['a0']))
  }

  const writerB = new Hypercore(ram)
  await base.addInput(writerB)

  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerB), writerB)
  }
  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['a0', 'b1', 'b0']))
  }

  const writerC = new Hypercore(ram)
  await base.addInput(writerC)

  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }
  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
  }

  await base.removeInput(writerC)

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'a3', 'a2', 'a1', 'a0']))
  }

  t.end()
})

test('dynamically adding inputs does not alter existing causal streams', async t => {
  const writerA = new Hypercore(ram)

  const base = new Autobase([writerA])
  await base.ready()

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['a0']))
  }

  const writerB = new Hypercore(ram)
  await base.addInput(writerB)

  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerB), writerB)
  }

  const output = []
  const stream = base.createCausalStream()
  await new Promise(resolve => stream.once('readable', resolve)) // Once the stream is opened, its heads are locked

  const writerC = new Hypercore(ram)
  await base.addInput(writerC)

  for await (const node of stream) { // The stream should not have writerC's nodes
    output.push(node)
  }
  t.same(output.map(v => v.value), bufferize(['a0', 'b1', 'b0']))

  t.end()
})

// TODO: Add a test case that links directly to the links of a previous input node.
