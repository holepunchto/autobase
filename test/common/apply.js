const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, linearizedValues } = require('../helpers')
const Autobase = require('../..')

test('applying - apply with one-to-one apply function', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    eagerUpdate: false
  })
  base.start({
    apply (view, batch) {
      batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
      return view.append(batch)
    }
  })

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

  const outputNodes = await linearizedValues(base.view)
  t.same(outputNodes.map(v => v.value), bufferize(['A0', 'B1', 'B0', 'C2', 'C1', 'C0']))

  t.end()
})

test('applying - applying into batches yields the correct clock on reads', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    eagerUpdate: false
  })
  base.start({
    apply (view, batch) {
      batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
      return view.append(batch)
    }
  })

  // Create three independent forks
  await base.append(['a0'], [], writerA)
  await base.append(['b0', 'b1'], [], writerB)
  await base.append(['c0', 'c1', 'c2'], [], writerC)

  const outputNodes = await linearizedValues(base.view)
  t.same(outputNodes.map(v => v.value), bufferize(['A0', 'B1', 'B0', 'C2', 'C1', 'C0']))

  t.end()
})

test('applying - one-to-many apply with reordering, local output', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    eagerUpdate: false
  })
  base.start({
    async apply (view, batch) {
      for (const node of batch) {
        await view.append(Buffer.from(node.value.toString() + '-0'))
        await view.append(Buffer.from(node.value.toString() + '-1'))
      }
    }
  })

  // Create two independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerB), writerB)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0-1', 'a0-0', 'b1-1', 'b1-0', 'b0-1', 'b0-0']))
  }

  // Shift A's fork to the back
  await base.append('a1', await base.latest(writerA), writerA)
  await base.append('a2', await base.latest(writerA), writerA)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1-1', 'b1-0', 'b0-1', 'b0-0', 'a2-1', 'a2-0', 'a1-1', 'a1-0', 'a0-1', 'a0-0']))
  }

  t.end()
})

test('applying - one-to-many apply with reordering, remote output up-to-date', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const applyFunction = async (view, batch) => {
    for (const node of batch) {
      await view.append(Buffer.from(node.value.toString() + '-0'))
      await view.append(Buffer.from(node.value.toString() + '-1'))
    }
  }

  const base1 = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    apply: applyFunction,
    eagerUpdate: false
  })
  const base2 = new Autobase({
    inputs: [writerA, writerB, writerC],
    outputs: [output],
    apply: applyFunction,
    eagerUpdate: false
  })

  // Create two independent forks
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerB), writerB)
  }
  await base1.view.update()

  {
    const outputNodes = await linearizedValues(base2.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0-1', 'a0-0', 'b1-1', 'b1-0', 'b0-1', 'b0-0']))
  }

  // Shift A's fork to the back
  await base1.append('a1', await base1.latest(writerA), writerA)
  await base1.append('a2', await base1.latest(writerA), writerA)
  await base1.view.update()

  {
    const outputNodes = await linearizedValues(base2.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1-1', 'b1-0', 'b0-1', 'b0-0', 'a2-1', 'a2-0', 'a1-1', 'a1-0', 'a0-1', 'a0-0']))
  }

  t.end()
})

test('applying - one-to-many apply with reordering, remote output out-of-date', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const applyValues = (values) => values.flatMap(v => [Buffer.from(v + '-0'), Buffer.from(v + '-1')])
  const applyFunction = async (view, batch) => {
    const values = batch.map(n => n.value.toString())
    const vals = applyValues(values)
    for (let i = vals.length - 1; i >= 0; i--) {
      await view.append(vals[i])
    }
  }

  const base1 = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    apply: applyFunction,
    eagerUpdate: false
  })
  const base2 = new Autobase({
    inputs: [writerA, writerB, writerC],
    outputs: [output],
    apply: applyFunction,
    eagerUpdate: false
  })

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerB), writerB)
  }
  for (let i = 0; i < 4; i++) {
    await base1.append(`c${i}`, await base1.latest(writerC), writerC)
  }
  await base1.view.update()

  {
    const outputNodes = await linearizedValues(base2.view)
    t.same(outputNodes.map(v => v.value), applyValues(['a0', 'b1', 'b0', 'c3', 'c2', 'c1', 'c0']))
  }

  // Shift A's fork to the middle
  await base1.append('a1', await base1.latest(writerA), writerA)
  await base1.append('a2', await base1.latest(writerA), writerA)
  // output is not updated with the latest reordering here

  {
    const outputNodes = await linearizedValues(base2.view)
    t.same(outputNodes.map(v => v.value), applyValues(['b1', 'b0', 'a2', 'a1', 'a0', 'c3', 'c2', 'c1', 'c0']))
  }
})
