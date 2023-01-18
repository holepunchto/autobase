const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, causalValues } = require('../helpers')
const Autobase = require('../..')

test('linearizes short branches on long branches', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC]
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

  const base = new Autobase({
    inputs: [writerA, writerB, writerC]
  })

  // Create three dependent branches
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerA), writerB)
  }
  for (let i = 0; i < 4; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'a0', 'c3', 'c2', 'c1', 'c0']))
  }

  // Add 4 more records to A -- should switch fork ordering
  for (let i = 1; i < 5; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const output = await causalValues(base)
    t.same(output.map(v => v.value), bufferize(['b1', 'b0', 'c3', 'c2', 'c1', 'c0', 'a4', 'a3', 'a2', 'a1', 'a0']))
  }

  t.end()
})

// TODO: This is an actual bug, but does not appear in practice. We will have to fix later.
test.skip('manually specifying clocks, unavailable blocks', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB]
  })

  await base.append('a0', await base.latest(writerA), writerA)
  await base.append('a1', await base.latest(writerA), writerA)
  await base.append('b0', [
    [writerA.key.toString('hex'), 40] // Links to a1
  ], writerB)
  await base.append('b1', await base.latest(writerB), writerB)
  await base.append('b2', await base.latest(writerB), writerB)
  console.log('latest here:', await base.latest())

  const output = await causalValues(base)
  t.same(output.map(v => v.value), bufferize(['a1', 'a0']))

  t.end()
})

test('manually specifying clocks, available blocks', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB]
  })

  await base.append('a0', await base.latest(writerA), writerA)
  await base.append('a1', await base.latest(writerA), writerA)
  await base.append('b0', [
    [writerA.key.toString('hex'), 1] // Links to a1
  ], writerB)
  await base.append('b1', await base.latest(writerB), writerB)
  await base.append('b2', await base.latest(writerB), writerB)

  const output = await causalValues(base)
  t.same(output.map(v => v.value), bufferize(['b2', 'b1', 'b0', 'a1', 'a0']))

  t.end()
})

test('supports a local input and default latest clocks', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase({
    inputs: [writerA, writerB],
    localInput: writerA
  })
  const base2 = new Autobase({
    inputs: [writerA, writerB],
    localInput: writerB
  })

  await base1.append('a0', await base1.latest())
  await base1.append('a1', await base1.latest())
  await base2.append('b0', await base2.latest())
  await base1.append('a2', await base1.latest())
  await base2.append('b1', await base2.latest())
  await base1.append('a3', await base1.latest())

  const output = await causalValues(base1)
  t.same(output.map(v => v.value), bufferize(['a3', 'b1', 'a2', 'b0', 'a1', 'a0']))
  t.same(output[0].change, writerA.key)
  t.same(output[1].change, writerB.key)

  t.end()
})

test('adding duplicate inputs is a no-op', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase({
    inputs: [writerA, writerA, writerB, writerB],
    localInput: writerA
  })
  const base2 = new Autobase({
    inputs: [writerA, writerB],
    localInput: writerB
  })

  await base2.addInput(writerA)
  await base2.addInput(writerB)

  t.same(base1.inputs.length, 2)
  t.same(base2.inputs.length, 2)

  await base1.append('a0', await base1.latest())
  await base1.append('a1', await base1.latest())
  await base2.append('b0', await base2.latest())
  await base1.append('a2', await base1.latest())
  await base2.append('b1', await base2.latest())
  await base1.append('a3', await base1.latest())

  const output = await causalValues(base1)
  t.same(output.map(v => v.value), bufferize(['a3', 'b1', 'a2', 'b0', 'a1', 'a0']))
  t.same(output[0].change, writerA.key)
  t.same(output[1].change, writerB.key)

  t.end()
})

test('dynamically adding/removing inputs', async t => {
  const writerA = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA]
  })

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

  const base = new Autobase({
    inputs: [writerA]
  })

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

test.skip('can parse headers', async t => {
  const output = new Hypercore(ram)
  const writer = new Hypercore(ram)
  const notAutobase = new Hypercore(ram)
  await notAutobase.append(Buffer.from('hello world'))

  const base = new Autobase({
    inputs: [writer],
    outputs: [output],
    localInput: writer,
    localOutput: output,
    autostart: true
  })
  await base.append('a0')
  await base.view.update()

  t.true(await Autobase.isAutobase(writer))
  t.true(await Autobase.isAutobase(output))
  t.false(await Autobase.isAutobase(notAutobase))

  t.end()
})

test('equal-sized forks are deterministically ordered by key', async t => {
  for (let i = 0; i < 5; i++) {
    const input1 = new Hypercore(ram)
    const input2 = new Hypercore(ram)
    const base = new Autobase({
      inputs: [input1, input2],
      autostart: true
    })

    await base.append('i10', [], input1)
    await base.append('i11', [], input1)
    await base.append('i20', [], input2)
    await base.append('i21', [], input2)

    const values = (await causalValues(base)).map(v => v.value.toString())
    if (input1.key > input2.key) {
      t.same(values, ['i21', 'i20', 'i11', 'i10'])
    } else {
      t.same(values, ['i11', 'i10', 'i21', 'i20'])
    }
  }

  t.end()
})

test('causal stream with clock', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC]
  })

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  const clock1 = await base.latest()

  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerB), writerB)
  }

  const clock2 = await base.latest()

  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  const clock3 = await base.latest()

  {
    const output = await causalValues(base, clock1)
    t.same(output.map(v => v.value), bufferize(['a0']))
  }

  {
    const output = await causalValues(base, clock2)
    t.same(output.map(v => v.value), bufferize(['a0', 'b1', 'b0']))
  }

  {
    const output = await causalValues(base, clock3)
    t.same(output.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
  }

  t.end()
})
