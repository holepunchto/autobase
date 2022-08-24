const Hypercore = require('hypercore')
const ram = require('random-access-memory')
const test = require('brittle')
const b4a = require('b4a')

const Autobase = require('../..')
const { create, causalValues } = require('../helpers')

test('linearizes short branches on long branches', async t => {
  const [baseA, baseB, baseC] = await create(3)

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`, [])
  }

  {
    const output = await causalValues(baseB)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  {
    const output = await causalValues(baseC)
    t.alike(output.map(v => b4a.toString(v.value)), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
  }
})

test('causal writes', async t => {
  const [baseA, baseB, baseC] = await create(3)

  // Create three dependent branches
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }
  for (let i = 0; i < 4; i++) {
    await baseC.append(`c${i}`, [])
  }

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => b4a.toString(v.value)), ['b1', 'b0', 'a0', 'c3', 'c2', 'c1', 'c0'])
  }

  // Add 4 more records to A -- should switch fork ordering
  for (let i = 1; i < 5; i++) {
    await baseA.append(`a${i}`, [])
  }

  {
    const output = await causalValues(baseC)
    t.alike(output.map(v => b4a.toString(v.value)), ['b1', 'b0', 'c3', 'c2', 'c1', 'c0', 'a4', 'a3', 'a2', 'a1', 'a0'])
  }
})

// TODO: Currently fails, requires a causal stream fix
test.skip('manually specifying clocks, unavailable blocks', async t => {
  const [baseA, baseB] = await create(2)

  await baseA.append('a0')
  await baseA.append('a1')
  await baseB.append('b0', [
    [baseA.localInputKey.toString('hex'), 40] // Links to a1
  ])
  await baseB.append('b1', [])
  await baseB.append('b2', [])

  const output = await causalValues(baseB)
  t.alike(output.map(v => b4a.toString(v.value)), ['a1', 'a0'])
})

test('manually specifying clocks, available blocks', async t => {
  const [baseA, baseB] = await create(2)

  await baseA.append('a0')
  await baseA.append('a1')
  await baseB.append('b0', [
    [baseA.localInputKey.toString('hex'), 1] // Links to a1
  ])
  await baseB.append('b1', [])
  await baseB.append('b2', [])

  const output = await causalValues(baseB)
  t.alike(output.map(v => b4a.toString(v.value)), ['b2', 'b1', 'b0', 'a1', 'a0'])
})

test('supports a local input and default latest clocks', async t => {
  const [baseA, baseB] = await create(2)

  await baseA.append('a0')
  await baseA.append('a1')
  await baseB.append('b0')
  await baseA.append('a2')
  await baseB.append('b1')
  await baseA.append('a3')

  const output = await causalValues(baseA)
  t.alike(output.map(v => b4a.toString(v.value)), ['a3', 'b1', 'a2', 'b0', 'a1', 'a0'])
  t.alike(output[0].change, baseA.localInputKey)
  t.alike(output[1].change, baseB.localInputKey)
})

test('adding duplicate inputs is a no-op', async t => {
  const [baseA, baseB] = await create(2)

  await baseA.addInput(baseB.localInputKey)
  await baseA.addInput(baseA.localInputKey)

  t.is(baseA.inputs.length, 2)

  await baseA.append('a0')
  await baseA.append('a1')
  await baseB.append('b0')
  await baseA.append('a2')
  await baseB.append('b1')
  await baseA.append('a3')

  const output = await causalValues(baseA)
  t.alike(output.map(v => b4a.toString(v.value)), ['a3', 'b1', 'a2', 'b0', 'a1', 'a0'])
  t.alike(output[0].change, baseA.localInputKey)
  t.alike(output[1].change, baseB.localInputKey)
})

test('dynamically adding/removing inputs', async t => {
  const [baseA, baseB, baseC] = await create(3, { noInputs: true })

  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }

  {
    const output = await causalValues(baseA)
    t.is(output.length, 1)
  }

  await baseA.addInput(baseA.localInputKey)

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0'])
  }

  await baseA.addInput(baseB.localInputKey)

  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0', 'b1', 'b0'])
  }

  await baseA.addInput(baseC.localInputKey)

  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`)
  }

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => b4a.toString(v.value)), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
  }

  await baseA.removeInput(baseC.localInputKey)

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => b4a.toString(v.value)), ['b1', 'b0', 'a3', 'a2', 'a1', 'a0'])
  }
})

test('dynamically adding inputs does not alter existing causal streams', async t => {
  const [baseA, baseB, baseC] = await create(3, { noInputs: true })

  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0'])
  }

  await baseA.addInput(baseB.localInputKey)

  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }

  const output = []
  const stream = baseA.createCausalStream()
  await new Promise(resolve => stream.once('readable', resolve)) // Once the stream is opened, its heads are locked

  await baseA.addInput(baseC.localInputKey)

  for await (const node of stream) { // The stream should not have writerC's nodes
    output.push(node)
  }
  t.alike(output.map(v => b4a.toString(v.value)), ['a0', 'b1', 'b0'])
})

test('can parse headers', async t => {
  const [baseA] = await create(1, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })
  const notAutobase = new Hypercore(ram)

  await notAutobase.append(Buffer.from('hello world'))

  await baseA.append('a0')
  await baseA.view.update()

  t.ok(await Autobase.isAutobase(baseA.localInput))
  t.ok(await Autobase.isAutobase(baseA.localOutputs[0].core))
  t.absent(await Autobase.isAutobase(notAutobase))
})

test('equal-sized forks are deterministically ordered by key', async t => {
  for (let i = 0; i < 5; i++) {
    const [baseA, baseB] = await create(2)
    await baseA.append('i10', [])
    await baseA.append('i11', [])
    await baseB.append('i20', [])
    await baseB.append('i21', [])

    const values = (await causalValues(baseA)).map(v => b4a.toString(v.value))
    if (baseA.localInputKey > baseB.localInputKey) {
      t.alike(values, ['i21', 'i20', 'i11', 'i10'])
    } else {
      t.alike(values, ['i11', 'i10', 'i21', 'i20'])
    }
  }
})

test('causal stream with clock', async t => {
  const [baseA, baseB, baseC] = await create(3)

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }

  const clock1 = await baseA.latest()

  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }

  const clock2 = await baseB.latest()

  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`, [])
  }

  const clock3 = await baseC.latest()

  {
    const output = await causalValues(baseA, clock1)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0'])
  }

  {
    const output = await causalValues(baseB, clock2)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0', 'b1', 'b0'])
  }

  {
    const output = await causalValues(baseC, clock3)
    t.alike(output.map(v => b4a.toString(v.value)), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  }
})
