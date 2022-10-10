const test = require('brittle')
const Corestore = require('corestore')
const ram = require('random-access-memory')

const { create, bufferize, causalValues, linearizedValues } = require('../helpers')
const Autobase = require('../..')

test('non-sparse local linearizing - can purge, causal writes', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create three causally-linked forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }
  for (let i = 0; i < 5; i++) {
    await baseC.append(`c${i}`, [])
  }

  t.alike(await linearizedValues(baseA.view), ['b1', 'b0', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 8)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 8)

  // Cut out writer A. B should no longer be satisfiable
  await baseA.removeInput(baseA.localInputKey)

  t.alike(await linearizedValues(baseA.view), ['c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 0)
  t.is(baseA.view.status.truncated, 3)
  t.is(baseA.localOutputs[0].length, 5)
})

test('non-sparse local linearizing - can purge, causal writes interleaved', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  for (let i = 0; i < 6; i++) {
    if (i % 2) {
      await baseA.append(`a${i}`)
    } else {
      await baseB.append(`b${i}`)
    }
  }

  t.alike(await linearizedValues(baseA.view), ['a5', 'b4', 'a3', 'b2', 'a1', 'b0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 6)

  // a5 is no longer satisfiable, so the causal stream cannot continue
  await baseA.removeInput(baseB.localInputKey)

  t.alike(await linearizedValues(baseA.view), [])
  t.is(baseA.view.status.appended, 0)
  t.is(baseA.view.status.truncated, 6)
  t.is(baseA.localOutputs[0].length, 0)
})

test.skip('non-sparse - causal stream, basic unavailable input', async t => {
  const storeA = new Corestore(ram)
  const storeB = new Corestore(ram)
  const storeC = new Corestore(ram)

  const inputA = storeA.get({ name: 'input' })
  const inputB = storeB.get({ name: 'input' })
  const inputC = storeC.get({ name: 'input' })
  await Promise.all([inputA.ready(), inputB.ready(), inputC.ready()])

  const baseA = new Autobase({
    inputs: [inputA, storeB.get(inputB.key), storeC.get(inputC.key)],
    localInput: inputA
  })
  const baseB = new Autobase({
    inputs: [storeA.get(inputA.key), inputB, storeC.get(inputC.key)],
    localInput: inputB
  })
  const baseC = new Autobase({
    inputs: [storeA.get(inputA.key), storeB.get(inputB.key), inputC],
    localInput: inputC
  })

  const r1 = replicate(storeA, storeB, t)
  const r2 = replicate(storeA, storeC, t)
  const r3 = replicate(storeB, storeC, t)

  // Two causally-connected writes
  await baseA.append('a0')
  await baseB.append(['b0', 'b1'])
  await baseC.append('c0')

  {
    const output = await causalValues(baseB)
    t.same(output.map(v => v.value), bufferize(['c0', 'b1', 'b0', 'a0']))
  }

  // Stop replicating between A and C
  await unreplicate(r2)
  await unreplicate(r1)
  await unreplicate(r3)

  // TODO: Finish test

  t.end()
})

function replicate (a, b, t) {
  const s1 = a.replicate(true, { keepAlive: false })
  const s2 = b.replicate(false, { keepAlive: false })
  s1.on('error', err => t.comment(`replication stream error (initiator): ${err}`))
  s2.on('error', err => t.comment(`replication stream error (responder): ${err}`))
  s1.pipe(s2).pipe(s1)
  return [s1, s2]
}

function unreplicate (streams) {
  return Promise.all(streams.map((s) => {
    return new Promise((resolve) => {
      s.on('error', () => {})
      s.on('close', resolve)
      s.destroy()
    })
  }))
}
