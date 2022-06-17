const test = require('tape')
const Corestore = require('corestore')
const ram = require('random-access-memory')

const { bufferize, causalValues } = require('../helpers')
const Autobase = require('../..')

test('non-sparse - causal stream, basic unavailable input', async t => {
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
