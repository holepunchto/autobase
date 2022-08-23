const Corestore = require('corestore')
const ram = require('random-access-memory')
const test = require('brittle')
const b4a = require('b4a')

const { create, causalValues, linearizedValues, bufferize } = require('../helpers')

test('batches - array-valued appends using partial input nodes', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create three dependent forks
  await baseA.append(['a0'])
  await baseB.append(['b0', 'b1'], await baseA.latest({ fork: true }))
  await baseC.append(['c0', 'c1', 'c2'], await baseA.latest({ fork: true }))

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a0']))
  }

  // Add 4 more records to A -- should switch fork ordering
  for (let i = 1; i < 5; i++) {
    await baseA.append(`a${i}`, [])
  }

  {
    const output = await causalValues(baseA)
    t.alike(output.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a4', 'a3', 'a2', 'a1', 'a0']))
  }
})

test('batches - batches store compressed clocks correctly', async t => {
  const store = new Corestore(ram)

  {
    const [baseA, baseB] = await create(2, { store, view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

    await baseA.append(['a0', 'a1'])
    await baseB.append(['b0', 'b1', 'b2'])
    await baseA.view.update()

    await Promise.all([baseA.close(), baseB.close()])
  }

  {
    const [baseA, baseB] = await create(2, { store, view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })
    await baseA.ready()

    // Can independently load the first block of the second batch
    const b0 = await baseA.localOutputs[0].get(2)

    t.alike(b0.change, baseB.localInputKey)
    t.is(b0.clock.size, 2) // The clock is the full batch clock
  }
})
