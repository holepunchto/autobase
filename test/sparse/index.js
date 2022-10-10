const test = require('brittle')

const { create, linearizedValues } = require('../helpers')

test('sparse local linearizing - can purge, causal writes', async t => {
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

  // Cut out writer A. Should truncate 3
  await baseA.removeInput(baseA.localInputKey)

  t.alike(await linearizedValues(baseA.view), ['b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 2) // b1 and b0 are reappended
  t.is(baseA.view.status.truncated, 3) // a0, b1, and b0 are removed
  t.is(baseA.localOutputs[0].length, 7)
})

test('sparse local linearizing - can purge, causal writes interleaved', async t => {
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

  await baseA.removeInput(baseB.localInputKey)

  t.alike(await linearizedValues(baseA.view), ['a5', 'a3', 'a1'])
  t.is(baseA.view.status.appended, 3)
  t.is(baseA.view.status.truncated, 6)
  t.is(baseA.localOutputs[0].length, 3)
})
