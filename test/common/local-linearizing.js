const p = require('path')
const os = require('os')

const Corestore = require('corestore')
const Keychain = require('keypear')
const test = require('brittle')
const b4a = require('b4a')

const { create, linearizedValues } = require('../helpers')
const { decodeKeys } = require('../../lib/nodes/messages')

test('local linearizing - three independent forks', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

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

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 6)

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, await baseA.latest({ fork: true }))
  }

  t.alike(await linearizedValues(baseA.view), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
  t.is(baseA.view.status.appended, 9)
  t.is(baseA.view.status.truncated, 6)
  t.is(baseA.localOutputs[0].length, 9)
})

test('local linearizing - three independent forks, not persisted', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false, persist: false } })

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

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 0)

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, await baseA.latest({ fork: true }))
  }

  t.alike(await linearizedValues(baseA.view), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
  t.is(baseA.view.status.appended, 9)
  t.is(baseA.view.status.truncated, 6)
  t.is(baseA.localOutputs[0].length, 0)
})

test('local linearizing - causal writes preserve clock', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create three causally-linked forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`)
  }

  t.alike(await linearizedValues(baseA.view), ['c2', 'c1', 'c0', 'b1', 'b0', 'a0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.view.length, 6)

  const wrapped = baseA.view.wrap()
  t.is(wrapped.length, baseA.view.length)

  for (let i = 1; i < wrapped.length; i++) {
    const prev = await wrapped.get(i - 1)
    const node = await wrapped.get(i)
    t.not(node.lt(prev), true)
  }
})

test('local linearizing - does not over-truncate', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }
  for (let i = 0; i < 5; i++) {
    await baseC.append(`c${i}`, [])
  }

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 8)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 8)

  // Add 3 more records to A -- should switch fork ordering (A after C)
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  t.alike(await linearizedValues(baseA.view), ['b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 3)
  t.is(baseA.localOutputs[0].length, 11)

  // Add 1 more record to B -- should not cause any reordering
  await baseB.append('b2', [])

  const values = await linearizedValues(baseA.view)
  t.alike(values, ['b2', 'b1', 'b0', 'a3', 'a2', 'a1', 'a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 1)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 12)
})

test('local linearizing - can purge', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }
  for (let i = 0; i < 5; i++) {
    await baseC.append(`c${i}`, [])
  }

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 8)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 8)

  // Cut out writer B. Should truncate 3
  await baseA.removeInput(baseB.localInputKey)

  t.alike(await linearizedValues(baseA.view), ['a0', 'c4', 'c3', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 1) // a0 is reindexed
  t.is(baseA.view.status.truncated, 3) // a0 is popped and reindexed
  t.is(baseA.localOutputs[0].length, 6)
})

test('local linearizing - can purge from the back', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create two independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 5; i++) {
    await baseB.append(`b${i}`, [])
  }

  t.alike(await linearizedValues(baseA.view), ['a0', 'b4', 'b3', 'b2', 'b1', 'b0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 6)

  // Cut out writer B. Should truncate 3
  await baseA.removeInput(baseB.localInputKey)

  t.alike(await linearizedValues(baseA.view), ['a0'])
  t.is(baseA.view.status.appended, 1) // a0 is reindexed
  t.is(baseA.view.status.truncated, 6) // a0 is popped and reindexed
  t.is(baseA.localOutputs[0].length, 1)
})

test('local linearizing - can purge from the front', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create two independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 5; i++) {
    await baseB.append(`b${i}`, [])
  }

  t.alike(await linearizedValues(baseA.view), ['a0', 'b4', 'b3', 'b2', 'b1', 'b0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 6)

  // Cut out writer A. Should truncate 1
  await baseA.removeInput(baseA.localInputKey)

  t.alike(await linearizedValues(baseA.view), ['b4', 'b3', 'b2', 'b1', 'b0'])
  t.is(baseA.view.status.appended, 0) // a0 is reindexed
  t.is(baseA.view.status.truncated, 1) // a0 is popped and reindexed
  t.is(baseA.localOutputs[0].length, 5)
})

test('local linearizing - many writers, no causal writes', async t => {
  const NUM_BASES = 10
  const NUM_APPENDS = 11

  const bases = await create(NUM_BASES, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })
  const middleBase = bases[Math.floor(bases.length / 2)]

  for (let i = 1; i < NUM_BASES + 1; i++) {
    const base = bases[i - 1]
    for (let j = 0; j < i; j++) {
      await base.append(`w${i}-${j}`, [])
    }
  }

  {
    const values = await linearizedValues(middleBase.view)
    t.is(values.length, (NUM_BASES * (NUM_BASES + 1)) / 2)
  }

  // Appending to the middle writer NUM_APPEND times should shift it to the back of the index.
  for (let i = 0; i < NUM_APPENDS; i++) {
    await middleBase.append(`new entry ${i}`, [])
  }

  await middleBase.view.update()

  for (let i = 0; i < NUM_APPENDS + Math.floor(NUM_BASES.length / 2); i++) {
    const value = await middleBase.view.get(i)
    t.is(b4a.toString(value), b4a.toString((await middleBase._getInputNode(middleBase._localInput, i)).value))
  }
})

test('local linearizing - double-linearizing is a no-op', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

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

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 6)

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 0)
  t.is(baseA.view.status.truncated, 0)
  t.is(baseA.localOutputs[0].length, 6)
})

test('local linearizing - can dynamically add a local output', async t => {
  const [baseA, baseB, baseC] = await create(3, { opts: { autostart: true, eagerUpdate: false } })

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

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.view.status.appended, 6)
  t.is(baseA.view.status.truncated, 0)
  t.absent(baseA._localOutputs())

  await baseA.addOutput(baseA.localOutputKey)

  t.alike(await linearizedValues(baseA.view), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  // TODO: Fix these, they should both be 0
  t.is(baseA.view.status.appended, 6) // should be 0
  t.is(baseA.view.status.truncated, 6) // should be 0
  t.is(baseA.localOutputs[0].length, 6)
})

test('local linearizing - truncation does not break key compression', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

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
    // A's key initially should be stored in the 6th block
    await baseA.view.update()
    const keys = decodeKeys(await baseA.localOutputs[0].get(5))
    t.is(keys.length, 1)
    t.alike(keys[0], baseA.localInputKey)
  }

  // Add 3 more records to A -- should switch fork ordering
  // A's key should be re-recorded into the 0th block after truncation
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  {
    // A's key should be stored in the 0th block
    await baseA.view.update()
    const keys = decodeKeys(await baseA.localOutputs[0].get(0))
    t.is(keys.length, 1)
    t.alike(keys[0], baseA.localInputKey)
  }
})

test('local linearizing - creating two branch snapshots with a common update clones core snapshots', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

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

  await baseA.view.update()
  const snapshot1 = baseA.view.snapshot()
  const snapshot2 = baseA.view.snapshot()

  t.is(baseA.localOutputs[0].length, 6)

  t.alike(await linearizedValues(snapshot1, { update: false }), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.localOutputs[0].length, 6)

  t.alike(await linearizedValues(snapshot2, { update: false }), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.localOutputs[0].length, 6)

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  await snapshot2.update()

  t.alike(await linearizedValues(snapshot2, { update: false }), ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0'])
  t.is(baseA.localOutputs[0].length, 9)

  t.alike(await linearizedValues(snapshot1, { update: false }), ['a0', 'b1', 'b0', 'c2', 'c1', 'c0'])
  t.is(baseA.localOutputs[0].length, 9)
})

test('local linearizing - consistent reads with a pre-update snapshot', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  await baseA.append('a0')
  await baseB.append('b0')

  await baseA.view.update()
  const snapshot = baseA.view.snapshot()

  t.is(snapshot.length, 2)

  const nodes = []
  for (let i = 0; i < snapshot.length; i++) {
    nodes.push(await snapshot.get(i))
  }
  t.alike(nodes.map(n => b4a.toString(n)), ['a0', 'b0'])
})

test('local linearizing - does not truncate on restart', async t => {
  const tmpdir = p.join(os.tmpdir(), 'autobase-test-' + process.pid)
  const store = new Corestore(tmpdir)
  const keychain = new Keychain()

  {
    const [baseA, baseB] = await create(2, { store, keychain, view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

    await baseA.append('a0')
    await baseB.append('b0')
    await baseA.view.update()

    t.is(baseA.view.status.appended, 2)
    t.is(baseA.localOutputs[0].length, 2)
    t.is(baseA.localOutputs[0].fork, 0)

    await Promise.all([baseA.close(), baseB.close()])
  }

  {
    const [baseA] = await create(2, { store, keychain, view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

    await baseA.view.update()

    t.is(baseA.view.status.appended, 2) // TODO: Will currently emit a faulty append on restart
    t.is(baseA.localOutputs[0].length, 2)
    t.is(baseA.localOutputs[0].fork, 0)
  }
})
