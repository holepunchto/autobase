const Corestore = require('corestore')
const Keychain = require('keypear')
const ram = require('random-access-memory')
const test = require('brittle')

const { create, linearizedValues } = require('../helpers')
const Autobase = require('../..')

test('remote linearizing - selects longest remote output', async t => {
  const store = new Corestore(ram)
  const keychain = new Keychain()
  const [baseA, baseB, baseC] = await create(3, { store, keychain, view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create three independent forks and linearize them into separate outputs
  for (let i = 0; i < 3; i++) {
    await baseA.append(`a${i}`, [])
  }

  await baseA.view.update()

  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }

  await baseB.view.update()

  for (let i = 0; i < 1; i++) {
    await baseC.append(`c${i}`, [])
  }

  await baseC.view.update()

  {
    // Base C's output should be fully up-to-date
    const base = new Autobase(store, keychain.sub('base-1'), {
      inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey, baseC.localInputKeyPair.publicKey],
      outputs: [baseC.localOutputKeyPair.publicKey],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.absent(base._internalView.nodes[0])
    t.is(base.view.length, 6)
  }

  {
    // Should not have to add B and C
    const base = new Autobase(store, keychain.sub('base-2'), {
      inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey, baseC.localInputKeyPair.publicKey],
      outputs: [baseA.localOutputKeyPair.publicKey],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.is(base._internalView.nodes[0].length, 3)
    t.is(base.view.length, 6)
  }

  {
    // Should select Base B's output
    const base = new Autobase(store, keychain.sub('base-3'), {
      inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey, baseC.localInputKeyPair.publicKey],
      outputs: [baseA.localOutputKeyPair.publicKey, baseB.localOutputKeyPair.publicKey],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.is(base._internalView.nodes[0].length, 1)
    t.is(base.view.length, 6)
  }

  {
    // Should select Base C's output
    const base = new Autobase(store, keychain.sub('base-3'), {
      inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey, baseC.localInputKeyPair.publicKey],
      outputs: [baseA.localOutputKeyPair.publicKey, baseB.localOutputKeyPair.publicKey, baseC.localOutputKeyPair.publicKey],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.absent(base._internalView.nodes[0])
    t.is(base.view.length, 6)
  }
})

test('remote linearizing - can locally extend an out-of-date remote output', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { oneRemote: true }, opts: { autostart: true, eagerUpdate: false } })

  for (let i = 0; i < 3; i++) {
    await baseA.append(`a${i}`, [])
  }
  await baseA.view.update()
  await baseB.view.update()

  t.is(baseA.view.length, 3)
  t.is(baseB.view.length, 3)
  t.absent(baseB._internalView.nodes[0])

  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }

  await baseB.view.update()
  t.is(baseB.view.length, 5)
  t.is(baseB._internalView.nodes[0].length, 2)

  for (let i = 0; i < 1; i++) {
    await baseC.append(`c${i}`, [])
  }

  await baseB.view.update()
  t.is(baseB.view.length, 6)
  t.is(baseB._internalView.nodes[0].length, 3)

  // Extend C and lock the previous forks (will not reorg)
  for (let i = 1; i < 4; i++) {
    await baseC.append(`c${i}`)
  }

  await baseB.view.update()
  t.is(baseB.view.length, 9)
  t.is(baseB._internalView.nodes[0].length, 6)

  // Create a new B fork at the back (full reorg)
  for (let i = 1; i < 11; i++) {
    await baseB.append(`b${i}`, [])
  }

  await baseB.view.update()
  t.is(baseB.view.length, 19)
  t.is(baseB._internalView.nodes[0].length, 19)
})

test('remote linearizing - will discard local in-memory view if remote is updated', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { oneRemote: true }, opts: { autostart: true, eagerUpdate: false } })

  for (let i = 0; i < 3; i++) {
    await baseA.append(`a${i}`, [])
  }

  await baseA.view.update() // Pull the first 3 nodes into output1
  await baseB.view.update()
  t.absent(baseB._internalView.nodes[0]) // It should start up-to-date

  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }

  await baseB.view.update() // view extends Base A's output in memory
  t.is(baseB._internalView.nodes[0].length, 2)

  for (let i = 0; i < 1; i++) {
    await baseC.append(`c${i}`, [])
  }

  await baseB.view.update()
  t.is(baseB._internalView.nodes[0].length, 3)
  t.alike(await linearizedValues(baseB.view), ['c0', 'b1', 'b0', 'a2', 'a1', 'a0'])

  await baseC.append('c1')

  // Pull the latest changes into the output1
  await baseA.view.update()
  await baseB.view.update()
  t.absent(baseB._internalView.nodes[0])

  t.is(baseB.view.length, 7)
  t.alike(await linearizedValues(baseB.view), ['c1', 'c0', 'b1', 'b0', 'a2', 'a1', 'a0'])
})
