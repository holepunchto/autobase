const Corestore = require('corestore')
const Keychain = require('keypear')
const ram = require('random-access-memory')
const test = require('brittle')
const b = require('b4a')

const Autobase = require('../..')
const { create, collect } = require('../helpers')

test('read stream -- not live, causally-linked writes', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  // Create three dependent branches
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`)
  }

  {
    const output = await collect(baseA.createReadStream())
    t.is(output.length, 6)
    validateReadOrder(t, output)
  }

  // Add 3 more records to A -- not causally linked to B or C
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  {
    const output = await collect(baseA.createReadStream())
    t.is(output.length, 9)
    validateReadOrder(t, output)
  }
})

test('read stream -- not live, inputs snapshotted', async t => {
  const store = new Corestore(ram)
  const keychain = new Keychain()

  const [baseA, baseB] = await create(2, { store, keychain, view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }

  const stream = baseA.createReadStream()
  await new Promise(resolve => stream.once('readable', resolve))

  const baseC = new Autobase(store, keychain.sub('base-c'), {
    inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey]
  })
  await baseC.ready()

  await baseA.addInput(baseC.localInputKeyPair)
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`, [])
  }

  {
    const output = await collect(stream)
    t.is(output.length, 3)
    validateReadOrder(t, output)
  }
})

test('read stream -- live, causally-linked writes', async t => {
  const store = new Corestore(ram)
  const keychain = new Keychain()

  const [baseA, baseB] = await create(2, { store, keychain, view: { localOnly: true }, opts: { autostart: true, eagerUpdate: false } })

  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }

  const stream = baseA.createReadStream({ live: true })
  const nodes = []
  stream.on('data', node => {
    nodes.push(node)
  })

  // Delay to ensure stream doesn't end after inputs are exhausted
  await new Promise(resolve => setTimeout(resolve, 50))

  const baseC = new Autobase(store, keychain.sub('base-c'), {
    inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey]
  })
  await baseC.ready()

  await baseA.addInput(baseC.localInputKeyPair.publicKey)
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`, [])
  }

  // Delay to ensure at most 6 data events are emitted
  await new Promise(resolve => setTimeout(resolve, 50))

  t.is(nodes.length, 6)
  validateReadOrder(t, nodes)
})

test('read stream - onresolve hook, resolvable', async t => {
  const store = new Corestore(ram)
  const keychain = new Keychain()

  const baseA = new Autobase(store, keychain.sub('base-a'))
  const baseB = new Autobase(store, keychain.sub('base-b'))
  const baseC = new Autobase(store, keychain.sub('base-c'))
  await Promise.all([baseA.ready(), baseB.ready()])

  // A and B both acknowledge each other's writes
  await baseA.addInput(baseB.localInputKeyPair.publicKey)
  await baseB.addInput(baseA.localInputKeyPair.publicKey)

  // C does not initially know about A
  await baseC.addInput(baseB.localInputKeyPair.publicKey)

  // Create two dependent branches
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }

  {
    // Without the onresolve hook, the read stream should consider A to be purged
    const output = await collect(baseC.createReadStream())
    t.is(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the onresolve hook, the read stream can be passed missing writers
    const output = await collect(baseC.createReadStream({
      async onresolve (node) {
        t.is(node.id, b.toString(baseB.localInputKeyPair.publicKey, 'hex'))
        t.is(node.clock.get(b.toString(baseA.localInputKeyPair.publicKey, 'hex')), 0)
        await baseC.addInput(baseA.localInputKeyPair.publicKey)
        return true
      }
    }))

    t.is(output.length, 3)
    validateReadOrder(t, output)
  }
})

test('read stream - onresolve hook, not resolvable', async t => {
  const store = new Corestore(ram)
  const baseA = new Autobase(store.namespace('base-a'))
  const baseB = new Autobase(store.namespace('base-b'))
  const baseC = new Autobase(store.namespace('base-c'))
  await Promise.all([baseA.ready(), baseB.ready()])

  // A and B both acknowledge each other's writes
  await baseA.addInput(baseB.localInputKeyPair.publicKey)
  await baseB.addInput(baseA.localInputKeyPair.publicKey)

  // C does not initially know about A
  await baseC.addInput(baseB.localInputKeyPair.publicKey)

  // Create two dependent branches
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }

  {
    // Without the onresolve hook, the read stream should consider A to be purged
    const output = await collect(baseC.createReadStream())
    t.is(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the onresolve hook, returning false should emit the unresolved nodes (same behavior as { onresolve: undefined } option)
    const output = await collect(baseC.createReadStream({
      async onresolve (node) {
        t.is(node.id, b.toString(baseB.localInputKeyPair.publicKey, 'hex'))
        t.is(node.clock.get(b.toString(baseA.localInputKeyPair.publicKey, 'hex')), 0)
        return false
      }
    }))
    t.is(output.length, 2)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream - onwait hook', async t => {
  const store = new Corestore(ram)
  const baseA = new Autobase(store.namespace('base-a'))
  const baseB = new Autobase(store.namespace('base-b'))
  const baseC = new Autobase(store.namespace('base-c'))
  await Promise.all([baseA.ready(), baseB.ready()])

  // A and B both acknowledge each other's writes
  await baseA.addInput(baseB.localInputKeyPair.publicKey)
  await baseB.addInput(baseA.localInputKeyPair.publicKey)

  // C does not initially know about A
  await baseC.addInput(baseB.localInputKeyPair.publicKey)

  // Create two dependent branches
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }

  {
    // Without the onwait hook, the read stream should consider A to be purged
    const output = await collect(baseC.createReadStream())
    t.is(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the onwait hook, inputs can be added before the stream ends
    const output = await collect(baseC.createReadStream({
      async onwait (node) {
        if (b.toString(node.value) !== 'b1') return
        await baseC.addInput(baseA.localInputKeyPair.publicKey)
      }
    }))
    t.is(output.length, 3)
    validateReadOrder(t, output)
  }
})

test('read stream - resume from checkpoint', async t => {
  const store = new Corestore(ram)
  const keychain = new Keychain()

  const [baseA, baseB, baseC] = await create(3, { store, keychain })

  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`, [])
  }

  const fullBase = new Autobase(store, keychain.sub('full-base'), {
    inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey, baseC.localInputKeyPair.publicKey]
  })

  const firstStream = fullBase.createReadStream()

  {
    const output = await collect(firstStream)
    t.is(output.length, 6)
    validateReadOrder(t, output)
  }

  // Add 3 more records to A -- not causally linked to B or C
  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  {
    const output = await collect(fullBase.createReadStream({ checkpoint: firstStream.checkpoint }))
    t.is(output.length, 3)
    validateReadOrder(t, output)
  }
})

test('read stream - resume from empty checkpoint', async t => {
  const store = new Corestore(ram)
  const keychain = new Keychain()

  const [baseA, baseB, baseC] = await create(3, { store, keychain })

  const fullBase = new Autobase(store, keychain.sub('full-base'), {
    inputs: [baseA.localInputKeyPair.publicKey, baseB.localInputKeyPair.publicKey, baseC.localInputKeyPair.publicKey]
  })

  const firstStream = fullBase.createReadStream()

  {
    const output = await collect(firstStream)
    t.is(output.length, 0)
  }

  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`)
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`)
  }
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`, [])
  }

  {
    const output = await collect(fullBase.createReadStream({ checkpoint: firstStream.checkpoint }))
    t.is(output.length, 6)
    validateReadOrder(t, output)
  }
})

// Skipped in non-sparse mode because all input blocks will be downloaded eagerly.
if (+process.env['NON_SPARSE'] !== 1) { // eslint-disable-line
  test('read stream - { wait: false } will not download remote blocks', async t => {
    const storeA = new Corestore(ram)
    const storeB = new Corestore(ram)
    const keychain = new Keychain()

    const r = storeA.replicate(true)
    r.pipe(storeB.replicate(false)).pipe(r)

    const baseA = new Autobase(storeA, keychain.sub('store-a'))
    const baseB = new Autobase(storeB, keychain.sub('store-b'))
    await Promise.all([baseA.ready(), baseB.ready()])

    await baseA.addInput(baseB.localInputKeyPair.publicKey)
    await baseB.addInput(baseA.localInputKeyPair.publicKey)

    await baseA.append('a0')
    await baseB.append('b0')
    await baseA.append('a1')
    await baseB.append('b1')

    await baseB._inputsByKey.get(b.toString(baseA.localInputKeyPair.publicKey, 'hex')).get(0) // Download the first block

    {
      // With wait: false, the read stream should only yield locally-available nodes
      const output = await collect(baseB.createReadStream({ wait: false }))
      t.is(output.length, 3)
      validateReadOrder(t, output)
    }

    {
      // The normal read stream should download all blocks.
      const output = await collect(baseB.createReadStream())
      t.is(output.length, 4)
      validateReadOrder(t, output)
    }
  })
}

test('read stream - tail option will start at the latest clock', async t => {
  const store = new Corestore(ram)
  const [baseA, baseB, baseC] = await create(3, { store })

  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }
  for (let i = 0; i < 3; i++) {
    await baseC.append(`c${i}`, [])
  }

  const expected = ['a1', 'a2', 'a3']
  const firstStream = baseA.createReadStream({ tail: true, live: true })
  const sawUpdates = new Promise(resolve => {
    firstStream.on('data', node => {
      t.is(b.toString(node.value), expected.shift())
      if (!expected.length) resolve()
    })
  })

  for (let i = 1; i < 4; i++) {
    await baseA.append(`a${i}`, [])
  }

  await sawUpdates
})

function validateReadOrder (t, nodes) {
  for (let i = 0; i < nodes.length - 2; i++) {
    t.is(lteOrIndependent(nodes[i], nodes[i + 1]), true)
  }
}

function lteOrIndependent (n1, n2) {
  return n1.lte(n2) || !n2.contains(n1)
}
