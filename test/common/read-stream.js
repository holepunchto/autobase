const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { collect } = require('../helpers')
const Autobase = require('../..')

test('read stream -- not live, causally-linked writes', async t => {
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
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  {
    const output = await collect(base.createReadStream())
    t.same(output.length, 6)
    validateReadOrder(t, output)
  }

  // Add 3 more records to A -- not causally linked to B or C
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const output = await collect(base.createReadStream())
    t.same(output.length, 9)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream -- not live, inputs snapshotted', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB]
  })

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerA), writerB)
  }

  const stream = base.createReadStream()
  await new Promise(resolve => stream.once('readable', resolve))

  await base.addInput(writerC)
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  {
    const output = await collect(stream)
    t.same(output.length, 3)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream -- live, causally-linked writes', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB]
  })

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerA), writerB)
  }

  const stream = base.createReadStream({ live: true })
  const nodes = []
  stream.on('data', node => {
    nodes.push(node)
  })

  // Delay to ensure stream doesn't end after inputs are exhausted
  await new Promise(resolve => setTimeout(resolve, 50))

  await base.addInput(writerC)
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  // Delay to ensure at most 6 data events are emitted
  await new Promise(resolve => setTimeout(resolve, 50))

  t.same(nodes.length, 6)
  validateReadOrder(t, nodes)

  t.end()
})

test('read stream - onresolve hook, resolvable', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase({
    inputs: [writerA, writerB]
  })
  const base2 = new Autobase({
    inputs: [writerB]
  })

  // Create three dependent branches
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerA), writerB)
  }

  {
    // Without the onresolve hook, the read stream should consider A to be purged
    const output = await collect(base2.createReadStream())
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the onresolve hook, the read stream can be passed missing writers
    const output = await collect(base2.createReadStream({
      async onresolve (node) {
        t.same(node.id, writerB.key.toString('hex'))
        t.same(node.clock.get(writerA.key.toString('hex')), 0)
        await base2.addInput(writerA)
        return true
      }
    }))
    t.same(output.length, 3)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream - onresolve hook, not resolvable', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase({
    inputs: [writerA, writerB]
  })
  const base2 = new Autobase({
    inputs: [writerB]
  })

  // Create three dependent branches
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerA), writerB)
  }

  {
    // Without the onresolve hook, the read stream should consider A to be purged
    const output = await collect(base2.createReadStream())
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the onresolve hook, returning false should emit the unresolved nodes (same behavior as { onresolve: undefined } option)
    const output = await collect(base2.createReadStream({
      async onresolve (node) {
        t.same(node.id, writerB.key.toString('hex'))
        t.same(node.clock.get(writerA.key.toString('hex')), 0)
        return false
      }
    }))
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream - onwait hook', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase({
    inputs: [writerA, writerB]
  })
  const base2 = new Autobase({
    inputs: [writerB]
  })

  // Create two independent branches
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerB), writerB)
  }

  {
    // Without the onwait hook, the read stream should consider A to be purged
    const output = await collect(base2.createReadStream())
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the onwait hook, inputs can be added before the stream ends
    const output = await collect(base2.createReadStream({
      async onwait (node) {
        if (node.value.toString() !== 'b1') return
        await base2.addInput(writerA)
      }
    }))
    t.same(output.length, 3)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream - resume from checkpoint', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC]
  })

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerA), writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  const firstStream = base.createReadStream()

  {
    const output = await collect(firstStream)
    t.same(output.length, 6)
    validateReadOrder(t, output)
  }

  // Add 3 more records to A -- not causally linked to B or C
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const output = await collect(base.createReadStream({ checkpoint: firstStream.checkpoint }))
    t.same(output.length, 3)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream - resume from empty checkpoint', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC]
  })

  const firstStream = base.createReadStream()

  {
    const output = await collect(firstStream)
    t.same(output.length, 0)
  }

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerA), writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  {
    const output = await collect(base.createReadStream({ checkpoint: firstStream.checkpoint }))
    t.same(output.length, 6)
    validateReadOrder(t, output)
  }

  t.end()
})

// Skipped in non-sparse mode because all input blocks will be downloaded eagerly.
if (!process.argv.includes('--non-sparse')) {
  test('read stream - { wait: false } will not download remote blocks', async t => {
    const writerA = new Hypercore(ram)
    await writerA.ready()
    const writerB = new Hypercore(ram)
    const remoteWriterA = new Hypercore(ram, writerA.key)

    const base1 = new Autobase({
      inputs: [writerA],
      localInput: writerA
    })
    const base2 = new Autobase({
      inputs: [remoteWriterA, writerB],
      localInput: writerB
    })

    const s1 = writerA.replicate(true, { live: true })
    const s2 = remoteWriterA.replicate(false, { live: true })
    s1.pipe(s2).pipe(s1)

    await base1.append('a0')
    await base2.append('b0')
    await base1.append('a1')
    await base2.append('b1')

    await remoteWriterA.get(0) // Download the first block

    {
      // With wait: false, the read stream should only yield locally-available nodes
      const output = await collect(base2.createReadStream({ wait: false }))
      t.same(output.length, 3)
      validateReadOrder(t, output)
    }

    {
      // The normal read stream should download all blocks.
      const output = await collect(base2.createReadStream())
      t.same(output.length, 4)
      validateReadOrder(t, output)
    }

    t.end()
  })
}

test('read stream - tail option will start at the latest clock', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC]
  })

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, await base.latest(writerA), writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, await base.latest(writerC), writerC)
  }

  const expected = ['a1', 'a2', 'a3']
  const firstStream = base.createReadStream({ tail: true, live: true })
  const sawUpdates = new Promise(resolve => {
    firstStream.on('data', node => {
      t.same(node.value.toString(), expected.shift())
      if (!expected.length) resolve()
    })
  })

  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  await sawUpdates
  t.end()
})

function validateReadOrder (t, nodes) {
  for (let i = 0; i < nodes.length - 2; i++) {
    t.true(lteOrIndependent(nodes[i], nodes[i + 1]))
  }
}

function lteOrIndependent (n1, n2) {
  return n1.lte(n2) || !n2.contains(n1)
}
