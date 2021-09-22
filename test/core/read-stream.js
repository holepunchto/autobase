const test = require('tape')
const Hypercore = require('hypercore-x')
const ram = require('random-access-memory')

const { collect } = require('../helpers')
const Autobase = require('../..')

test('read stream -- not live, causally-linked writes', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase([writerA, writerB, writerC])
  await base.ready()

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

  const base = new Autobase([writerA, writerB])
  await base.ready()

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

  const base = new Autobase([writerA, writerB])
  await base.ready()

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

test('read stream - resolve hook, resolvable', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase([writerA, writerB])
  const base2 = new Autobase([writerB])

  // Create three dependent branches
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerA), writerB)
  }

  {
    // Without the resolve hook, the read stream should consider A to be purged
    const output = await collect(base2.createReadStream())
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the resolve hook, the read stream can be passed missing writers
    const output = await collect(base2.createReadStream({
      async resolve (node) {
        t.same(node.id, writerB.key.toString('hex'))
        t.same(node.clock.get(writerA.key.toString('hex')), 1)
        await base2.addInput(writerA)
        return true
      }
    }))
    t.same(output.length, 3)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream - resolve hook, not resolvable', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase([writerA, writerB])
  const base2 = new Autobase([writerB])

  // Create three dependent branches
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerA), writerB)
  }

  {
    // Without the resolve hook, the read stream should consider A to be purged
    const output = await collect(base2.createReadStream())
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the resolve hook, returning false should emit the unresolved nodes (same behavior as { resolve: undefined } option)
    const output = await collect(base2.createReadStream({
      async resolve (node) {
        t.same(node.id, writerB.key.toString('hex'))
        t.same(node.clock.get(writerA.key.toString('hex')), 1)
        return false
      }
    }))
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  t.end()
})

test('read stream - wait hook', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)

  const base1 = new Autobase([writerA, writerB])
  const base2 = new Autobase([writerB])

  // Create two independent branches
  for (let i = 0; i < 1; i++) {
    await base1.append(`a${i}`, await base1.latest(writerA), writerA)
  }
  // B's writes depend on a0
  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, await base1.latest(writerB), writerB)
  }

  {
    // Without the wait hook, the read stream should consider A to be purged
    const output = await collect(base2.createReadStream())
    t.same(output.length, 2)
    validateReadOrder(t, output)
  }

  {
    // With the wait hook, inputs can be added before the stream ends
    const output = await collect(base2.createReadStream({
      async wait (node) {
        if (node.value.toString() !== 'b1') return
        await base2.addInput(writerA)
      }
    }))
    t.same(output.length, 3)
    validateReadOrder(t, output)
  }

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
