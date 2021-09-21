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

// TODO: Revisit once we figure out the right bootstrapping strategy
test.skip('read stream -- bootstrapping', async t => {
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

  // writerA and writerB should be snapshotted, writerC shouldn't
  const stream = base.createReadStream({ bootstrapping: true })
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

  // These writes to A should not be emitted
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  // Delay to ensure at most 6 data events are emitted
  await new Promise(resolve => setTimeout(resolve, 50))

  t.same(nodes.length, 6)
  validateReadOrder(t, nodes)

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
