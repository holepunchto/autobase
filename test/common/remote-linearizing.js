const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const Autobase = require('../..')

test('remote linearizing - selects longest remote output', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const output1 = new Hypercore(ram)
  const output2 = new Hypercore(ram)
  const output3 = new Hypercore(ram)

  const inputs = [writerA, writerB, writerC]
  const base1 = new Autobase({
    inputs,
    localOutput: output1,
    eagerUpdate: false
  })
  const base2 = new Autobase({
    inputs,
    localOutput: output2,
    eagerUpdate: false
  })
  const base3 = new Autobase({
    inputs,
    localOutput: output3,
    eagerUpdate: false
  })
  base1.start()
  base2.start()
  base3.start()

  // Create three independent forks and linearize them into separate outputs
  for (let i = 0; i < 3; i++) {
    await base1.append(`a${i}`, [], writerA)
  }

  await base1.view.update()

  for (let i = 0; i < 2; i++) {
    await base1.append(`b${i}`, [], writerB)
  }

  await base2.view.update()

  for (let i = 0; i < 1; i++) {
    await base1.append(`c${i}`, [], writerC)
  }

  await base3.view.update()

  {
    // Should not have to modify output3
    const base = new Autobase({
      inputs,
      outputs: [output3],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.same(base.view.status.appended, 0)
    t.same(base.view.status.truncated, 0)
    t.same(base.view.length, 6)
  }

  {
    // Should not have to add B and C
    const base = new Autobase({
      inputs,
      outputs: [output1],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.same(base.view.status.appended, 3)
    t.same(base.view.status.truncated, 0)
    t.same(base.view.length, 6)
  }

  {
    // Should select output2
    const base = new Autobase({
      inputs,
      outputs: [output1, output2],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.same(base.view.status.appended, 1)
    t.same(base.view.status.truncated, 0)
    t.same(base.view.length, 6)
  }

  {
    // Should select output3
    const base = new Autobase({
      inputs,
      outputs: [output1, output2, output3],
      autostart: true,
      eagerUpdate: false
    })
    await base.view.update()
    t.same(base.view.status.appended, 0)
    t.same(base.view.status.truncated, 0)
    t.same(base.view.length, 6)
  }

  t.end()
})

test('remote linearizing - can locally extend an out-of-date remote output', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const output1 = new Hypercore(ram)

  const inputs = [writerA, writerB, writerC]
  const writerBase = new Autobase({
    inputs,
    localOutput: output1,
    autostart: true,
    eagerUpdate: false
  })
  const readerBase = new Autobase({
    inputs,
    outputs: [output1],
    autostart: true,
    eagerUpdate: false
  })

  for (let i = 0; i < 3; i++) {
    await writerBase.append(`a${i}`, [], writerA)
  }
  await writerBase.view.update()
  await readerBase.view.update()

  t.same(writerBase.view.status.appended, 3)
  t.same(writerBase.view.status.truncated, 0)
  t.same(writerBase.view.length, 3)
  t.same(readerBase.view.status.appended, 0)
  t.same(readerBase.view.status.truncated, 0)
  t.same(readerBase.view.length, 3)

  for (let i = 0; i < 2; i++) {
    await writerBase.append(`b${i}`, [], writerB)
  }

  await readerBase.view.update()
  t.same(readerBase.view.status.appended, 2)
  t.same(readerBase.view.status.truncated, 0)
  t.same(readerBase.view.length, 5)

  for (let i = 0; i < 1; i++) {
    await writerBase.append(`c${i}`, [], writerC)
  }

  await readerBase.view.update()
  t.same(readerBase.view.status.appended, 1)
  t.same(readerBase.view.status.truncated, 0)
  t.same(readerBase.view.length, 6)

  // Extend C and lock the previous forks (will not reorg)
  for (let i = 1; i < 4; i++) {
    await writerBase.append(`c${i}`, writerC)
  }

  await readerBase.view.update()
  t.same(readerBase.view.status.appended, 3)
  t.same(readerBase.view.status.truncated, 0)
  t.same(readerBase.view.length, 9)

  // Create a new B fork at the back (full reorg)
  for (let i = 1; i < 11; i++) {
    await writerBase.append(`b${i}`, [], writerB)
  }

  await readerBase.view.update()
  t.same(readerBase.view.status.appended, 19)
  t.same(readerBase.view.status.truncated, 9)
  t.same(readerBase.view.length, 19)

  t.end()
})

test('remote linearizing - will discard local in-memory view if remote is updated', async t => {
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const output1 = new Hypercore(ram)

  const inputs = [writerA, writerB, writerC]
  const writerBase = new Autobase({
    inputs,
    localOutput: output1,
    autostart: true,
    eagerUpdate: false
  })
  const readerBase = new Autobase({
    inputs,
    outputs: [output1],
    autostart: true,
    eagerUpdate: false
  })

  for (let i = 0; i < 3; i++) {
    await writerBase.append(`a${i}`, [], writerA)
  }

  await writerBase.view.update() // Pull the first 3 nodes into output1
  await readerBase.view.update()
  t.same(readerBase.view.status.nodes.length, 0) // It should start up-to-date

  for (let i = 0; i < 2; i++) {
    await writerBase.append(`b${i}`, [], writerB)
  }

  await readerBase.view.update() // view extends output1 in memory
  t.same(readerBase.view.status.nodes.length, 2)

  for (let i = 0; i < 1; i++) {
    await writerBase.append(`c${i}`, [], writerC)
  }

  await readerBase.view.update()
  t.same(readerBase.view.status.nodes.length, 3)

  // Pull the latest changes into the output1
  await writerBase.view.update()
  await readerBase.view.update()
  t.same(readerBase.view.status.nodes.length, 0)

  t.end()
})

test('remote linearizing - can purge a writer', async t => {

})
