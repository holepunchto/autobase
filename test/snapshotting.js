const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, linearizedValues } = require('./helpers')
const Autobase = require('../')

test('snapshotting - can snapshot a view', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true,
    eagerUpdate: false
  })

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(base.view.status.appended, 6)
    t.same(base.view.status.truncated, 0)
    t.same(output.length, 6)
  }

  const s1 = base.view.snapshot()

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(`a${i}`, await base.latest(writerA), writerA)
  }

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']))
    t.same(base.view.status.appended, 9)
    t.same(base.view.status.truncated, 6)
    t.same(output.length, 9)
  }

  t.same(s1.length, 6)

  // The snapshot should still be on the old branch
  {
    const outputNodes = await linearizedValues(s1, { update: false })
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(s1.length, 6)
  }

  t.end()
})

test('snapshotting - snapshot updates are locked', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
  })

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  const s1 = base.view.snapshot()
  const s2 = base.view.snapshot()

  await Promise.all([s1.update(), s2.update(), base.view.update()])

  t.same(output.length, 6)
  t.same(s1.length, 6)
  t.same(s2.length, 6)

  t.end()
})

test('snapshotting - basic snapshot close', async t => {
  const output = new Hypercore(ram)
  const writerA = new Hypercore(ram)
  const writerB = new Hypercore(ram)
  const writerC = new Hypercore(ram)

  const base = new Autobase({
    inputs: [writerA, writerB, writerC],
    localOutput: output,
    autostart: true
  })

  const s1 = base.view.snapshot()
  const s2 = base.view.session()

  for (let i = 0; i < 1; i++) {
    await base.append(`a${i}`, [], writerA)
  }
  for (let i = 0; i < 2; i++) {
    await base.append(`b${i}`, [], writerB)
  }
  for (let i = 0; i < 3; i++) {
    await base.append(`c${i}`, [], writerC)
  }

  const s3 = base.view.snapshot()

  t.same(s1._sessionIdx, 0)
  t.same(s2._sessionIdx, 1)
  t.same(s3._sessionIdx, 0)
  t.same(base.view._sessionIdx, 0)
  // One for s2, one for base.view, and one for the internal view session.
  t.same(base.view._core._sessions.length, 3)

  await s2.close()
  t.same(base.view._sessionIdx, 0)
  t.same(base.view._core._sessions.length, 2)

  await s1.close()
  t.same(s1._core._sessions.length, 0)

  await s3.close()
  t.same(s3._core._sessions.length, 0)
  t.true(s3._core._sessions !== s2._core._sessions)

  {
    const outputNodes = await linearizedValues(base.view)
    t.same(outputNodes.map(v => v.value), bufferize(['a0', 'b1', 'b0', 'c2', 'c1', 'c0']))
    t.same(output.length, 6)
  }

  t.end()
})

test('snapshotting - snapshot session close', async t => {
  // TODO: implement
  t.end()
})
