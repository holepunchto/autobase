const test = require('brittle')
const b4a = require('b4a')

const { create, linearizedValues } = require('../helpers')

test('applying - apply with one-to-one apply function', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const view = baseA.start({
    apply (view, batch) {
      batch = batch.map(({ value }) => b4a.from(b4a.toString(value).toUpperCase()))
      return view.append(batch)
    }
  })

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

  t.alike(await linearizedValues(view), ['A0', 'B1', 'B0', 'C2', 'C1', 'C0'])
})

test('applying - applying into batches yields the correct clock on reads', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const view = baseA.start({
    apply (view, batch) {
      batch = batch.map(({ value }) => b4a.from(b4a.toString(value).toUpperCase()))
      return view.append(batch)
    }
  })

  // Create three independent forks with batches
  await baseA.append(['a0'], [])
  await baseB.append(['b0', 'b1'], [])
  await baseC.append(['c0', 'c1', 'c2'], [])

  t.alike(await linearizedValues(view), ['A0', 'B1', 'B0', 'C2', 'C1', 'C0'])
})

test('applying - one-to-many apply with reordering, local output', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const view = baseA.start({
    async apply (view, batch) {
      for (const node of batch) {
        await view.append(Buffer.from(node.value.toString() + '-0'))
        await view.append(Buffer.from(node.value.toString() + '-1'))
      }
    }
  })

  // Create two independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }

  t.alike(await linearizedValues(view), ['a0-1', 'a0-0', 'b1-1', 'b1-0', 'b0-1', 'b0-0'])

  // Shift A's fork to the back
  await baseA.append('a1', [])
  await baseA.append('a2', [])

  t.alike(await linearizedValues(view), ['b1-1', 'b1-0', 'b0-1', 'b0-0', 'a2-1', 'a2-0', 'a1-1', 'a1-0', 'a0-1', 'a0-0'])
})

test('applying - one-to-many apply with reordering, remote output up-to-date', async t => {
  const [baseA, baseB] = await create(2, { view: { oneRemote: true }, opts: { apply, eagerUpdate: false } })

  // Create two independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }
  await baseA.view.update()

  t.alike(await linearizedValues(baseB.view), ['a0-1', 'a0-0', 'b1-1', 'b1-0', 'b0-1', 'b0-0'])

  // Shift A's fork to the back
  await baseA.append('a1', [])
  await baseA.append('a2', [])

  await baseA.view.update()

  console.log('\n\n === \n\n')
  t.alike(await linearizedValues(baseB.view), ['b1-1', 'b1-0', 'b0-1', 'b0-0', 'a2-1', 'a2-0', 'a1-1', 'a1-0', 'a0-1', 'a0-0'])

  async function apply (view, batch) {
    for (const node of batch) {
      await view.append(b4a.from(b4a.toString(node.value) + '-0'))
      await view.append(b4a.from(b4a.toString(node.value) + '-1'))
    }
  }
})

test('applying - one-to-many apply with reordering, remote output out-of-date', async t => {
  const [baseA, baseB, baseC] = await create(3, { view: { oneRemote: true }, opts: { apply, eagerUpdate: false } })

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await baseA.append(`a${i}`, [])
  }
  for (let i = 0; i < 2; i++) {
    await baseB.append(`b${i}`, [])
  }
  for (let i = 0; i < 4; i++) {
    await baseC.append(`c${i}`, [])
  }
  await baseA.view.update()

  t.alike(await linearizedValues(baseB.view), applyValues(['a0', 'b1', 'b0', 'c3', 'c2', 'c1', 'c0']).map(v => b4a.toString(v)))

  // Shift A's fork to the middle
  await baseA.append('a1', [])
  await baseA.append('a2', [])
  // baseA's local output is not updated with the latest reordering here

  t.alike(await linearizedValues(baseB.view), applyValues(['b1', 'b0', 'a2', 'a1', 'a0', 'c3', 'c2', 'c1', 'c0']).map(v => b4a.toString(v)))

  function applyValues (values) {
    return values.flatMap(v => [b4a.from(v + '-0'), b4a.from(v + '-1')])
  }

  async function apply (view, batch) {
    const values = batch.map(n => n.value.toString())
    const vals = applyValues(values)
    for (let i = vals.length - 1; i >= 0; i--) {
      await view.append(vals[i])
    }
  }
})
