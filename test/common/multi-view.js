const test = require('brittle')
const Hyperbee = require('hyperbee')
const lexint = require('lexicographic-integer')
const b = require('b4a')

const { create, linearizedValues } = require('../helpers')

test('multi-view - two identical views, local-only indexing, no rebasing', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const viewOptions = {
    views: 2,
    open: (core1, core2) => [core1, core2],
    apply: (core1, core2, batch) => Promise.all([
      core1.append(batch.map(n => n.value)),
      core2.append(batch.map(n => n.value))
    ])
  }
  const [coreA1, coreA2] = baseA.start(viewOptions)
  const [coreB1, coreB2] = baseB.start(viewOptions)

  await baseA.append('a0')
  await baseB.append('b0')
  await baseA.append('a1')
  await baseB.append('b1')

  await Promise.all([coreA1.update(), coreB1.update()])

  t.is(coreA1.length, 4)
  t.is(coreA2.length, 4)
  t.is(coreB1.length, 4)
  t.is(coreB2.length, 4)

  t.alike(await linearizedValues(coreA1), ['b1', 'a1', 'b0', 'a0'])
  t.alike(await linearizedValues(coreA2), ['b1', 'a1', 'b0', 'a0'])
  t.alike(await linearizedValues(coreB1), ['b1', 'a1', 'b0', 'a0'])
  t.alike(await linearizedValues(coreB2), ['b1', 'a1', 'b0', 'a0'])
})

test('multi-view - two identical views, remote indexing, no rebasing', async t => {
  const [baseA, baseB] = await create(2, { view: { oneRemote: true }, opts: { autostart: false, eagerUpdate: false } })

  const viewOptions = {
    views: 2,
    open: (core1, core2) => [core1, core2],
    apply: (core1, core2, batch) => Promise.all([
      core1.append(batch.map(n => n.value)),
      core2.append(batch.map(n => n.value))
    ])
  }
  const [coreA1, coreA2] = baseA.start(viewOptions)
  const [coreB1, coreB2] = baseB.start(viewOptions)

  await baseA.append('a0')
  await baseB.append('b0')
  await baseA.append('a1')
  await baseB.append('b1')

  await coreA1.update() // Will update both A1 and A2
  await Promise.all([coreB1.update(), coreB2.update()]) // Should use Base A's index
  t.absent(baseB._internalView.nodes[0])
  t.absent(baseB._internalView.nodes[1])

  t.is(coreA1.length, 4)
  t.is(coreA2.length, 4)
  t.is(coreB1.length, 4)
  t.is(coreB2.length, 4)

  t.alike(await linearizedValues(coreA1), ['b1', 'a1', 'b0', 'a0'])
  t.alike(await linearizedValues(coreA2), ['b1', 'a1', 'b0', 'a0'])
  t.alike(await linearizedValues(coreB1), ['b1', 'a1', 'b0', 'a0'])
  t.alike(await linearizedValues(coreB2), ['b1', 'a1', 'b0', 'a0'])
})

test('multi-view - two identical views, remote indexing, one rebase', async t => {

})

test('multi-view - two identical views, remote indexing, two rebases', async t => {

})

test('multi-view - different-length views, local-only indexing, no rebasing', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const viewOptions = {
    views: 2,
    open: (core1, core2) => [core1, core2],
    apply: async (core1, core2, batch) => {
      for (const node of batch) {
        if (b.toString(node.value).startsWith('a')) {
          await core1.append(node.value)
        } else {
          await core2.append(node.value)
        }
      }
    }
  }
  const [coreA1, coreA2] = baseA.start(viewOptions)
  const [coreB1, coreB2] = baseB.start(viewOptions)

  await baseA.append('a0')
  await baseB.append('b0')
  await baseA.append('a1')
  await baseB.append('b1')

  await Promise.all([coreA1.update(), coreB1.update()])

  t.is(coreA1.length, 2)
  t.is(coreA2.length, 2)
  t.is(coreB1.length, 2)
  t.is(coreB2.length, 2)

  t.alike(await linearizedValues(coreA1), ['a1', 'a0'])
  t.alike(await linearizedValues(coreA2), ['b1', 'b0'])
  t.alike(await linearizedValues(coreB1), ['a1', 'a0'])
  t.alike(await linearizedValues(coreB2), ['b1', 'b0'])
})

test('multi-view - different-length views, remote indexing, no rebasing', async t => {
  const [baseA, baseB] = await create(2, { view: { oneRemote: true }, opts: { autostart: false, eagerUpdate: false } })

  const viewOptions = {
    views: 2,
    open: (core1, core2) => [core1, core2],
    apply: async (core1, core2, batch) => {
      for (const node of batch) {
        if (b.toString(node.value).startsWith('a')) {
          await core1.append(node.value)
        } else {
          await core2.append(node.value)
        }
      }
    }
  }
  const [coreA1, coreA2] = baseA.start(viewOptions)
  const [coreB1, coreB2] = baseB.start(viewOptions)

  await baseA.append('a0')
  await baseB.append('b0')
  await baseA.append('a1')
  await baseB.append('b1')

  await coreA1.update()
  await coreB1.update()

  t.is(coreA1.length, 2)
  t.is(coreA2.length, 2)
  t.is(coreB1.length, 2)
  t.is(coreB2.length, 2)

  t.alike(await linearizedValues(coreA1), ['a1', 'a0'])
  t.alike(await linearizedValues(coreA2), ['b1', 'b0'])
  t.alike(await linearizedValues(coreB1), ['a1', 'a0'])
  t.alike(await linearizedValues(coreB2), ['b1', 'b0'])

  t.absent(baseB._internalView.nodes[0])
  t.absent(baseB._internalView.nodes[1])
})

test('multi-view - different-length views, remote indexing, one rebase', async t => {

})

test('multi-view - uneven views, local-only indexing, no rebasing', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const viewOptions = {
    views: 2,
    open: (core1, core2) => [core1, core2],
    apply: async (core1, core2, batch) => {
      for (const node of batch) {
        if (b.toString(node.value).startsWith('a')) {
          await core1.append(node.value)
        } else {
          await core2.append(node.value)
        }
      }
    }
  }
  const [coreA1, coreA2] = baseA.start(viewOptions)
  const [coreB1, coreB2] = baseB.start(viewOptions)

  await baseA.append('a0')
  await baseA.append('a1')
  await baseA.append('a2')
  await baseB.append('b0')

  await Promise.all([coreA1.update(), coreB1.update()])

  t.is(coreA1.length, 3)
  t.is(coreA2.length, 1)
  t.is(coreB1.length, 3)
  t.is(coreB2.length, 1)

  t.alike(await linearizedValues(coreA1), ['a2', 'a1', 'a0'])
  t.alike(await linearizedValues(coreA2), ['b0'])
  t.alike(await linearizedValues(coreB1), ['a2', 'a1', 'a0'])
  t.alike(await linearizedValues(coreB2), ['b0'])
})

test('multi-view - double-appending view, local-only indexing, no rebasing', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const viewOptions = {
    views: 2,
    open: (core1, core2) => [core1, core2],
    apply: async (core1, core2, batch) => {
      for (const node of batch) {
        await core1.append(node.value)
        await core1.append(node.value)
        await core2.append(node.value)
      }
    }
  }
  const [coreA1, coreA2] = baseA.start(viewOptions)
  const [coreB1, coreB2] = baseB.start(viewOptions)

  await baseA.append('a0')
  await baseB.append('b0')
  await Promise.all([coreA1.update(), coreB1.update()])

  await baseA.append('a1')
  await baseB.append('b1')
  await Promise.all([coreA1.update(), coreB1.update()])

  t.is(coreA1.length, 8)
  t.is(coreA2.length, 4)
  t.is(coreB1.length, 8)
  t.is(coreB2.length, 4)

  t.alike(await linearizedValues(coreA1), ['b1', 'b1', 'a1', 'a1', 'b0', 'b0', 'a0', 'a0'])
  t.alike(await linearizedValues(coreA2), ['b1', 'a1', 'b0', 'a0'])
  t.alike(await linearizedValues(coreB1), ['b1', 'b1', 'a1', 'a1', 'b0', 'b0', 'a0', 'a0'])
  t.alike(await linearizedValues(coreB2), ['b1', 'a1', 'b0', 'a0'])
})

test('multi-view - two views, hyperbee and raw', async t => {
  const [baseA, baseB] = await create(2, { view: { localOnly: true }, opts: { autostart: false, eagerUpdate: false } })

  const viewOptions = {
    views: 2,
    open: (core1, core2) => [
      core1,
      new Hyperbee(core2, {
        keyEncoding: 'utf-8',
        valueEncoding: 'utf-8',
        extension: false
      })
    ],
    apply: async (core, bee, batch) => {
      const b = bee.batch({ update: false })
      for (const node of batch) {
        await core.append(node.value) // core just records the raw messages
        const pos = core.length - 1
        for (const word of node.value.toString().split(' ')) {
          const key = `${word}-${lexint.pack(pos, 'hex')}`
          await b.put(key, lexint.pack(pos, 'hex'))
        }
      }
      await b.flush()
    }
  }
  const [core1, bee1] = baseA.start(viewOptions)
  const [core2, bee2] = baseB.start(viewOptions)

  await baseA.append('hey there')
  await baseB.append('hey how is it going')
  await baseA.append('it is good')
  await baseB.append('ah nice that is hey')

  await baseA._internalView.update()
  await baseB._internalView.update()

  // Find the latest occurrence of 'hey'
  for await (const node of bee1.createReadStream({ gt: 'hey-', lt: 'hey-~', reverse: true })) { // eslint-disable-line
    t.is(b.toString(await core2.get(lexint.unpack(node.value, 'hex'))), 'ah nice that is hey')
    break
  }
  // Find the latest occurrence of 'good'
  for await (const node of bee2.createReadStream({ gt: 'good-', lt: 'good-~', reverse: true })) { // eslint-disable-line
    t.is(b.toString(await core1.get(lexint.unpack(node.value, 'hex'))), 'it is good')
    break
  }
})
