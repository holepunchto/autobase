const b4a = require('b4a')
const test = require('brittle')

const {
  create,
  confirm,
  replicateAndSync,
  addWriterAndSync
} = require('./helpers')

/*

c - b - a - c - b - a

*/

test('linearizer - simple', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm(bases)

  await c.append('c' + ci++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await c.append('c' + ci++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  t.alike(a.view.indexedLength, 4)
  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  t.alike(a.view.length, 6)
  t.alike(a.view.length, b.view.length)
  t.alike(c.view.length, b.view.length)
  t.alike(a.view.length, c.view.length)

  let aval = await a.view.get(0)
  let bval = await b.view.get(0)
  let cval = await c.view.get(0)

  t.is(aval, 'c0')
  t.is(bval, 'c0')
  t.is(cval, 'c0')

  aval = await a.view.get(1)
  bval = await b.view.get(1)
  cval = await c.view.get(1)

  t.is(aval, 'b0')
  t.is(bval, 'b0')
  t.is(cval, 'b0')

  aval = await a.view.get(2)
  bval = await b.view.get(2)
  cval = await c.view.get(2)

  t.is(aval, 'a0')
  t.is(bval, 'a0')
  t.is(cval, 'a0')

  aval = await a.view.get(3)
  bval = await b.view.get(3)
  cval = await c.view.get(3)

  t.is(aval, 'c1')
  t.is(bval, 'c1')
  t.is(cval, 'c1')

  aval = await a.view.get(4)
  bval = await b.view.get(4)
  cval = await c.view.get(4)

  t.is(aval, 'b1')
  t.is(bval, 'b1')
  t.is(cval, 'b1')

  aval = await a.view.get(5)
  bval = await b.view.get(5)
  cval = await c.view.get(5)

  t.is(aval, 'a1')
  t.is(bval, 'a1')
  t.is(cval, 'a1')

  await t.exception(a.view.get(6))
  await t.exception(b.view.get(6))
  await t.exception(c.view.get(6))

  t.is(a.linearizer.tails.size, 1)
})

test('linearizer - compete', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm(bases)

  await c.append('c' + ci++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await c.append('c' + ci++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await a.append('a' + ai++)
  await replicateAndSync(bases)

  t.alike(a.view.indexedLength, 4)
  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  t.alike(a.view.length, 6)
  t.alike(a.view.length, b.view.length)
  t.alike(c.view.length, b.view.length)
  t.alike(a.view.length, c.view.length)

  let aval = await a.view.get(0)
  let bval = await b.view.get(0)
  let cval = await c.view.get(0)

  t.is(aval, 'c0')
  t.is(bval, 'c0')
  t.is(cval, 'c0')

  aval = await a.view.get(1)
  bval = await b.view.get(1)
  cval = await c.view.get(1)

  t.is(aval, 'b0')
  t.is(bval, 'b0')
  t.is(cval, 'b0')

  aval = await a.view.get(2)
  bval = await b.view.get(2)
  cval = await c.view.get(2)

  t.is(aval, 'a0')
  t.is(bval, 'a0')
  t.is(cval, 'a0')

  aval = await a.view.get(3)
  bval = await b.view.get(3)
  cval = await c.view.get(3)

  t.is(aval, 'c1')
  t.is(bval, 'c1')
  t.is(cval, 'c1')

  aval = await a.view.get(4)
  bval = await b.view.get(4)
  cval = await c.view.get(4)

  t.is(aval, 'a1')
  t.is(bval, 'a1')
  t.is(cval, 'a1')

  aval = await a.view.get(5)
  bval = await b.view.get(5)
  cval = await c.view.get(5)

  t.is(aval, 'b1')
  t.is(bval, 'b1')
  t.is(cval, 'b1')

  await t.exception(a.view.get(6))
  await t.exception(b.view.get(6))
  await t.exception(c.view.get(6))

  t.is(a.linearizer.tails.size, 2)
})

test('linearizer - count ordering', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm(bases)

  await c.append('c' + ci++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await c.append('c' + ci++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await a.append('a' + ai++)

  await replicateAndSync([c, b])
  await c.append('c' + ci++)

  await replicateAndSync([b, c])

  t.alike(a.view.indexedLength, 3)
  t.alike(c.view.indexedLength, 4)
  t.alike(b.view.indexedLength, c.view.indexedLength)

  t.alike(a.view.length, 5)
  t.alike(c.view.length, 6)
  t.alike(b.view.length, c.view.length)

  let aval = await a.view.get(0)
  let bval = await b.view.get(0)
  let cval = await c.view.get(0)

  t.is(aval, 'c0')
  t.is(bval, 'c0')
  t.is(cval, 'c0')

  aval = await a.view.get(1)
  bval = await b.view.get(1)
  cval = await c.view.get(1)

  t.is(aval, 'b0')
  t.is(bval, 'b0')
  t.is(cval, 'b0')

  aval = await a.view.get(2)
  bval = await b.view.get(2)
  cval = await c.view.get(2)

  t.is(aval, 'a0')
  t.is(bval, 'a0')
  t.is(cval, 'a0')

  aval = await a.view.get(3)
  bval = await b.view.get(3)
  cval = await c.view.get(3)

  t.is(aval, 'c1')
  t.is(bval, 'c1')
  t.is(cval, 'c1')

  aval = await a.view.get(4)
  t.is(aval, 'a1')

  bval = await b.view.get(4)
  cval = await c.view.get(4)

  t.is(bval, 'b1')
  t.is(cval, 'b1')

  bval = await b.view.get(5)
  cval = await c.view.get(5)

  t.is(bval, 'c2')
  t.is(cval, 'c2')

  await t.exception(a.view.get(5))
  await t.exception(b.view.get(6))
  await t.exception(c.view.get(6))

  await replicateAndSync(bases)

  t.ok(b4a.compare(a.local.key, c.local.key) < 0)

  aval = await a.view.get(4)
  bval = await b.view.get(4)
  cval = await c.view.get(4)

  t.is(aval, 'a1')
  t.is(bval, 'a1')
  t.is(cval, 'a1')

  aval = await a.view.get(5)
  bval = await b.view.get(5)
  cval = await c.view.get(5)

  t.is(aval, 'b1')
  t.is(bval, 'b1')
  t.is(cval, 'b1')

  aval = await a.view.get(6)
  bval = await b.view.get(6)
  cval = await c.view.get(6)

  t.is(aval, 'c2')
  t.is(bval, 'c2')
  t.is(cval, 'c2')

  await t.exception(a.view.get(7))
  await t.exception(b.view.get(7))
  await t.exception(c.view.get(7))

  t.is(a.linearizer.tails.size, 2)
})

test('linearizer - reordering', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriterAndSync(a, c, true, bases)

  // a will be isolated up to a3
  await a.append('a' + ai++)
  await a.append('a' + ai++)
  await a.append('a' + ai++)

  await addWriterAndSync(c, b)

  await b.append('b' + bi++)
  await replicateAndSync([c, b])

  t.is(await b.view.get(0), 'b0')
  t.is(await c.view.get(0), 'b0')

  await replicateAndSync([c, a])

  t.is(await b.view.get(0), 'b0')
  t.is(await c.view.get(0), 'a0')
  t.is(await c.view.get(3), 'b0')

  await c.append('c' + ci++)
  await replicateAndSync(bases)

  t.is(await a.view.get(0), 'a0')
  t.is(await b.view.get(0), 'a0')
  t.is(await c.view.get(0), 'a0')

  t.is(await a.view.get(3), 'b0')
  t.is(await b.view.get(3), 'b0')
  t.is(await c.view.get(3), 'b0')

  t.is(await a.view.get(4), 'c0')
  t.is(await b.view.get(4), 'c0')
  t.is(await c.view.get(4), 'c0')
})

test('linearizer - reordering after restart', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriterAndSync(a, b)

  // b will be isolated
  await b.append('b' + bi++)

  await addWriterAndSync(a, c)

  await c.append('c' + ci++)
  await replicateAndSync([a, c])

  await a.append('a' + ai++)

  t.is(await b.view.get(0), 'b0')

  // trigger restart and reorder
  await replicateAndSync(bases)
  await replicateAndSync(bases)

  t.is(await b.view.get(0), 'b0')
  t.is(await b.view.get(1), 'c0')
  t.is(await b.view.get(2), 'a0')
})

test('linearizer - shouldAck', async t => {
  const { bases } = await create(3, t)

  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await a.ack() // be the last node

  await replicateAndSync(bases)

  // someone needs to ack
  t.ok(a.linearizer.shouldAck(a.localWriter) || b.linearizer.shouldAck(b.localWriter))

  // consistent across bases
  t.is(b.linearizer.shouldAck(getWriter(b, a.localWriter)), a.linearizer.shouldAck(a.localWriter))
  t.is(c.linearizer.shouldAck(getWriter(c, a.localWriter)), a.linearizer.shouldAck(a.localWriter))

  t.is(a.linearizer.shouldAck(getWriter(a, b.localWriter)), b.linearizer.shouldAck(b.localWriter))
  t.is(c.linearizer.shouldAck(getWriter(c, b.localWriter)), b.linearizer.shouldAck(b.localWriter))

  // c is not a writer yet
  t.absent(a.linearizer.shouldAck(getWriter(a, c.localWriter)))
  t.absent(b.linearizer.shouldAck(getWriter(b, c.localWriter)))
  t.absent(c.linearizer.shouldAck(c.localWriter))

  function getWriter (base, writer) {
    return base.activeWriters.get(writer.core.key)
  }
})

// review: test passes, but not sure what this test is for?
test.skip('linearizer - no loop', async t => {
  const { bases } = await create(4, t)

  const [a, b, c, d] = bases

  let ai = 0
  let bi = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await replicateAndSync(bases)

  let i = 0
  while (++i < 20) {
    await replicateAndSync(bases)

    if (a.linearizer.shouldAck(a.localWriter)) {
      await a.append('a' + ai++)
      continue
    }

    if (b.linearizer.shouldAck(b.localWriter)) {
      await b.append('b' + bi++)
      continue
    }

    break
  }

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  t.not(i, 20)
})
