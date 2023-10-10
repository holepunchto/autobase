const test = require('brittle')

const {
  create,
  replicateAndSync,
  addWriterAndSync,
  apply,
  confirm,
  compareViews
} = require('./helpers')

// a - b - a

test('simple 2', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await addWriterAndSync(a, b)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  // await b.append('b' + bi++)
  await replicateAndSync(bases)

  t.alike(a.view.indexedLength, 1)
  t.alike(b.view.indexedLength, 1)

  compareViews([a, b], t)

  t.is(a.linearizer.tails.size, 1)
})

/*

c - b - a - c - b - a

*/

test('simple 3', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm(bases)

  await replicateAndSync(bases)

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

  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  compareViews([a, b, c], t)

  t.is(a.linearizer.tails.size, 1)
})

/*

a   b
| / |
b   c
| / |
c   a
| / |
a   b
| / |
b   c
| / |
c   a

*/

// known: test fails for current linearizer
test.skip('convergence', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm(bases)

  // --- loop ---

  await a.append('a' + ai++)
  await b.append('b' + bi++)

  await replicateAndSync([c, b])
  await replicateAndSync([b, a])

  await b.append('b' + bi++)
  await c.append('c' + ci++)

  await replicateAndSync([a, c])
  await replicateAndSync([c, b])
  await replicateAndSync([b, a])

  await a.append('a' + ai++)
  await c.append('c' + ci++)

  await replicateAndSync([a, c])
  await replicateAndSync([c, b])
  await replicateAndSync([b, a])

  await a.append('a' + ai++)
  await b.append('b' + bi++)

  await replicateAndSync([c, b])
  await replicateAndSync([b, a])

  await b.append('b' + bi++)
  await c.append('c' + ci++)

  await replicateAndSync([a, c])
  await replicateAndSync([c, b])
  await replicateAndSync([b, a])

  await a.append('a' + ai++)
  await c.append('c' + ci++)

  await replicateAndSync([c, a])
  await replicateAndSync([b, c])
  await replicateAndSync([a, b])

  // --- loop ---

  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  t.is(a.linearizer.tails.size, 1)

  compareViews([a, b, c], t)
})

/*

    b   c   d
  / | x | x | \
 a  b   c   d  e
  \ | x | x | /
    b   c   d
    | /
    b

*/

// known: test fails for current linearizer
test.skip('inner majority', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  let ai = 0
  let bi = 0
  let ci = 0
  let di = 0
  let ei = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e)

  await confirm(bases)

  // --- write ---

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await replicateAndSync([a, b])
  await replicateAndSync([e, d])
  await replicateAndSync([b, c])
  await replicateAndSync([d, c])
  await replicateAndSync([c, b])
  await replicateAndSync([c, d])

  await a.append('a' + ai++)
  await e.append('e' + ei++)
  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await replicateAndSync([b, c])
  await replicateAndSync([d, c])
  await replicateAndSync([c, d])
  await replicateAndSync([c, b])
  await replicateAndSync([b, a])
  await replicateAndSync([d, e])

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await replicateAndSync([b, c])

  await b.append('b' + bi++)

  t.is(b.view.indexedLength, 3)
  t.is(b.linearizer.tails.size, 1)
})

/*

  b - c - d - b - c - d

*/

test('majority alone - convergence', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  let bi = 0
  let ci = 0
  let di = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e)

  await confirm(bases)

  t.is(a.linearizer.indexers.length, 5)
  t.is(b.linearizer.indexers.length, 5)
  t.is(c.linearizer.indexers.length, 5)
  t.is(d.linearizer.indexers.length, 5)
  t.is(e.linearizer.indexers.length, 5)

  // --- write ---

  await b.append('b' + bi++)
  await replicateAndSync([b, c, d])

  await c.append('c' + ci++)
  await replicateAndSync([b, c, d])

  await d.append('d' + di++)
  await replicateAndSync([b, c, d])

  await b.append('b' + bi++)
  await replicateAndSync([b, c, d])

  await c.append('c' + ci++)
  await replicateAndSync([b, c, d])

  await d.append('d' + di++)
  await replicateAndSync([b, c, d])

  compareViews([b, c, d], t)

  t.is(b.view.indexedLength, 2)
  t.is(c.view.indexedLength, 2)
  t.is(d.view.indexedLength, 2)

  t.is(b.linearizer.tails.size, 1)
})

test('add writer', async t => {
  const [a, b, c] = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  let ai = 0
  let bi = 0
  let ci = 0

  await a.append('a' + ai++)

  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  compareViews([a, b], t)

  await addWriterAndSync(a, b)
  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  compareViews([a, b], t)

  await replicateAndSync([a, b, c])

  t.is(c.view.indexedLength, 1)

  compareViews([a, c], t)

  await addWriterAndSync(a, c)

  await b.append('b' + bi++)
  await a.append('a' + ai++)

  await replicateAndSync([a, b, c])

  await c.append('c' + ci++)
  await b.append('b' + bi++)
  await a.append('a' + ai++)
  await c.append('c' + ci++)
  await b.append('b' + bi++)
  await a.append('a' + ai++)

  await replicateAndSync([a, b, c])

  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(b.view.indexedLength, c.view.indexedLength)

  compareViews([a, b], t)
  compareViews([a, c], t)

  t.is(a.linearizer.tails.size, b.linearizer.tails.size)
  t.is(b.linearizer.tails.size, c.linearizer.tails.size)
})

/*

  b   c   d
  | x | x |
  b   c   d
  | x | x |
  b   c   d
  | /
  b

*/

test('majority alone - non-convergence', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  let bi = 0
  let ci = 0
  let di = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e)

  await confirm(bases)

  // --- write ---

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await replicateAndSync([b, c])
  await replicateAndSync([d, c])
  await replicateAndSync([c, b])
  await replicateAndSync([c, d])

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await replicateAndSync([b, c])
  await replicateAndSync([d, c])
  await replicateAndSync([c, b])
  await replicateAndSync([c, d])

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await replicateAndSync([b, c])

  await b.append('b' + bi++)

  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(b.view.indexedLength, c.view.indexedLength)

  compareViews([a, b, c], t)

  await replicateAndSync(bases)

  t.is(a.linearizer.tails.size, b.linearizer.tails.size)
  t.is(b.linearizer.tails.size, c.linearizer.tails.size)
})

/*

    a0  e0
    |   |
    b0  d0
    |   |
    c0  |
    |   |
    a1  |
    |   |
    b1  |
    | \ |
    c1  b2
    |   |
    a2  d1
    | / |
    d2  e2
    |   |
    a3  b3

[[a0, b0, c0, a1, ]]
*/

test('double fork', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  let ai = 0
  let bi = 0
  let ci = 0
  let di = 0
  let ei = 0

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e)

  await confirm(bases)

  await a.append('a' + ai++)
  await e.append('e' + ei++)

  await replicateAndSync([b, a])
  await replicateAndSync([d, e])

  await b.append('b' + bi++)
  await d.append('d' + di++)

  await replicateAndSync([c, b])
  await c.append('c' + ci++)

  await replicateAndSync([a, c])
  await a.append('a' + ai++)

  await replicateAndSync([b, a])
  await b.append('b' + bi++)

  await replicateAndSync([c, b])
  await replicateAndSync([b, d])

  await b.append('b' + bi++)
  await c.append('c' + ci++)

  await replicateAndSync([a, c])
  await replicateAndSync([d, b])

  await a.append('a' + ai++)
  await d.append('d' + di++)

  await replicateAndSync([e, d])
  await replicateAndSync([d, a])

  await d.append('d' + di++)
  await e.append('e' + ei++)

  await replicateAndSync([b, e])
  await replicateAndSync([a, d])

  await b.append('b' + bi++)
  await a.append('a' + ai++)

  // --- done ---

  const length = Math.min(a.view.indexedLength, b.view.indexedLength)

  for (let i = 0; i < length; i++) {
    const left = await a.view.get(i)
    const right = await b.view.get(i)

    if (left.value === right.value) continue

    t.fail()
    break
  }
})
