const test = require('brittle')

const {
  create,
  sync,
  addWriter,
  apply,
  confirm,
  compare
} = require('./helpers')

// a - b - a

test('simple 2', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await addWriter(a, b)
  await sync(bases)

  await a.append('a' + ai++)
  await sync(bases)

  await b.append('b' + bi++)
  await sync(bases)

  await a.append('a' + ai++)
  await sync(bases)

  // await b.append('b' + bi++)
  await sync(bases)

  t.alike(a.view.indexedLength, 1)
  t.alike(b.view.indexedLength, 1)

  try {
    await compare(a, b)
  } catch (e) {
    t.fail(e.message)
  }

  t.is(a.linearizer.tails.length, 1)
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

  await addWriter(a, b)
  await addWriter(a, c)

  await sync(bases)

  await c.append('c' + ci++)
  await sync(bases)

  await b.append('b' + bi++)
  await sync(bases)

  await a.append('a' + ai++)
  await sync(bases)

  await c.append('c' + ci++)
  await sync(bases)

  await b.append('b' + bi++)
  await sync(bases)

  await a.append('a' + ai++)
  await sync(bases)

  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  try {
    await compare(a, b)
    await compare(a, c)
  } catch (e) {
    t.fail(e.message)
  }

  t.is(a.linearizer.tails.length, 1)
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

  await addWriter(a, b)
  await addWriter(a, c)

  await confirm(bases)

  // --- loop ---

  await a.append('a' + ai++)
  await b.append('b' + bi++)

  await syncTo(c, b)
  await syncTo(b, a)

  await b.append('b' + bi++)
  await c.append('c' + ci++)

  await syncTo(a, c)
  await syncTo(c, b)
  await syncTo(b, a)

  await a.append('a' + ai++)
  await c.append('c' + ci++)

  await syncTo(a, c)
  await syncTo(c, b)
  await syncTo(b, a)

  await a.append('a' + ai++)
  await b.append('b' + bi++)

  await syncTo(c, b)
  await syncTo(b, a)

  await b.append('b' + bi++)
  await c.append('c' + ci++)

  await syncTo(a, c)
  await syncTo(c, b)
  await syncTo(b, a)

  await a.append('a' + ai++)
  await c.append('c' + ci++)

  await sync([c, a])
  await sync([b, c])
  await sync([a, b])

  // --- loop ---

  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  t.is(a.linearizer.tails.length, 1)

  try {
    await compare(a, b)
    await compare(a, c)
  } catch (e) {
    t.fail(e.message)
  }
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

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await confirm(bases)

  // --- write ---

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await syncTo(a, b)
  await syncTo(e, d)
  await syncTo(b, c)
  await syncTo(d, c)
  await syncTo(c, b)
  await syncTo(c, d)

  await a.append('a' + ai++)
  await e.append('e' + ei++)
  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await syncTo(b, c)
  await syncTo(d, c)
  await syncTo(c, d)
  await syncTo(c, b)
  await syncTo(b, a)
  await syncTo(d, e)

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await syncTo(b, c)

  await b.append('b' + bi++)

  t.is(b.view.indexedLength, 3)
  t.is(b.linearizer.tails.length, 1)
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

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await confirm(bases)

  // --- write ---

  await b.append('b' + bi++)
  await sync([b, c, d])

  await c.append('c' + ci++)
  await sync([b, c, d])

  await d.append('d' + di++)
  await sync([b, c, d])

  await b.append('b' + bi++)
  await sync([b, c, d])

  await c.append('c' + ci++)
  await sync([b, c, d])

  await d.append('d' + di++)
  await sync([b, c, d])

  try {
    await compare(b, c)
    await compare(b, d)
  } catch (e) {
    t.fail(e.message)
  }

  t.is(b.view.indexedLength, 2)
  t.is(c.view.indexedLength, 2)
  t.is(d.view.indexedLength, 2)

  t.is(b.linearizer.tails.length, 1)
})

test('add writer', async t => {
  const [a, b, c] = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  let ai = 0
  let bi = 0
  let ci = 0

  await a.append('a' + ai++)

  await sync([a, b])

  await b.update()

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  await t.execution(compare(a, b))

  await addWriter(a, b)
  await sync([a, b])

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  await t.execution(compare(a, b))

  await sync([a, b, c])

  t.is(c.view.indexedLength, 1)

  await t.execution(compare(a, c))

  await addWriter(a, c)

  await b.append('b' + bi++)
  await a.append('a' + ai++)

  await sync([a, b, c])

  await c.append('c' + ci++)
  await b.append('b' + bi++)
  await a.append('a' + ai++)
  await c.append('c' + ci++)
  await b.append('b' + bi++)
  await a.append('a' + ai++)

  await sync([a, b, c])

  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(b.view.indexedLength, c.view.indexedLength)

  await t.execution(compare(a, b))
  await t.execution(compare(a, c))

  t.is(a.linearizer.tails.length, b.linearizer.tails.length)
  t.is(b.linearizer.tails.length, c.linearizer.tails.length)
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

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await sync(bases)

  // --- write ---

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await syncTo(b, c)
  await syncTo(d, c)
  await syncTo(c, b)
  await syncTo(c, d)

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await syncTo(b, c)
  await syncTo(d, c)
  await syncTo(c, b)
  await syncTo(c, d)

  await b.append('b' + bi++)
  await c.append('c' + ci++)
  await d.append('d' + di++)

  await syncTo(b, c)

  await b.append('b' + bi++)

  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(b.view.indexedLength, c.view.indexedLength)

  await t.execution(compare(a, b))
  await t.execution(compare(a, c))

  await sync(bases)

  t.is(a.linearizer.tails.length, b.linearizer.tails.length)
  t.is(b.linearizer.tails.length, c.linearizer.tails.length)
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

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await confirm(bases)

  await a.append('a' + ai++)
  await e.append('e' + ei++)

  await syncTo(b, a)
  await syncTo(d, e)

  await b.append('b' + bi++)
  await d.append('d' + di++)

  await syncTo(c, b)
  await c.append('c' + ci++)

  await syncTo(a, c)
  await a.append('a' + ai++)

  await syncTo(b, a)
  await b.append('b' + bi++)

  await syncTo(c, b)
  await syncTo(b, d)

  await b.append('b' + bi++)
  await c.append('c' + ci++)

  await syncTo(a, c)
  await syncTo(d, b)

  await a.append('a' + ai++)
  await d.append('d' + di++)

  await syncTo(e, d)
  await syncTo(d, a)

  await d.append('d' + di++)
  await e.append('e' + ei++)

  await syncTo(b, e)
  await syncTo(a, d)

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

async function syncTo (a, b) {
  const s1 = a.store.replicate(true)
  const s2 = b.store.replicate(false)

  s1.on('error', () => {})
  s2.on('error', () => {})

  s1.pipe(s2).pipe(s1)

  await a.update({ wait: true })

  s1.destroy()
  s2.destroy()
}
