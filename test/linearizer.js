const b4a = require('b4a')
const test = require('brittle')

const {
  create,
  sync,
  addWriter,
  apply
} = require('./helpers')

/*

c - b - a - c - b - a

*/

test('linearizer - simple', async t => {
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
  await a.append('a' + ai++)
  await sync(bases)

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
  await a.append('a' + ai++)

  await syncTo(c, b)
  await c.append('c' + ci++)

  await syncTo(b, c)

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

  await sync(bases)

  t.ok(b4a.compare(c.local.key, a.local.key) < 0)

  aval = await a.view.get(4)
  bval = await b.view.get(4)
  cval = await c.view.get(4)

  t.is(aval, 'b1')
  t.is(bval, 'b1')
  t.is(cval, 'b1')

  aval = await a.view.get(5)
  bval = await b.view.get(5)
  cval = await c.view.get(5)

  t.is(aval, 'c2')
  t.is(bval, 'c2')
  t.is(cval, 'c2')

  aval = await a.view.get(6)
  bval = await b.view.get(6)
  cval = await c.view.get(6)

  t.is(aval, 'a1')
  t.is(bval, 'a1')
  t.is(cval, 'a1')

  await t.exception(a.view.get(7))
  await t.exception(b.view.get(7))
  await t.exception(c.view.get(7))

  t.is(a.linearizer.tails.size, 2)
})

test('linearizer - reordering', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriter(a, c)
  await sync(bases)

  // a will be isolated up to a3
  await a.append('a' + ai++)
  await a.append('a' + ai++)
  await a.append('a' + ai++)

  await addWriter(c, b)
  await syncTo(b, c)

  await b.append('b' + bi++)
  await syncTo(c, b)

  t.is(await b.view.get(0), 'b0')
  t.is(await c.view.get(0), 'b0')

  await syncTo(c, a)

  t.is(await b.view.get(0), 'b0')
  t.is(await c.view.get(0), 'a0')
  t.is(await c.view.get(3), 'b0')

  await c.append('c' + ci++)
  await sync(bases)

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
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  let ai = 0
  let bi = 0
  let ci = 0

  await addWriter(a, b)
  await sync(bases)

  // b will be isolated
  await b.append('b' + bi++)

  await addWriter(a, c)
  await syncTo(c, a)

  await c.append('c' + ci++)
  await syncTo(a, c)

  await a.append('a' + ai++)

  t.is(await b.view.get(0), 'b0')

  // trigger restart and reorder
  await sync(bases)
  await sync(bases)

  t.is(await b.view.get(0), 'c0')
  t.is(await b.view.get(1), 'a0')
  t.is(await b.view.get(2), 'b0')
})

test('linearizer - shouldAck', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  await addWriter(a, b)
  await sync(bases)

  await addWriter(a, c)
  await sync(bases)

  t.absent(a.linearizer.shouldAck(a.localWriter))
  t.absent(b.linearizer.shouldAck(getWriter(b, a.localWriter)))
  t.absent(c.linearizer.shouldAck(getWriter(c, a.localWriter)))

  t.ok(a.linearizer.shouldAck(getWriter(a, b.localWriter)))
  t.ok(b.linearizer.shouldAck(b.localWriter))
  t.ok(c.linearizer.shouldAck(getWriter(c, b.localWriter)))

  // c is not a writer yet
  t.absent(a.linearizer.shouldAck(getWriter(a, c.localWriter)))
  t.absent(b.linearizer.shouldAck(getWriter(b, c.localWriter)))
  t.absent(c.linearizer.shouldAck(c.localWriter))

  function getWriter (base, writer) {
    for (const w of base.writers) {
      if (b4a.compare(w.core.key, writer.core.key)) continue
      return w
    }

    return null
  }
})

test('linearizer - no loop', async t => {
  const bases = await create(4, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d] = bases

  let ai = 0
  let bi = 0

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await sync(bases)

  let i = 0
  while (++i < 20) {
    await sync(bases)

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

async function syncTo (a, b) {
  const s1 = a.store.replicate(true)
  const s2 = b.store.replicate(false)

  s1.on('error', () => {})
  s2.on('error', () => {})

  s1.pipe(s2).pipe(s1)

  await sync(a)

  s1.destroy()
  s2.destroy()
}
