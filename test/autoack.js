const test = require('brittle')

const {
  create,
  apply,
  addWriter,
  replicate,
  sync,
  eventFlush
} = require('./helpers')

test('autoack - simple', async t => {
  t.plan(4)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 10, ackThreshold: 0 })

  const str1 = a.store.replicate(true)
  const str2 = b.store.replicate(false)

  str1.pipe(str2).pipe(str1)

  t.teardown(() => {
    a.close()
    b.close()
  })

  await addWriter(a, b)
  await sync(b)
  await b.append('b0')

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  setTimeout(() => {
    t.is(a.view.indexedLength, 1)
    t.is(b.view.indexedLength, 1)
  }, 100)
})

test('autoack - 5 writers', async t => {
  t.plan(22)

  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 10, ackThreshold: 0 })
  const [a, b, c, d, e] = bases

  replicate(bases)

  t.teardown(() => {
    bases.forEach(b => b.close())
  })

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await Promise.all(bases.map(sync))

  t.not(e.linearizer.indexers.length, 5)

  await b.append('b0')

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)
  t.is(c.view.indexedLength, 0)
  t.is(d.view.indexedLength, 0)
  t.is(e.view.indexedLength, 0)

  await new Promise(resolve => setTimeout(resolve, 1000))

  let alen = a.local.length
  let blen = b.local.length
  let clen = c.local.length
  let dlen = d.local.length
  let elen = e.local.length

  await new Promise(resolve => setTimeout(resolve, 300))

  // check that acks stop
  t.is(a.local.length, alen)
  t.is(b.local.length, blen)
  t.is(c.local.length, clen)
  t.is(d.local.length, dlen)
  t.is(e.local.length, elen)

  alen = a.local.length
  blen = b.local.length
  clen = c.local.length
  dlen = d.local.length
  elen = e.local.length

  await new Promise(resolve => setTimeout(resolve, 300))

  // check that acks stop
  t.is(a.local.length, alen)
  t.is(b.local.length, blen)
  t.is(c.local.length, clen)
  t.is(d.local.length, dlen)
  t.is(e.local.length, elen)

  await Promise.all([a, b, c, d, e].map(b => b.update()))

  t.is(e.linearizer.indexers.length, 5)

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)
  t.is(c.view.indexedLength, 1)
  t.is(d.view.indexedLength, 1)
  t.is(e.view.indexedLength, 1)
})

test('autoack - concurrent', async t => {
  t.plan(10)

  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 100, ackThreshold: 0 })
  const [a, b, c, d, e] = bases

  replicate(bases)

  t.teardown(() => {
    bases.forEach(b => b.close())
  })

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await Promise.all(bases.map(sync))

  await b.append(null)

  await Promise.all(bases.map(n => message(n, 10)))

  async function message (w, n) {
    for (let i = 0; i < n; i++) {
      await w.append(w.local.key.toString('hex').slice(0, 2) + n)
    }
  }

  setTimeout(async () => {
    t.is(a.view.indexedLength, 50)
    t.is(b.view.indexedLength, 50)
    t.is(c.view.indexedLength, 50)
    t.is(d.view.indexedLength, 50)
    t.is(e.view.indexedLength, 50)

    // max acks for any writer is bounded
    t.ok(a.local.length < 21)
    t.ok(b.local.length < 17)
    t.ok(c.local.length < 17)
    t.ok(d.local.length < 17)
    t.ok(e.local.length < 17)
  }, 1600)
})

test('autoack - threshold', async t => {
  t.plan(2)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 0, ackThreshold: 1 })

  const str1 = a.store.replicate(true)
  const str2 = b.store.replicate(false)

  str1.pipe(str2).pipe(str1)

  t.teardown(() => {
    a.close()
    b.close()
  })

  await addWriter(a, b)

  await sync(b)
  b.append('b0')
  b.append('b1')
  b.append('b2')
  await b.append('b3')

  await sync(a)

  await eventFlush()

  t.is(a.view.indexedLength, 4)
  t.is(b.view.indexedLength, 4)
})

test('autoack - threshold with interval', async t => {
  t.plan(3)

  const ackInterval = 10000
  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval, ackThreshold: 1 })

  const str1 = a.store.replicate(true)
  const str2 = b.store.replicate(false)

  str1.pipe(str2).pipe(str1)

  t.teardown(() => {
    a.close()
    b.close()
  })

  await addWriter(a, b)

  await sync(b)
  b.append('b0')
  b.append('b1')
  b.append('b2')
  await b.append('b3')
  await sync(a)

  await eventFlush()

  t.is(a._ackTimer._interval, ackInterval)
  t.is(a.view.indexedLength, 4)
  t.is(b.view.indexedLength, 4)
})
