const test = require('brittle')

const {
  create,
  apply,
  addWriter,
  addWriterAndSync,
  confirm,
  replicateAndSync,
  replicate,
  sync,
  eventFlush
} = require('./helpers')

test('autoack - simple', async t => {
  t.plan(6)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 10, ackThreshold: 0 })

  const unreplicate = replicate([a, b])
  t.teardown(unreplicate)

  await addWriterAndSync(a, b)
  await confirm([a, b])

  await b.append('b0')

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  setTimeout(() => {
    t.not(a.local.length, 1)
    t.not(b.local.length, 1)

    t.is(a.view.indexedLength, 1)
    t.is(b.view.indexedLength, 1)
  }, 100)
})

test('autoack - 5 writers', async t => {
  t.plan(26)

  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 10, ackThreshold: 0 })
  const [a, b, c, d, e] = bases

  const unreplicate = replicate(bases)

  t.teardown(async () => {
    await unreplicate()
    await Promise.all(bases.map(b => b.close()))
  })

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e)

  await sync(bases)

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

  await sync([a, b, c, d, e])

  t.is(a.linearizer.indexers.length, 5)
  t.is(b.linearizer.indexers.length, 5)
  t.is(c.linearizer.indexers.length, 5)
  t.is(d.linearizer.indexers.length, 5)
  t.is(e.linearizer.indexers.length, 5)

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)
  t.is(c.view.indexedLength, 1)
  t.is(d.view.indexedLength, 1)
  t.is(e.view.indexedLength, 1)
})

test('autoack - concurrent', async t => {
  t.plan(10)

  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 20, ackThreshold: 0 })
  const [a, b, c, d, e] = bases

  const unreplicate = replicate(bases)

  t.teardown(async () => {
    await unreplicate()
    await Promise.all(bases.map(b => b.close()))
  })

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e)

  await sync(bases)

  await Promise.all(bases.map(n => message(n, 10)))

  setTimeout(async () => {
    t.is(a.view.indexedLength, 50)
    t.is(b.view.indexedLength, 50)
    t.is(c.view.indexedLength, 50)
    t.is(d.view.indexedLength, 50)
    t.is(e.view.indexedLength, 50)

    const alen = a.local.length
    const blen = b.local.length
    const clen = c.local.length
    const dlen = d.local.length
    const elen = e.local.length

    // check no acks after index
    setTimeout(() => {
      t.is(a.local.length, alen)
      t.is(b.local.length, blen)
      t.is(c.local.length, clen)
      t.is(d.local.length, dlen)
      t.is(e.local.length, elen)
    }, 500)
  }, 2000)

  async function message (w, n) {
    for (let i = 0; i < n; i++) {
      await w.append(w.local.key.toString('hex').slice(0, 2) + n)
    }
  }
})

test('autoack - threshold', async t => {
  t.plan(2)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 0, ackThreshold: 1 })

  const unreplicate = replicate([a, b])

  t.teardown(async () => {
    await unreplicate()
    await a.close()
    await b.close()
  })

  await addWriterAndSync(a, b)

  await sync([a, b])

  for (let i = 0; i < 4; i++) b.append('b')

  await sync([a, b])
  await eventFlush() // allow the bg threshold to react...
  for (let i = 0; i < 4; i++) await b.append('b')
  await sync([a, b])

  t.ok(a.view.indexedLength >= 4)
  t.ok(b.view.indexedLength >= 4)
})

test('autoack - threshold with interval', async t => {
  t.plan(3)

  const ackInterval = 800
  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval, ackThreshold: 1 })

  const unreplicate = replicate([a, b])

  t.teardown(async () => {
    await unreplicate()
    await a.close()
    await b.close()
  })

  await addWriterAndSync(a, b)

  await sync([a, b])

  b.append('b0')
  b.append('b1')
  b.append('b2')
  await b.append('b3')
  await new Promise(resolve => setTimeout(resolve, 400)) // fast ack
  await sync([a, b])
  await eventFlush() // allow the bg threshold to react...
  b.append('b4')
  b.append('b5')
  b.append('b6')
  await b.append('b7')
  await sync([a, b])
  await new Promise(resolve => setTimeout(resolve, 400)) // fast ack

  t.is(a._ackTimer.interval, ackInterval)
  t.ok(a.view.indexedLength >= 4)
  t.ok(b.view.indexedLength >= 4)
})

test('autoack - no null acks', async t => {
  t.plan(2)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 10, ackThreshold: 0 })

  const unreplicate = replicate([a, b])
  t.teardown(unreplicate)

  await addWriterAndSync(a, b)
  await confirm([a, b])

  await a.append(null)

  const alen = a.local.length
  const blen = b.local.length

  setTimeout(() => {
    t.is(a.local.length, alen)
    t.is(b.local.length, blen)
  }, 100)
})

test('autoack - value beneath null values', async t => {
  t.plan(4)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 10, ackThreshold: 0 })

  const unreplicate = replicate([a, b])
  t.teardown(unreplicate)

  await addWriterAndSync(a, b)
  await confirm([a, b])

  await b.append('b0')
  await b.append(null)

  await sync([a, b])

  const alen = a.local.length
  const blen = b.local.length

  setTimeout(() => {
    t.not(a.local.length, alen)
    t.not(b.local.length, blen)
    t.is(b.view.length, b.view.indexedLength)
    t.is(a.view.length, a.view.indexedLength)
  }, 100)
})

test('autoack - merge', async t => {
  t.plan(2)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 0, ackThreshold: 0 })

  await addWriterAndSync(a, b)
  await replicateAndSync([a, b])

  await a.append('a0')
  await b.append('b0')
  await replicateAndSync([a, b])

  await a.append('a1')
  await b.append('b1')
  await replicateAndSync([a, b])

  t.ok(a.linearizer.shouldAck(a.localWriter))
  t.ok(b.linearizer.shouldAck(b.localWriter))
})

test('autoack - merge when not head', async t => {
  t.plan(12)

  const [a, b, c] = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 0, ackThreshold: 0 })

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)

  await confirm([a, b, c])

  await a.append('a0')
  await b.append('b0')
  await replicateAndSync([a, b, c])

  await c.append('c0')
  await replicateAndSync([a, b, c])

  t.absent(a.linearizer.shouldAck(getWriter(a, c.localWriter)))
  t.absent(b.linearizer.shouldAck(getWriter(b, c.localWriter)))
  t.absent(c.linearizer.shouldAck(c.localWriter))

  await a.append('a1')
  await b.append('b1')
  await replicateAndSync([a, b, c])

  t.ok(a.linearizer.shouldAck(a.localWriter))
  t.ok(a.linearizer.shouldAck(getWriter(a, b.localWriter)))
  t.ok(a.linearizer.shouldAck(getWriter(a, c.localWriter)))

  t.ok(b.linearizer.shouldAck(getWriter(b, a.localWriter)))
  t.ok(b.linearizer.shouldAck(b.localWriter))
  t.ok(b.linearizer.shouldAck(getWriter(b, c.localWriter)))

  t.ok(c.linearizer.shouldAck(getWriter(c, a.localWriter)))
  t.ok(c.linearizer.shouldAck(getWriter(c, b.localWriter)))
  t.ok(c.linearizer.shouldAck(c.localWriter))
})

test('autoack - more merges', async t => {
  t.plan(8)

  const [a, b, c] = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 0, ackThreshold: 0 })

  await addWriterAndSync(a, b)
  await confirm([a, b, c])

  await a.append('a0')
  await b.append('b0')

  await replicateAndSync([b, c])
  await replicateAndSync([a, c])

  t.is(a.system.heads.length, 2)
  t.is(b.system.heads.length, 1)
  t.alike(b.system.heads[0].key, b.local.key)

  t.ok(a.linearizer.shouldAck(a.localWriter))

  await a.append('a1')
  await b.append('b1')

  await replicateAndSync([a, c])
  await replicateAndSync([b, c])

  t.ok(b.linearizer.shouldAck(getWriter(b, a.localWriter)))
  t.ok(b.linearizer.shouldAck(b.localWriter))

  await replicateAndSync([a, c])

  t.ok(a.linearizer.shouldAck(a.localWriter))
  t.ok(a.linearizer.shouldAck(getWriter(a, b.localWriter)))
})

test('autoack - pending writers', async t => {
  t.plan(5)

  const [a, b, c] = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 0, ackThreshold: 0 })

  t.teardown(await replicate([a, b, c]))

  await addWriter(a, b)
  await addWriter(a, c)

  await eventFlush()

  t.ok(b.local.length > 0)
  t.ok(c.local.length > 0)

  t.is(a.system.indexers.length, 3)
  t.is(b.system.indexers.length, 3)
  t.is(c.system.indexers.length, 3)
})

test('autoack - pending migrates', async t => {
  t.plan(5)

  const [a, b] = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 0, ackThreshold: 0 })

  t.teardown(await replicate([a, b]))

  await addWriter(a, b)
  await eventFlush()

  await new Promise(resolve => setTimeout(resolve, 100))

  await sync([a, b])

  await a.append(null)

  await sync([a, b])

  t.ok(b.local.length > 0)

  t.is(b.system.core._source._indexers, 2)
  t.is(b.system.core._source.queued, -1)

  t.is(a.system.core._source._indexers, 2)
  t.is(a.system.core._source.queued, -1)
})

test('autoack - minority indexers who are both tails', async t => {
  const [a, b, c, d] = await create(4, apply, store => store.get('test', { valueEncoding: 'json' }), null, { ackInterval: 10, ackThreshold: 0 })

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)

  await replicateAndSync([a, b, c, d])
  await b.append(null)
  await c.append(null)
  await d.append(null)

  await confirm([a, b, c, d])

  // this will get confirmed
  await a.append(null)

  await replicateAndSync([a, b]) // e is inert, use them to "one way" sync

  // we want these two left as tails
  await a.append('a0')
  await b.append('b0')

  await replicateAndSync([b, c])
  await c.append(null)
  await replicateAndSync([c, d])
  await d.append(null)
  await replicateAndSync([b, d])
  await b.append(null)

  // now we keep c and d offline
  t.teardown(await replicate([a, b]))

  await new Promise(resolve => setTimeout(resolve, 400))

  const alen = a.local.length
  const blen = b.local.length

  const asize = a.linearizer.size
  const bsize = b.linearizer.size

  await new Promise(resolve => setTimeout(resolve, 400))

  t.absent(b.linearizer.shouldAck(b.localWriter))

  t.is(a.local.length, alen)
  t.is(a.linearizer.size, asize)

  t.is(b.local.length, blen)
  t.is(b.linearizer.size, bsize)
})

function getWriter (base, writer) {
  return base.activeWriters.get(writer.core.key)
}
