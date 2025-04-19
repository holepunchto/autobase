const test = require('brittle')

const {
  create,
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

  const { bases } = await create(2, t, {
    ackInterval: 10,
    ackThreshold: 0
  })

  const [a, b] = bases

  const unreplicate = replicate([a, b])
  t.teardown(unreplicate)

  await addWriterAndSync(a, b)
  await confirm([a, b])

  await b.append('b0')

  t.is(a.view.signedLength, 0)
  t.is(b.view.signedLength, 0)

  await new Promise(resolve => setTimeout(resolve, 500))

  t.not(a.local.length, 1)
  t.not(b.local.length, 1)

  t.is(await getIndexedViewLength(a), 1)
  t.is(await getIndexedViewLength(b), 1)
})

// TODO: unflake this test (skipping to avoid false positives on canary)
test.skip('autoack - 5 writers', async t => {
  t.plan(21)

  const ackInterval = 50

  const { bases } = await create(5, t, {
    ackInterval,
    ackThreshold: 0
  })

  const [a, b, c, d, e] = bases

  t.teardown(replicate(bases))

  addWriter(a, b)
  addWriter(a, c)
  addWriter(a, d)
  await addWriter(a, e)

  await sync(bases)

  t.not(e.linearizer.indexers.length, 5)

  await b.append('b0')

  t.is(a.view.signedLength, 0)
  t.is(b.view.signedLength, 0)
  t.is(c.view.signedLength, 0)
  t.is(d.view.signedLength, 0)
  t.is(e.view.signedLength, 0)

  await poll(() => bases.reduce((acc, b) => acc && b.view.signedLength === 1, true), ackInterval)

  // allow acks to settle
  await new Promise(resolve => setTimeout(resolve, 10 * ackInterval))

  const alen = a.local.length
  const blen = b.local.length
  const clen = c.local.length
  const dlen = d.local.length
  const elen = e.local.length

  await new Promise(resolve => setTimeout(resolve, 10 * ackInterval))

  // check that acks stop
  t.is(a.local.length, alen)
  t.is(b.local.length, blen)
  t.is(c.local.length, clen)
  t.is(d.local.length, dlen)
  t.is(e.local.length, elen)

  t.is(a.linearizer.indexers.length, 5)
  t.is(b.linearizer.indexers.length, 5)
  t.is(c.linearizer.indexers.length, 5)
  t.is(d.linearizer.indexers.length, 5)
  t.is(e.linearizer.indexers.length, 5)

  t.is(a.view.signedLength, 1)
  t.is(b.view.signedLength, 1)
  t.is(c.view.signedLength, 1)
  t.is(d.view.signedLength, 1)
  t.is(e.view.signedLength, 1)
})

// skipping cause flaking
test.skip('autoack - concurrent', async t => {
  t.plan(10)

  const ackInterval = 100

  const { bases } = await create(5, t, {
    ackInterval,
    ackThreshold: 0
  })

  const [a, b, c, d, e] = bases

  t.teardown(replicate(bases))

  addWriter(a, b)
  addWriter(a, c)
  addWriter(a, d)
  await addWriter(a, e)

  await sync(bases)

  await Promise.all(bases.map(n => message(n, 10)))

  await new Promise(resolve => setTimeout(resolve, 40 * ackInterval))

  // TODO: autoack should ensure views are fully signed

  t.is(await getIndexedViewLength(a), 50)
  t.is(await getIndexedViewLength(b), 50)
  t.is(await getIndexedViewLength(c), 50)
  t.is(await getIndexedViewLength(d), 50)
  t.is(await getIndexedViewLength(e), 50)

  const alen = a.local.length
  const blen = b.local.length
  const clen = c.local.length
  const dlen = d.local.length
  const elen = e.local.length

  await new Promise(resolve => setTimeout(resolve, 40 * ackInterval))

  t.is(a.local.length, alen)
  t.is(b.local.length, blen)
  t.is(c.local.length, clen)
  t.is(d.local.length, dlen)
  t.is(e.local.length, elen)

  async function message (w, n) {
    for (let i = 0; i < n; i++) {
      await w.append(w.local.key.toString('hex').slice(0, 2) + n)
    }
  }
})

test.skip('autoack - threshold', async t => {
  t.plan(2)

  const { bases } = await create(2, t, {
    ackInterval: 0,
    ackThreshold: 1
  })

  const [a, b] = bases

  t.teardown(await replicate([a, b]))

  await addWriterAndSync(a, b)

  await sync([a, b])

  for (let i = 0; i < 4; i++) b.append('b')

  await sync([a, b])
  await eventFlush() // allow the bg threshold to react...
  for (let i = 0; i < 4; i++) await b.append('b')
  await sync([a, b])

  t.ok(a.view.signedLength >= 4)
  t.ok(b.view.signedLength >= 4)
})

test.skip('autoack - threshold with interval', async t => {
  t.plan(3)

  const ackInterval = 800
  const { bases } = await create(2, t, {
    ackInterval,
    ackThreshold: 1
  })

  const [a, b] = bases

  t.teardown(await replicate([a, b]))

  await addWriterAndSync(a, b)

  await sync([a, b])

  b.append('b0')
  b.append('b1')
  b.append('b2')
  await b.append('b3')
  await new Promise(resolve => setTimeout(resolve, 1000)) // fast ack
  await sync([a, b])
  await eventFlush() // allow the bg threshold to react...

  await b.append('b4')
  await b.append('b5')
  await b.append('b6')
  await b.append('b7')
  await sync([a, b])
  await new Promise(resolve => setTimeout(resolve, 2000)) // fast ack

  t.is(a._ackTimer.interval, ackInterval)
  t.ok(a.view.signedLength >= 4)
  t.ok(b.view.signedLength >= 4)
})

test('autoack - no null acks', async t => {
  t.plan(2)

  const { bases } = await create(2, t, {
    ackInterval: 10,
    ackThreshold: 0
  })

  const [a, b] = bases

  t.teardown(await replicate([a, b]))

  await addWriterAndSync(a, b)
  await confirm([a, b])

  await a.append(null)

  const alen = a.local.length
  const blen = b.local.length

  setTimeout(() => {
    t.is(a.local.length, alen)
    t.is(b.local.length, blen)
  }, 1000)
})

test('autoack - value beneath null values', async t => {
  t.plan(4)

  const { bases } = await create(2, t, {
    ackInterval: 10,
    ackThreshold: 0
  })

  const [a, b] = bases

  t.teardown(await replicate([a, b]))

  await addWriterAndSync(a, b)
  await confirm([a, b])

  await b.append('b0')
  await b.append(null) // place null value above tail

  await sync([a, b])

  const alen = a.local.length
  const blen = b.local.length

  await new Promise(resolve => setTimeout(resolve, 1000))

  t.not(a.local.length, alen) // a should ack
  t.is(b.local.length, blen) // b0 is indexed by a's ack (all indexes acked)

  t.is(await getIndexedViewLength(a), a.view.length)
  t.is(b.view.length, a.view.length)
})

test('autoack - merge', async t => {
  t.plan(2)

  const { bases } = await create(2, t, {
    ackInterval: 0,
    ackThreshold: 0
  })

  const [a, b] = bases

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

  const { bases } = await create(3, t, {
    ackInterval: 0,
    ackThreshold: 0
  })

  const [a, b, c] = bases

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

  const { bases } = await create(3, t, {
    ackInterval: 0,
    ackThreshold: 0
  })

  const [a, b, c] = bases

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

  const { bases } = await create(3, t, {
    ackInterval: 0,
    ackThreshold: 0
  })

  const [a, b, c] = bases

  t.teardown(await replicate([a, b, c]))

  await addWriter(a, b)
  await addWriter(a, c)

  await eventFlush()

  await new Promise(resolve => setTimeout(resolve, 500))

  t.ok(b.local.length > 0)
  t.ok(c.local.length > 0)

  t.is(a.system.indexers.length, 3)
  t.is(b.system.indexers.length, 3)
  t.is(c.system.indexers.length, 3)
})

test.skip('autoack - pending migrates', async t => {
  t.plan(3)

  const { bases } = await create(2, t, {
    ackInterval: 0,
    ackThreshold: 0
  })

  const [a, b] = bases

  t.teardown(await replicate([a, b]))

  await addWriter(a, b)
  await eventFlush()

  await new Promise(resolve => setTimeout(resolve, 100))

  await sync([a, b])

  await a.append(null)

  await sync([a, b])

  t.ok(b.local.length > 0)

  // TODO: prop should be absent here?
  t.ok(a._viewStore.shouldMigrate())
  t.ok(b._viewStore.shouldMigrate())
})

test('autoack - minority indexers who are both tails', async t => {
  const { bases } = await create(4, t, {
    ackInterval: 10,
    ackThreshold: 0
  })

  const [a, b, c, d] = bases

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

  await new Promise(resolve => setTimeout(resolve, 1000))

  const alen = a.local.length
  const blen = b.local.length

  const asize = a.linearizer.size
  const bsize = b.linearizer.size

  await new Promise(resolve => setTimeout(resolve, 1000))

  t.absent(b.linearizer.shouldAck(b.localWriter))

  t.is(a.local.length, alen)
  t.is(a.linearizer.size, asize)

  t.is(b.local.length, blen)
  t.is(b.linearizer.size, bsize)
})

test('autoack - minority indexers with non-indexer tails', async t => {
  const { bases } = await create(5, t, {
    ackInterval: 10,
    ackThreshold: 0
  })

  const [a, b, c, d, e] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(a, c)
  await addWriterAndSync(a, d)
  await addWriterAndSync(a, e, false)

  await replicateAndSync([a, b, c, d])
  await b.append(null)
  await c.append(null)
  await d.append(null)

  await confirm([a, b, c, d])

  // this will get confirmed
  await a.append(null)

  await replicateAndSync([a, b]) // e is inert, use them to "one way" sync

  // we want these two left as tails
  await e.append('e0')
  await a.append('a0')
  await b.append('b0')

  await replicateAndSync([b, c])
  await c.append(null)
  await replicateAndSync([c, d])
  await d.append(null)
  await replicateAndSync([b, d])
  await b.append(null)

  // now we keep c and d offline
  t.teardown(await replicate([a, b, e]))

  await new Promise(resolve => setTimeout(resolve, 1000))

  const alen = a.local.length
  const blen = b.local.length

  const asize = a.linearizer.size
  const bsize = b.linearizer.size

  await new Promise(resolve => setTimeout(resolve, 1000))

  t.absent(b.linearizer.shouldAck(b.localWriter))

  t.is(a.local.length, alen)
  t.is(a.linearizer.size, asize)

  t.is(b.local.length, blen)
  t.is(b.linearizer.size, bsize)
})

function getWriter (base, writer) {
  return base.activeWriters.get(writer.core.key)
}

function poll (fn, interval) {
  return new Promise(resolve => {
    const int = setInterval(check, interval)

    function check () {
      if (fn()) done()
    }

    function done () {
      clearInterval(int)
      resolve()
    }
  })
}

async function getIndexedViewLength (base, index = -1) {
  const info = await base.getIndexedInfo()
  if (index === -1) index = info.views.length - 1
  return info.views[index] ? info.views[index].length : 0
}
