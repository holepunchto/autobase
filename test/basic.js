const test = require('brittle')
const ram = require('random-access-memory')
const Corestore = require('corestore')
const b4a = require('b4a')

const Autobase = require('..')

const {
  create,
  sync,
  apply,
  addWriter,
  confirm,
  compare
} = require('./helpers')

test('basic - two writers', async t => {
  const [base1, base2, base3] = await create(3, apply)

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await confirm([base1, base2, base3])

  await base2.append({
    add: base3.local.key.toString('hex'),
    debug: 'this is adding c'
  })

  await confirm([base1, base2, base3])

  t.is(base2.system.digest.writers.length, 3)
  t.is(base2.system.digest.writers.length, base3.system.digest.writers.length)
  t.is(base2.system.digest.writers.length, base2.writers.length)
  t.is(base3.system.digest.writers.length, base3.writers.length)

  // tests skipped: fix with linearizer update - batching

  // t.alike(await base1.system.checkpoint(), await base2.system.checkpoint())
  // t.alike(await base1.system.checkpoint(), await base3.system.checkpoint())
})

test('basic - view', async t => {
  const [base] = await create(1, apply, store => store.get('test', { valueEncoding: 'json' }))

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.system.digest.writers.length, 1)
  t.is(base.view.indexedLength, 1)
  t.alike(await base.view.get(0), block)
})

test('basic - view with close', async t => {
  const [base] = await create(1, apply, open, close)

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.system.digest.writers.length, 1)
  t.is(base.view.core.indexedLength, 1)
  t.alike(await base.view.core.get(0), block)

  t.is(base.view.lastBlock, null)
  await base.close()
  t.is(base.view.lastBlock.message, 'hello, world!')

  function open (store) {
    const core = store.get('test', { valueEncoding: 'json' })
    return {
      core,
      lastBlock: null,
      append: v => core.append(v)
    }
  }

  async function close (view) {
    view.lastBlock = await view.core.get(view.core.length - 1)
    await view.core.close()
  }
})

test('basic - view/writer userdata is set', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))
  const [base1, base2] = bases

  await base1.append({ add: base2.local.key.toString('hex') })

  await confirm(bases)

  await verifyUserData(base1)
  await verifyUserData(base2)

  async function verifyUserData (base) {
    const viewData = await Autobase.getUserData(base.view)
    const systemData = await Autobase.getUserData(base.system.core)

    t.alike(systemData.referrer, base.bootstrap)
    t.alike(viewData.referrer, base.bootstrap)
    t.is(viewData.view, 'test')

    t.is(base.writers.length, 2)
    for (const writer of base.writers) {
      const writerData = await Autobase.getUserData(writer.core)
      t.alike(writerData.referrer, base.bootstrap)
    }
  }
})

test('basic - compare views', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b] = bases
  await a.append({ add: b.local.key.toString('hex') })

  await confirm(bases)

  for (let i = 0; i < 6; i++) await bases[i % 2].append('msg' + i)

  await confirm(bases)

  t.is(a.system.digest.writers.length, b.system.digest.writers.length)
  t.is(a.view.indexedLength, b.view.indexedLength)

  try {
    await compare(a, b)
  } catch (e) {
    t.fail(e.message)
  }
})

test('basic - online majority', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  await a.append({ add: b.local.key.toString('hex') })
  await a.append({ add: c.local.key.toString('hex') })

  await confirm(bases)

  await a.append({ message: 'a0' })
  await b.append({ message: 'b0' })
  await c.append({ message: 'c0' })

  await confirm(bases)

  const indexed = a.view.indexedLength

  await a.append({ message: 'a1' })
  await b.append({ message: 'b1' })
  await c.append({ message: 'c1' })
  await a.append({ message: 'a2' })
  await b.append({ message: 'b2' })
  await c.append({ message: 'c2' })

  await confirm([a, b])

  t.not(a.view.indexedLength, indexed)
  t.is(c.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)
  try {
    await compare(a, b)
  } catch (e) {
    t.fail(e.message)
  }

  await sync([b, c])

  t.is(a.view.indexedLength, c.view.indexedLength)

  try {
    await compare(a, c)
  } catch (e) {
    t.fail(e.message)
  }
})

test('basic - rotating majority', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  await a.append({ add: b.local.key.toString('hex') })
  await a.append({ add: c.local.key.toString('hex') })

  await confirm(bases)

  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })

  await confirm(bases)

  let indexed = a.view.indexedLength

  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })
  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })

  await confirm([a, b])

  t.not(a.view.indexedLength, indexed)
  t.is(c.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)

  indexed = a.view.indexedLength

  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })
  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })

  await confirm([b, c])

  t.not(b.view.indexedLength, indexed)
  t.is(a.view.indexedLength, indexed)
  t.is(b.view.indexedLength, c.view.indexedLength)

  indexed = b.view.indexedLength

  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })
  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })

  await confirm([a, c])

  t.not(c.view.indexedLength, indexed)
  t.is(b.view.indexedLength, indexed)
  t.is(a.view.indexedLength, c.view.indexedLength)

  indexed = a.view.indexedLength

  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })
  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })

  await confirm(bases)

  t.not(a.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(a.view.indexedLength, c.view.indexedLength)

  try {
    await compare(a, b)
    await compare(a, c)
  } catch (e) {
    t.fail(e.message)
  }
})

test('basic - throws', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b] = bases

  await a.append({ message: 'msg1' })
  await a.append({ message: 'msg2' })
  await a.append({ message: 'msg3' })

  await confirm([a, b])

  await t.exception(b.append({ message: 'not writable' }))
  await t.exception(a.view.append({ message: 'append outside apply' }))
  t.exception(() => a.system.addWriter(b.local.key))
})

test('basic - add 5 writers', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  await a.append({ add: b.local.key.toString('hex') })
  await a.append({ add: c.local.key.toString('hex') })
  await a.append({ add: d.local.key.toString('hex') })
  await a.append({ add: e.local.key.toString('hex') })

  await confirm(bases)

  t.is(a.writers.length, 5)
  t.is(a.system.digest.writers.length, 5)

  t.is(a.writers.length, b.writers.length)
  t.is(a.system.digest.writers.length, b.system.digest.writers.length)

  t.is(a.writers.length, c.writers.length)
  t.is(a.system.digest.writers.length, c.system.digest.writers.length)

  t.is(a.writers.length, d.writers.length)
  t.is(a.system.digest.writers.length, d.system.digest.writers.length)

  t.is(a.writers.length, e.writers.length)
  t.is(a.system.digest.writers.length, e.system.digest.writers.length)
})

test('basic - online minorities', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  await a.append({ add: b.local.key.toString('hex') })
  await a.append({ add: c.local.key.toString('hex') })
  await a.append({ add: d.local.key.toString('hex') })
  await a.append({ add: e.local.key.toString('hex') })

  await confirm(bases)

  t.is(a.view.indexedLength, c.view.indexedLength)

  await a.append({ message: 'msg0' })
  await b.append({ message: 'msg1' })
  await c.append({ message: 'msg2' })
  await d.append({ message: 'msg3' })
  await e.append({ message: 'msg4' })
  await a.append({ message: 'msg5' })
  await b.append({ message: 'msg6' })
  await c.append({ message: 'msg7' })
  await d.append({ message: 'msg8' })
  await e.append({ message: 'msg9' })

  await a.append({ message: 'msg10' })
  await b.append({ message: 'msg11' })
  await a.append({ message: 'msg12' })
  await b.append({ message: 'msg13' })
  await a.append({ message: 'msg14' })

  await d.append({ message: 'msg15' })
  await c.append({ message: 'msg16' })
  await d.append({ message: 'msg17' })
  await c.append({ message: 'msg18' })
  await d.append({ message: 'msg19' })
  await c.append({ message: 'msg20' })
  await d.append({ message: 'msg21' })
  await c.append({ message: 'msg22' })

  await confirm([a, b])
  await confirm([c, d])

  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(c.view.indexedLength, d.view.indexedLength)

  t.not(a.view.length, a.view.indexedLength)
  t.is(a.view.length, b.view.length)
  t.not(c.view.length, a.view.length)
  t.is(c.view.length, d.view.length)

  try {
    await compare(a, b, true)
    await compare(c, d, true)
  } catch (e) {
    t.fail(e.message)
  }

  await confirm(bases)

  t.is(a.view.length, c.view.length)
  t.is(a.view.indexedLength, c.view.indexedLength)

  try {
    await compare(a, b, true)
    await compare(a, c, true)
    await compare(a, d, true)
    await compare(a, e, true)
  } catch (e) {
    t.fail(e.message)
  }
})

test('basic - restarting sets bootstrap correctly', async t => {
  const store = new Corestore(ram.reusable())

  let bootstrapKey = null
  let localKey = null

  {
    const ns = store.namespace('random-name')
    const base = new Autobase(ns, null, {})
    await base.ready()

    bootstrapKey = base.bootstrap
    localKey = base.local.key

    await base.close()
  }

  {
    const ns = store.namespace(bootstrapKey)
    const base = new Autobase(ns, bootstrapKey, {})
    await base.ready()

    t.alike(base.bootstrap, bootstrapKey)
    t.alike(base.local.key, base.bootstrap)
    t.alike(base.local.key, localKey)
  }
})

test('batch append', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b] = bases
  a.on('error', (e) => console.error(e))
  b.on('error', (e) => console.error(e))

  await addWriter(a, b)

  await confirm(bases)

  await a.append(['a0', 'a1'])
  await t.execution(confirm(bases))
})

test('undoing a batch', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b] = bases
  a.on('error', (e) => console.error(e))
  b.on('error', (e) => console.error(e))

  await addWriter(a, b)

  await confirm(bases)

  await a.append('a0')
  await confirm(bases)

  await Promise.all([
    a.append('a1'),
    b.append(['b0', 'b1'])
  ])

  await t.execution(confirm(bases))
})

test('append during restart', async t => {
  const bases = await create(4, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d] = bases

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await sync(bases)

  t.ok(!!a.localWriter)
  t.ok(!!b.localWriter)
  t.ok(!!c.localWriter)
  t.ok(!!d.localWriter)

  await addWriter(b, d)

  const s1 = b.store.replicate(true)
  const s2 = d.store.replicate(false)

  s1.on('error', () => {})
  s2.on('error', () => {})

  s1.pipe(s2).pipe(s1)

  for (const w of d.writers) {
    if (w === d.localWriter) continue

    await w.core.update({ wait: true })
    const start = w.core.length
    const end = w.core.core.tree.length

    await w.core.download({ start, end, ifAvailable: true }).done()
  }

  await d.append('hello')

  t.is(await d.view.get(d.view.length - 1), 'hello')
})

test('closing an autobase', async t => {
  const [base] = await create(1, apply, store => store.get('test'))

  // Sanity check
  t.is(base.local.closed, false)

  await base.close()
  t.is(base.local.closed, true)
})

test('flush after restart', async t => {
  const bases = await create(9, apply, store => store.get('test', { valueEncoding: 'json' }))

  const root = bases[0]
  const adds = []
  let msg = 0

  adds.push(addWriter(root, bases[1]))
  adds.push(addWriter(root, bases[2]))
  adds.push(addWriter(root, bases[3]))
  adds.push(addWriter(root, bases[4]))
  adds.push(addWriter(root, bases[5]))
  adds.push(addWriter(root, bases[6]))
  adds.push(addWriter(root, bases[7]))
  adds.push(addWriter(root, bases[8]))

  await Promise.all(adds)
  await sync(bases)

  await t.execution(bases[0].append('msg' + msg++))
  await t.execution(bases[1].append('msg' + msg++))
  await t.execution(bases[2].append('msg' + msg++))
  await t.execution(bases[3].append('msg' + msg++))
  await t.execution(bases[4].append('msg' + msg++))
  await t.execution(bases[5].append('msg' + msg++))
  await t.execution(bases[6].append('msg' + msg++))
  await t.execution(bases[7].append('msg' + msg++))
  await t.execution(bases[8].append('msg' + msg++))
})

test('restart', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  let msg = 0

  await Promise.all([
    addWriter(a, b),
    addWriter(a, c)
  ])

  await confirm([a, b])

  // a sends message
  await a.append('a:' + msg++)

  // b and c add writer
  await addWriter(b, bases[3])
  await confirm([b, c], 3)

  t.is(b.system.digest.writers.length, 4)

  // trigger restart for a
  await sync([a, b, c, d])

  await a.append('a:' + msg++)
  await b.append('b:' + msg++)
  await c.append('c:' + msg++)
  await d.append('d:' + msg++)

  await sync([a, b, c, d])

  // d sends message
  await d.append('d:' + msg++)

  // a, b and c add writer
  await addWriter(a, e)
  await confirm([a, b, c], 4)

  t.is(b.system.digest.writers.length, 5)

  // trigger restart for a
  await sync([a, b, c, d, e])

  t.is(a.system.digest.heads.length, 1)
  t.is(a.system.digest.writers.length, bases.length)

  t.not(a.view.indexedLength, a.view.length)

  for (let i = 1; i < bases.length; i++) {
    const [l, r] = [bases[0], bases[i]]

    t.is(l.view.indexedLength, r.view.indexedLength)
    t.is(l.view.length, r.view.length)
  }
})

test('sequential restarts', async t => {
  const bases = await create(9, apply, store => store.get('test', { valueEncoding: 'json' }))

  const root = bases[0]
  const adds = []
  let msg = 0

  adds.push(addWriter(root, bases[1]))
  adds.push(addWriter(root, bases[2]))

  await Promise.all(adds)
  await confirm(bases.slice(0, 3))

  for (let i = 2; i < bases.length + 5; i++) {
    const appends = []

    const syncers = bases.slice(0, i + 1)
    const [isolated] = syncers.splice(i - 1, 1)

    // confirm over this node
    // include all dag except previous addWriter
    await sync(syncers)
    await bases[0].append(null)
    await sync(syncers)

    // everyone writes a message
    for (let j = 0; j < Math.min(i, bases.length); j++) {
      appends.push(bases[j].append('msg' + msg++))
    }

    await Promise.all(appends)
    await sync(syncers)

    // unsynced writer adds a node
    if (i < bases.length) {
      const newguy = bases[i]
      await addWriter(isolated, newguy)

      await sync([isolated, newguy])
    }

    if (i % 2 === 1) {
      if (i < bases.length) {
        t.is(
          bases[0].linearizer.indexers.length,
          bases[i - 1].linearizer.indexers.length
        )
      }
    }
  }

  t.is(bases[0].system.digest.heads.length, bases.length)
  t.is(bases[0].system.digest.writers.length, bases.length)

  t.not(bases[0].view.indexedLength, 0)
  t.not(bases[0].view.indexedLength, bases[0].view.length)

  await sync(bases)

  for (let i = 1; i < bases.length; i++) {
    t.is(bases[0].view.indexedLength, bases[i].view.indexedLength)
    t.is(bases[0].view.length, bases[i].view.length)
  }
})

// test should throw for commit f41f5544dbe8743f6ad3b9886e7aa472344bc9c7 (with debug set to false)
test.skip('consistent writers', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  await addWriter(a, b)
  await sync(bases)

  await addWriter(b, c)
  await addWriter(b, d)

  await sync(bases)

  // trigger reorg
  addWriter(a, e)

  const s1 = a.store.replicate(true)
  const s2 = d.store.replicate(false)

  s1.on('error', () => {})
  s2.on('error', () => {})

  s1.pipe(s2).pipe(s1)

  for (const w of d.writers) {
    if (w === d.localWriter) continue

    await w.core.update({ wait: true })
    const start = w.core.length
    const end = w.core.core.tree.length

    await w.core.download({ start, end, ifAvailable: true }).done()
  }

  d.debug = true
  d.update().then(() => { d.debug = false })

  t.ok(await checkWriters(d))

  async function checkWriters (b) {
    while (b.debug) {
      for (const w of b.writers) {
        for (const rem of b._removedWriters) {
          if (b4a.equals(w.core.key, rem.core.key)) return false
        }
      }

      await new Promise(resolve => setImmediate(resolve))
    }

    return true
  }
})

test('basic - pass exisiting store', async t => {
  const [base1] = await create(1, apply)

  const store = new Corestore(ram.reusable(), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const ns2 = store.namespace('1')
  const base2 = new Autobase(ns2, base1.local.key, { apply, valueEncoding: 'json' })
  await base2.ready()

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await base1.append({
    value: 'base1'
  })

  await confirm([base1, base2])

  await base2.append({
    value: 'base2'
  })

  await confirm([base1, base2])

  t.is(base2.system.digest.writers.length, 2)

  await base2.close()

  const ns3 = store.namespace('1')
  const base3 = new Autobase(ns3, base1.local.key, { apply, valueEncoding: 'json' })
  await base3.ready()

  await base3.update({ wait: false })

  t.is(base3.system.digest.writers.length, 2)
})

test.skip('two writers write many messages, third writer joins', async t => {
  // TODO: test this passes with next linearliser version
  const [base1, base2, base3] = await create(3, apply)

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding writer 2'
  })

  for (let i = 0; i < 10000; i++) {
    base1.append({ value: `Message${i}` })
  }

  await confirm([base1, base2])

  await base1.append({
    add: base3.local.key.toString('hex'),
    debug: 'this is adding writer 3'
  })

  await confirm([base1, base2, base3])
  t.pass('Confirming did not throw RangeError: Maximum call stack size exceeded')
})

test('basic - gc indexed nodes', async t => {
  const [base] = await create(1, apply, store => store.get('test', { valueEncoding: 'json' }))

  await base.append({ message: '0' })
  await base.append({ message: '1' })
  await base.append({ message: '2' })
  await base.append({ message: '3' })
  await base.append({ message: '4' })

  t.is(base.system.digest.writers.length, 1)
  t.is(base.view.indexedLength, 5)
  t.is(base.localWriter.nodes.size, 0)

  t.alike(await base.view.get(0), { message: '0' })
  t.alike(await base.view.get(1), { message: '1' })
  t.alike(await base.view.get(2), { message: '2' })
  t.alike(await base.view.get(3), { message: '3' })
  t.alike(await base.view.get(4), { message: '4' })
})

test('basic - isAutobase', async t => {
  const [base1, base2, base3] = await create(3, apply)

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await confirm([base1, base2, base3])

  await base2.append({
    add: base3.local.key.toString('hex'),
    debug: 'this is adding c'
  })

  await confirm([base1, base2, base3])

  t.is(await Autobase.isAutobase(base1.local), true)
  t.is(await Autobase.isAutobase(base2.local), true)
  await t.exception(Autobase.isAutobase(base3.local, { wait: false }))

  await base3.append('hello')

  t.is(await Autobase.isAutobase(base3.local), true)
})
