const test = require('brittle')
const ram = require('random-access-memory')
const Corestore = require('corestore')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')

const Autobase = require('..')

const {
  create,
  replicateAndSync,
  apply,
  addWriter,
  confirm,
  replicate,
  compare
} = require('./helpers')

test('basic - two writers', async t => {
  const [base1, base2, base3] = await create(3, apply)

  await addWriter(base1, base2)

  await confirm([base1, base2, base3])

  await addWriter(base2, base3)

  await confirm([base1, base2, base3])

  t.is(base2.system.members, 3)
  t.is(base2.system.members, base3.system.members)
  t.is(base2.system.members, base2.activeWriters.size)
  t.is(base3.system.members, base3.activeWriters.size)

  // tests skipped: fix with linearizer update - batching

  // t.alike(await base1.system.checkpoint(), await base2.system.checkpoint())
  // t.alike(await base1.system.checkpoint(), await base3.system.checkpoint())
})

test('basic - writable event fires', async t => {
  t.plan(1)
  const [base1, base2] = await create(2, apply)

  base2.on('writable', () => {
    t.ok(base2.writable, 'Writable event fired when autobase writable')
  })

  await addWriter(base1, base2)

  await confirm([base1, base2])
})

test('basic - local key pair', async t => {
  const keyPair = crypto.keyPair()
  const [base] = await create(1, apply, store => store.get('test', { valueEncoding: 'json' }), null, { keyPair })

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.view.indexedLength, 1)
  t.alike(await base.view.get(0), block)
  t.is(base.local.manifest.signer.publicKey, keyPair.publicKey)
})

test('basic - view', async t => {
  const [base] = await create(1, apply, store => store.get('test', { valueEncoding: 'json' }))

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.system.members, 1)
  t.is(base.view.indexedLength, 1)
  t.alike(await base.view.get(0), block)
})

test('basic - view with close', async t => {
  const [base] = await create(1, apply, open, close)

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.system.members, 1)
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

  await addWriter(base1, base2)

  await confirm(bases)

  await verifyUserData(base1)
  await verifyUserData(base2)

  async function verifyUserData (base) {
    const systemData = await Autobase.getUserData(base.system.core)

    t.alike(systemData.referrer, base.bootstrap)

    t.is(base.activeWriters.size, 2)
    for (const writer of base.activeWriters) {
      const writerData = await Autobase.getUserData(writer.core)
      t.alike(writerData.referrer, base.bootstrap)
    }
  }
})

test('basic - compare views', async t => {
  const bases = await create(2, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b] = bases
  await addWriter(a, b)

  await confirm(bases)

  for (let i = 0; i < 6; i++) await bases[i % 2].append('msg' + i)

  await confirm(bases)

  t.is(a.system.members, b.system.members)
  t.is(a.view.indexedLength, b.view.indexedLength)

  await t.execution(compare(a, b))
})

test('basic - online majority', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  await addWriter(a, b)
  await addWriter(a, c)

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
  await t.execution(compare(a, b))

  await replicateAndSync([b, c])

  t.is(a.view.indexedLength, c.view.indexedLength)

  await t.execution(compare(a, c))
})

test('basic - rotating majority', async t => {
  const bases = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c] = bases

  await addWriter(a, b)
  await addWriter(a, c)

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

  await t.execution(compare(a, b))
  await t.execution(compare(a, c))
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
  await t.exception(() => a.addWriter(b.local.key))
})

test('basic - add 5 writers', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

  await confirm(bases)

  t.is(a.activeWriters.size, 5)
  t.is(a.system.members, 5)

  t.is(a.activeWriters.size, b.activeWriters.size)
  t.is(a.system.members, b.system.members)

  t.is(a.activeWriters.size, c.activeWriters.size)
  t.is(a.system.members, c.system.members)

  t.is(a.activeWriters.size, d.activeWriters.size)
  t.is(a.system.members, d.system.members)

  t.is(a.activeWriters.size, e.activeWriters.size)
  t.is(a.system.members, e.system.members)
})

test('basic - online minorities', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await addWriter(a, e)

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

  await t.execution(compare(a, b, true))
  await t.execution(compare(c, d, true))

  await confirm(bases)

  t.is(a.view.length, c.view.length)
  t.is(a.view.indexedLength, c.view.indexedLength)

  await t.execution(compare(a, b, true))
  await t.execution(compare(a, c, true))
  await t.execution(compare(a, d, true))
  await t.execution(compare(a, e, true))
})

test('basic - restarting sets bootstrap correctly', async t => {
  const store = new Corestore(ram.reusable())

  let bootstrapKey = null
  let localKey = null

  {
    const ns = store.namespace('random-name')
    const base = new Autobase(ns, null, { ackInterval: 0, ackThreshold: 0 })
    await base.ready()

    bootstrapKey = base.bootstrap
    localKey = base.local.key

    await base.close()
  }

  {
    const ns = store.namespace(bootstrapKey)
    const base = new Autobase(ns, bootstrapKey, { ackInterval: 0, ackThreshold: 0 })
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

test('append during reindex', async t => {
  const bases = await create(4, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d] = bases

  await addWriter(a, b)
  await addWriter(a, c)
  await addWriter(a, d)
  await replicateAndSync(bases)

  t.ok(!!a.localWriter)
  t.ok(!!b.localWriter)
  t.ok(!!c.localWriter)
  t.ok(!!d.localWriter)

  await addWriter(b, d)

  const unreplicate = replicate([b, d])

  for (const w of d.activeWriters) {
    if (w === d.localWriter) continue

    const start = w.core.contiguousLength
    const end = w.core.length

    await w.core.download({ start, end }).done()
  }

  await d.append('hello')

  t.is(await d.view.get(d.view.length - 1), 'hello')

  await unreplicate()
})

test('closing an autobase', async t => {
  const [base] = await create(1, apply, store => store.get('test'))

  // Sanity check
  t.is(base.local.closed, false)

  await base.close()
  t.is(base.local.closed, true)
})

test('flush after reindex', async t => {
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
  await replicateAndSync(bases)

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

test('reindex', async t => {
  const bases = await create(5, apply, store => store.get('test', { valueEncoding: 'json' }))

  const [a, b, c, d, e] = bases

  let msg = 0

  await Promise.all([
    addWriter(a, b),
    addWriter(a, c)
  ])

  await confirm([a, b, c])

  // a sends message
  await a.append('a:' + msg++)

  // b and c add writer
  await addWriter(b, d)
  await confirm([b, c, d], { majority: 2 })

  t.is(b.system.members, 4)

  // trigger reindex for a
  await replicateAndSync([a, b, c, d])

  await a.append('a:' + msg++)
  await b.append('b:' + msg++)
  await c.append('c:' + msg++)
  await d.append('d:' + msg++)

  await replicateAndSync([a, b, c, d])

  // d sends message
  await d.append('d:' + msg++)

  // a, b and c add writer
  await addWriter(a, e)
  await confirm([a, b, c, e], { majority: 3 })

  t.is(b.system.members, 5)

  // trigger reindex for a
  await replicateAndSync([a, b, c, d, e])

  t.is(a.system.heads.length, 2)
  t.is((await a.system.getIndexedInfo()).heads.length, 1, 'only one indexed head')
  t.is(a.system.members, bases.length)

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

  for (let i = 2; i < bases.length + 6; i++) {
    const appends = []

    const syncers = bases.slice(0, i + 1)
    const [isolated] = syncers.splice(i - 1, 1)

    // confirm over this node
    // include all dag except previous addWriter
    await replicateAndSync(syncers)
    await bases[0].append(null)
    await replicateAndSync(syncers)

    // everyone writes a message
    for (let j = 0; j < Math.min(i, bases.length); j++) {
      appends.push(bases[j].append('msg' + msg++))
    }

    await Promise.all(appends)
    await replicateAndSync(syncers)

    // unsynced writer adds a node
    if (i < bases.length) {
      const newguy = bases[i]
      await addWriter(isolated, newguy)

      await replicateAndSync([isolated, newguy])
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

  // only bases[0] node should be head
  t.is(bases[0].system.heads.length, 9)
  const indexedHeads = (await bases[0].system.getIndexedInfo()).heads
  t.is(indexedHeads.length, 1)
  t.alike(indexedHeads[0].key, bases[0].local.key)

  t.is(bases[0].system.members, bases.length)

  t.not(bases[0].view.indexedLength, 0)
  t.not(bases[0].view.indexedLength, bases[0].view.length)

  await replicateAndSync(bases)

  for (let i = 1; i < bases.length; i++) {
    t.is(bases[i].system.core._source.queued, -1)
    t.alike(bases[0].system.core.key, bases[i].system.core.key)
    t.alike(bases[0].view.key, bases[i].view.key)
    t.is(bases[0].view.indexedLength, bases[i].view.indexedLength)
    t.is(bases[0].view.length, bases[i].view.length)
    t.alike(await bases[0].system.core.getBackingCore().treeHash(), await bases[i].system.core.getBackingCore().treeHash())
    t.alike(await bases[0].view.getBackingCore().treeHash(), await bases[i].view.getBackingCore().treeHash())
  }
})

test('two writers write many messages, third writer joins', async t => {
  const [base1, base2, base3] = await create(3, apply, store => store.get('test', { valueEncoding: 'json' }))

  await addWriter(base1, base2)

  for (let i = 0; i < 10000; i++) {
    base1.append({ value: `Message${i}` })
  }

  await confirm([base1, base2])

  await addWriter(base1, base3)

  await confirm([base1, base2, base3])
  t.pass('Confirming did not throw')

  await t.execution(compare(base1, base2))
  await t.execution(compare(base1, base3))
})

test('basic - gc indexed nodes', async t => {
  const [base] = await create(1, apply, store => store.get('test', { valueEncoding: 'json' }))

  await base.append({ message: '0' })
  await base.append({ message: '1' })
  await base.append({ message: '2' })
  await base.append({ message: '3' })
  await base.append({ message: '4' })

  t.is(base.system.members, 1)
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

  await addWriter(base1, base2)

  await confirm([base1, base2, base3])

  await t.exception(Autobase.isAutobase(base3.local, { wait: false }))

  await addWriter(base2, base3)

  await confirm([base1, base2, base3])

  t.is(await Autobase.isAutobase(base1.local), true)
  t.is(await Autobase.isAutobase(base2.local), true)

  await base3.append('hello')

  t.is(await Autobase.isAutobase(base3.local), true)
})

test('basic - catch apply throws', async t => {
  t.plan(1)

  const [a] = await create(1, apply, store => store.get('test', { valueEncoding: 'json' }))
  const b = new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(1) }), a.local.key, {
    apply: applyThrow,
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0
  })

  b.on('error', err => {
    t.pass(!!err)
  })

  await b.ready()

  const timeout = setTimeout(() => t.fail(), 1000)

  t.teardown(async () => {
    clearTimeout(timeout)
    await a.close()
    await b.close()
  })

  replicate([a, b])

  await addWriter(a, b)
  await a.update()

  await a.append('trigger')

  async function applyThrow (batch, view, base) {
    for (const node of batch) {
      if (node.value.add) {
        await base.addWriter(b4a.from(node.value.add, 'hex'))
      }

      if (node.value === 'trigger') {
        throw new Error('Syntehtic.')
      }
    }
  }
})

test('basic - non-indexed writer', async t => {
  const [a, b] = await create(2, applyWriter, store => store.get('test', { valueEncoding: 'json' }))

  await a.append({ add: b.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b])

  await b.append('b0')
  await b.append('b1')

  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  t.is(a.view.length, 2)
  t.is(b.view.length, 2)

  await a.append('a0')

  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)

  t.is(a.view.length, 3)
  t.is(b.view.length, 3)

  const a0 = await a.view.get(0)
  const b0 = await b.view.get(0)

  const a1 = await a.view.get(1)
  const b1 = await b.view.get(1)

  const a2 = await a.view.get(2)
  const b2 = await b.view.get(2)

  t.is(a0, 'b0')
  t.is(b0, 'b0')

  t.is(a1, 'b1')
  t.is(b1, 'b1')

  t.is(a2, 'a0')
  t.is(b2, 'a0')

  t.ok(await Autobase.isAutobase(a.local))
  t.ok(await Autobase.isAutobase(b.local))

  for await (const block of a.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of b.local.createReadStream()) {
    t.ok(block.checkpoint === null)
  }

  async function applyWriter (batch, view, base) {
    for (const node of batch) {
      if (node.value.add) {
        await base.addWriter(b4a.from(node.value.add, 'hex'), { isIndexer: !!node.value.indexer })
        continue
      }

      if (view) await view.append(node.value)
    }
  }
})

test('basic - non-indexed writers 3-of-5', async t => {
  const [a, b, c, d, e] = await create(5, applyWriter, store => store.get('test', { valueEncoding: 'json' }))

  await a.append({ add: b.local.key.toString('hex'), indexer: true })
  await a.append({ add: c.local.key.toString('hex'), indexer: true })
  await a.append({ add: d.local.key.toString('hex'), indexer: false })
  await a.append({ add: e.local.key.toString('hex'), indexer: false })

  await replicateAndSync([a, b, c, d, e])

  // confirm indexers
  await b.append(null)
  await c.append(null)

  await confirm([a, b, c, d, e])

  t.is(a.linearizer.indexers.length, 3)
  t.is(b.linearizer.indexers.length, 3)
  t.is(c.linearizer.indexers.length, 3)
  t.is(d.linearizer.indexers.length, 3)
  t.is(e.linearizer.indexers.length, 3)

  t.ok(d.writable)
  t.ok(e.writable)

  await e.append('e0')
  await replicateAndSync([d, e])

  await d.append('d0')
  await replicateAndSync([a, d])

  await a.append('a0') // later will only index to here
  await replicateAndSync([a, e])

  await e.append('e1')
  await replicateAndSync([d, e])

  await e.append('d1')
  await replicateAndSync([a, d, e])

  // e and d do not count
  t.is(a.view.indexedLength, 0)
  t.is(d.view.indexedLength, 0)
  t.is(e.view.indexedLength, 0)

  // confirm with only b and c
  await replicateAndSync([a, b, c])
  await b.append('b0')

  await replicateAndSync([b, c])
  await c.append('c0')

  // should only index up to a0
  t.is(c.view.indexedLength, 3)

  await replicateAndSync([a, b, c, d, e])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)
  t.is(d.view.indexedLength, 3)
  t.is(e.view.indexedLength, 3)

  const a0 = await a.view.get(0)
  const a1 = await a.view.get(1)
  const a2 = await a.view.get(2)

  t.is(a0, 'e0')
  t.is(a1, 'd0')
  t.is(a2, 'a0')

  await t.execution(compare(a, b))
  await t.execution(compare(a, c))
  await t.execution(compare(a, d))
  await t.execution(compare(a, e))

  t.ok(await Autobase.isAutobase(a.local))
  t.ok(await Autobase.isAutobase(b.local))
  t.ok(await Autobase.isAutobase(c.local))
  t.ok(await Autobase.isAutobase(d.local))
  t.ok(await Autobase.isAutobase(e.local))

  for await (const block of a.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of b.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of c.local.createReadStream()) {
    t.ok(block.checkpoint.length !== 0)
  }

  for await (const block of d.local.createReadStream()) {
    t.ok(block.checkpoint === null)
  }

  for await (const block of e.local.createReadStream()) {
    t.ok(block.checkpoint === null)
  }

  async function applyWriter (batch, view, base) {
    for (const node of batch) {
      if (node.value.add) {
        await base.addWriter(b4a.from(node.value.add, 'hex'), { isIndexer: !!node.value.indexer })
        continue
      }

      if (view) await view.append(node.value)
    }
  }
})

test('autobase should not detach the original store', async t => {
  const store = new Corestore(ram)
  const bootstrap = b4a.alloc(32)

  const base = new Autobase(store, bootstrap)
  t.ok(store !== base.store) // New session with the original attached to it

  await base.close()
  t.ok(store.closed)
  t.ok(base.store.closed)
})

test('basic - oplog digest', async t => {
  const [base1, base2] = await create(2, apply)

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await replicateAndSync([base1, base2])
  await base2.append(null)

  await replicateAndSync([base1, base2])
  await base1.append(null)
  await replicateAndSync([base1, base2])

  const last = await base1.local.get(1)

  t.is(last.digest.pointer, 0)
  t.is(last.digest.indexers?.length, 2)
})
