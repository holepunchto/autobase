const test = require('brittle')
const ram = require('random-access-memory')
const tmpDir = require('test-tmp')
const Corestore = require('corestore')
const b4a = require('b4a')

const Autobase = require('..')

const {
  create,
  sync,
  apply,
  addWriter,
  confirm
} = require('./helpers')

test('suspend - pass exisiting store', async t => {
  const [base1] = await create(1, apply)

  const store = new Corestore(ram.reusable(), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session2 = store.session()
  const base2 = new Autobase(session2, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
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

  const session3 = store.session()
  const base3 = new Autobase(session3, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
  await base3.ready()

  t.is(base3.system.digest.writers.length, 2)

  await base3.append('final')

  await t.execution(sync([base3, base1]))
})

test('suspend - pass exisiting fs store', async t => {
  const [base1] = await create(1, apply)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session2 = store.session()
  const base2 = new Autobase(session2, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
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

  const session3 = store.session()
  const base3 = new Autobase(session3, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
  await base3.ready()

  t.is(base3.system.digest.writers.length, 2)

  await base3.append('final')

  await t.execution(sync([base3, base1]))
})

test('suspend - 2 exisiting fs stores', async t => {
  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(0)
  })

  const base1 = new Autobase(store, null, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
  await base1.ready()

  const store2 = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session2 = store2.session()
  const base2 = new Autobase(session2, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
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

  const session3 = store2.session()
  const base3 = new Autobase(session3, base1.local.key, { apply, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0 })
  await base3.ready()

  t.is(base3.system.digest.writers.length, 2)

  await base3.append('final')

  await t.execution(sync([base3, base1]))

  await base1.close()
  await base2.close()
  await base3.close()
})

test('suspend - reopen after index', async t => {
  const [a] = await create(1, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0
  })

  await b.ready()

  await addWriter(a, b)

  await sync([a, b])

  await a.append('a0')

  await confirm([a, b])

  await b.append('b0')
  await a.append('a1')

  t.is(b.system.digest.writers.length, 2)

  const order = []
  for (let i = 0; i < b.view.length; i++) {
    order.push(await b.view.get(i))
  }

  await b.close()

  const session2 = store.session()
  const c = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open: store => store.get('view', {
      valueEncoding: 'json'
    }),
    ackInterval: 0,
    ackThreshold: 0
  })

  await c.ready()
  await c.update({ wait: false })

  t.is(c.view.length, order.length)

  for (let i = 0; i < c.view.length; i++) {
    t.alike(await c.view.get(i), order[i])
  }

  t.is(c.system.digest.writers.length, 2)

  await c.append('final')

  await t.execution(sync([a, c]))

  t.is(b.view.indexedLength, 1)
  t.is(c.view.indexedLength, 1)
  t.is(c.view.length, b.view.length + 2)

  await a.close()
  await b.close()
  await c.close()
})

test('suspend - reopen with sync in middle', async t => {
  const [a] = await create(1, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  await b.ready()

  await addWriter(a, b)

  await sync([a, b])

  await a.append('a0')

  await confirm([a, b])

  await b.append('b0')
  await a.append('a1')

  t.is(b.system.digest.writers.length, 2)
  t.is(b.view.length, 2)

  await b.close()

  const s1 = a.store.replicate(true)
  const s2 = store.replicate(false)

  s1.pipe(s2).pipe(s1)

  await a.update({ wait: true })

  for (const [key, core] of store.cores) {
    const end = a.store.cores.get(key).core.tree.length
    const range = core.download({ start: 0, end })
    await range.done()
  }

  s1.destroy()
  s2.destroy()

  await Promise.all([
    new Promise(resolve => s1.on('close', resolve)),
    new Promise(resolve => s2.on('close', resolve))
  ])

  const session2 = store.session()
  await session2.ready()

  const c = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  await c.ready()
  await c.update({ wait: false })

  t.is(c.system.digest.writers.length, 2)
  t.is(c.view.length, b.view.length + 1)

  await c.append('final')

  await t.execution(sync([c, b]))

  t.is(b.view.indexedLength, 1)
  t.is(c.view.indexedLength, 1)
  t.is(c.view.length, b.view.length + 2)

  await a.close()
  await b.close()
  await c.close()
})

test('suspend - reopen with indexing in middle', async t => {
  const [a, b] = await create(2, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(2)
  })

  const session1 = store.session()
  const c = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  await c.ready()

  await addWriter(a, b)
  await addWriter(a, c)

  await confirm([a, b, c])

  t.is(c.system.digest.writers.length, 3)
  t.is(c.view.length, 0)

  await c.append('c0')

  await c.close()

  // majority continues

  await a.append('a0')
  await sync([a, b])
  await b.append('b0')
  await sync([a, b])
  await a.append('a1')

  await confirm([a, b])

  const session2 = store.session()
  await session2.ready()

  const c2 = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  await c2.ready()
  await c2.update({ wait: false })

  t.is(c2.system.digest.writers.length, 3)
  t.is(c2.view.length, 1)
  t.is(c2.view.indexedLength, 0)

  t.alike(await c2.view.get(0), await c.view.get(0))

  await c2.append('final')

  await t.execution(sync([c2, b]))

  t.is(b.view.indexedLength, 3)
  t.is(c2.view.indexedLength, 3)
  t.is(c2.view.length, 5)

  await a.close()
  await b.close()
  await c2.close()
})

test('suspend - reopen with indexing + sync in middle', async t => {
  const [a, b] = await create(2, apply, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(2)
  })

  const session1 = store.session()
  const c = new Autobase(session1, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  await c.ready()

  await addWriter(a, b)
  await addWriter(a, c)

  await confirm([a, b, c])

  t.is(c.system.digest.writers.length, 3)
  t.is(c.view.length, 0)

  await c.append('c0')

  await c.close()

  // majority continues

  await a.append('a0')
  await sync([a, b])
  await b.append('b0')
  await sync([a, b])
  await a.append('a1')

  await confirm([a, b])

  const s1 = a.store.replicate(true)
  const s2 = store.replicate(false)

  s1.pipe(s2).pipe(s1)

  await a.update({ wait: true })

  for (const [key, core] of store.cores) {
    const end = a.store.cores.get(key).core.tree.length
    const range = core.download({ start: 0, end })
    await range.done()
  }

  s1.destroy()
  s2.destroy()

  await Promise.all([
    new Promise(resolve => s1.on('close', resolve)),
    new Promise(resolve => s2.on('close', resolve))
  ])

  const session2 = store.session()
  await session2.ready()

  const c2 = new Autobase(session2, a.local.key, {
    apply,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  await c2.ready()
  await c2.update({ wait: false })

  t.is(c2.system.digest.writers.length, 3)
  t.is(c2.view.length, 4)
  t.is(c2.view.indexedLength, 3)

  for (let i = 0; i < b.view.length; i++) {
    t.alike(await c2.view.get(i), await b.view.get(i))
  }

  await c2.append('final')

  await t.execution(sync([c2, b]))

  t.is(b.view.indexedLength, 3)
  t.is(c2.view.indexedLength, 3)
  t.is(c2.view.length, 5)

  await a.close()
  await b.close()
  await c2.close()
})

test('suspend - non-indexed writer', async t => {
  const [a] = await create(1, applyWriter, open)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: b4a.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    apply: applyWriter,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  await b.ready()
  await b.view.ready()

  await a.append({ add: b.local.key.toString('hex'), indexer: false })

  await sync([a, b])

  await b.append('b0')
  await b.append('b1')

  await sync([a, b])

  await a.append('a0')

  await confirm([a, b])

  const s1 = a.store.replicate(true)
  const s2 = store.replicate(false)

  s1.pipe(s2).pipe(s1)

  await a.update({ wait: true })
  await b.update({ wait: true })

  s1.destroy()
  s2.destroy()

  await Promise.all([
    new Promise(resolve => s1.on('close', resolve)),
    new Promise(resolve => s2.on('close', resolve))
  ])

  await b.close()

  const session2 = store.session()
  await session2.ready()

  const c = new Autobase(session2, a.local.key, {
    applyWriter,
    valueEncoding: 'json',
    open,
    ackInterval: 0,
    ackThreshold: 0
  })

  c.debug = true

  await c.ready()
  await c.update({ wait: false })

  t.is(c.view.indexedLength, a.view.indexedLength)
  t.is(c.view.length, a.view.length)

  await a.close()
  await c.close()

  async function applyWriter (batch, view, base) {
    for (const node of batch) {
      if (node.value.add) {
        base.addWriter(b4a.from(node.value.add, 'hex'), { indexer: !!node.value.indexer })
        continue
      }

      if (view) await view.append(node.value)
    }
  }
})

test('suspend - open new index after reopen', async t => {
  const [a] = await create(1, applyMultiple, openMultiple)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0
  })

  await b.ready()

  await addWriter(a, b)

  await sync([a, b])

  await a.append({ index: 1, data: 'a0' })

  await confirm([a, b])

  await b.append({ index: 2, data: 'b0' })
  await a.append({ index: 1, data: 'a1' })

  t.is(b.system.digest.writers.length, 2)

  const order = []
  for (let i = 0; i < b.view.first.length; i++) {
    order.push(await b.view.first.get(i))
  }

  for (let i = 0; i < b.view.second.length; i++) {
    order.push(await b.view.second.get(i))
  }

  await b.close()

  const session2 = store.session()
  const c = new Autobase(session2, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0
  })

  await c.ready()
  await c.update({ wait: false })

  t.is(c.view.first.length + c.view.second.length, order.length)

  for (let i = 0; i < c.view.first.length; i++) {
    t.alike(await c.view.first.get(i), order[i])
  }

  for (let i = 0; i < c.view.second.length; i++) {
    t.alike(await c.view.second.get(i), order[i + c.view.first.length])
  }

  t.is(c.system.digest.writers.length, 2)

  await c.append({ view: 1, data: 'final' })

  await t.execution(sync([a, c]))

  t.is(b.view.first.indexedLength, 1)
  t.is(c.view.first.indexedLength, 1)
  t.is(c.view.first.length, b.view.first.length + 1)

  await t.execution(confirm([a, c]))

  const an = await a.local.get(a.local.length - 1)
  const cn = await c.local.get(c.local.length - 1)

  t.is(an.checkpoint.length, 3)
  t.is(cn.checkpoint.length, 3)

  const acp1 = await a.localWriter.getCheckpoint(1)
  const acp2 = await a.localWriter.getCheckpoint(2)

  const ccp1 = await c.localWriter.getCheckpoint(1)
  const ccp2 = await c.localWriter.getCheckpoint(2)

  t.alike(acp1.treeHash, ccp1.treeHash)
  t.alike(acp1.length, ccp1.length)

  t.alike(acp2.treeHash, ccp2.treeHash)
  t.alike(acp2.length, ccp2.length)

  t.alike(acp1, a.view.first._source._checkpoint())
  t.alike(acp2, a.view.second._source._checkpoint())

  await a.close()
  await b.close()
  await c.close()
})

test('suspend - reopen multiple indexes', async t => {
  const [a] = await create(1, applyMultiple, openMultiple)

  const store = new Corestore(await tmpDir(t), {
    primaryKey: Buffer.alloc(32).fill(1)
  })

  const session1 = store.session()
  const b = new Autobase(session1, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0
  })

  await b.ready()

  await addWriter(a, b)

  await sync([a, b])

  await a.append({ index: 1, data: 'a0' })
  await a.append({ index: 2, data: 'a1' })

  await confirm([a, b])

  await b.append({ index: 2, data: 'b0' })
  await b.append({ index: 1, data: 'b1' })
  // await b.append({ index: 1, data: 'b2' })
  await a.append({ index: 1, data: 'a2' })

  t.is(b.system.digest.writers.length, 2)

  const order = []
  for (let i = 0; i < b.view.first.length; i++) {
    order.push(await b.view.first.get(i))
  }

  for (let i = 0; i < b.view.second.length; i++) {
    order.push(await b.view.second.get(i))
  }

  await b.close()

  const session2 = store.session()
  const c = new Autobase(session2, a.local.key, {
    valueEncoding: 'json',
    apply: applyMultiple,
    open: openMultiple,
    ackInterval: 0,
    ackThreshold: 0
  })

  await c.ready()
  await c.update({ wait: false })

  t.is(c.view.first.length + c.view.second.length, order.length)

  for (let i = 0; i < c.view.first.length; i++) {
    t.alike(await c.view.first.get(i), order[i])
  }

  for (let i = 0; i < c.view.second.length; i++) {
    t.alike(await c.view.second.get(i), order[i + c.view.first.length])
  }

  t.is(c.system.digest.writers.length, 2)

  await c.append({ view: 1, data: 'final' })

  await t.execution(sync([a, c]))

  t.is(b.view.first.indexedLength, 1)
  t.is(c.view.first.indexedLength, 1)
  t.is(c.view.first.length, b.view.first.length + 1)

  await t.execution(confirm([a, c]))

  const an = await a.local.get(a.local.length - 1)
  const cn = await c.local.get(c.local.length - 1)

  t.is(an.checkpoint.length, 3)
  t.is(cn.checkpoint.length, 3)

  const acp1 = await a.localWriter.getCheckpoint(1)
  const acp2 = await a.localWriter.getCheckpoint(2)

  const ccp1 = await c.localWriter.getCheckpoint(1)
  const ccp2 = await c.localWriter.getCheckpoint(2)

  t.alike(acp1.treeHash, ccp1.treeHash)
  t.alike(acp1.length, ccp1.length)

  t.alike(acp2.treeHash, ccp2.treeHash)
  t.alike(acp2.length, ccp2.length)

  t.alike(acp1, a.view.first._source._checkpoint())
  t.alike(acp2, a.view.second._source._checkpoint())

  await a.close()
  await b.close()
  await c.close()
})

test('restart non writer', async t => {
  const storeA = new Corestore(ram.reusable())
  const storeB = new Corestore(ram.reusable())

  const base = new Autobase(storeA, { apply, valueEncoding: 'json' })
  await base.append({ hello: 'world' })

  const other = new Autobase(storeB.session(), base.key, { apply, valueEncoding: 'json' })

  await other.ready()

  await sync([base, other])

  await other.close()
  await base.close()

  const other2 = new Autobase(storeB.session(), base.key, { apply, valueEncoding: 'json' })
  await t.execution(other2.ready(), 'should be able to start')
  await other2.close()
})

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

function openMultiple (store) {
  return {
    first: store.get('first', { valueEncoding: 'json' }),
    second: store.get('second', { valueEncoding: 'json' })
  }
}

async function applyMultiple (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      base.addWriter(Buffer.from(value.add, 'hex'))
      continue
    }

    if (value.index === 1) {
      await view.first.append(value.data)
    } else if (value.index === 2) {
      await view.second.append(value.data)
    }
  }
}
