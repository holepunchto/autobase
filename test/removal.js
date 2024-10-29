const test = require('brittle')
const b4a = require('b4a')

const {
  create,
  replicateAndSync,
  addWriter,
  addWriterAndSync,
  confirm
} = require('./helpers')

test('remove - basic', async t => {
  const { bases } = await create(3, t, { apply, open: null })
  const [a, b, c] = bases

  await addWriter(a, b, false)

  await confirm([a, b, c])

  await addWriter(b, c, false)

  await confirm([a, b, c])

  t.is(b.system.members, 3)
  t.is(b.system.members, c.system.members)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })

  await confirm([a, b, c])

  t.is(b.system.members, 2)
  t.is(b.system.members, c.system.members)
})

test('remove - remove and rejoin writer', async t => {
  const { bases } = await create(2, t, { apply, open: null })
  const [a, b] = bases

  await addWriter(a, b, false)
  await confirm([a, b])

  t.is(a.system.members, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await confirm([a, b])

  t.is(a.system.members, 1)

  await addWriter(a, b, false)
  await confirm([a, b])

  t.is(a.system.members, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await confirm([a, b])

  t.is(a.system.members, 1)
})

test('remove - non-indexer writer removes themselves', async t => {
  const { bases } = await create(2, t, { apply, open: null })
  const [a, b] = bases

  await addWriter(a, b, false)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await b.append({ remove: b4a.toString(b.local.key, 'hex') })

  await t.exception(b.append('fail'), /Not writable/)

  await confirm([a, b])

  t.is(a.system.members, 1)
  t.is(a.system.members, a.system.members)
})

test('remove - remove indexer', async t => {
  const { bases } = await create(3, t, { apply, open: null })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await b.append(null)
  await replicateAndSync([a, b, c])

  await confirm([a, b, c])

  await addWriterAndSync(b, c)
  await c.append(null)
  await replicateAndSync([a, b, c])

  await confirm([a, b, c])

  t.is(b.system.members, 3)
  t.is(b.system.members, c.system.members)

  t.is(b.system.indexers.length, 3)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })
  await confirm([a, b, c])

  t.is(c.writable, false)

  const info = await a.system.get(c.local.key)

  t.is(info.isIndexer, false)
  t.is(info.isRemoved, true)

  t.is(b.system.members, 2)
  t.is(b.system.indexers.length, 2)
  t.is(b.system.members, c.system.members)
})

test('remove - remove indexer and continue indexing', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(b, c)

  await c.append('c1')

  await confirm([a, b, c])

  t.is(b.view.getBackingCore().session.manifest.signers.length, 3)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })
  await confirm([a, b, c])

  t.is(c.writable, false)
  await t.exception(c.append('fail'), /Not writable/)

  const length = a.view.indexedLength

  await t.execution(b.append('hello'))
  await t.execution(confirm([a, b, c]))

  t.not(a.view.indexedLength, length)

  t.is(b.linearizer.indexers.length, 2)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)
})

test('remove - remove indexer back to previously used indexer set', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)

  await b.append('b1')

  await confirm([a, b, c])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  await addWriterAndSync(b, c)

  await c.append('c1')

  await confirm([a, b, c])

  t.is(b.view.indexedLength, 2)

  const manifest1 = b.system.core.getBackingCore().session.manifest
  t.is(manifest1.signers.length, 3)
  t.is(b.system.indexers.length, 3)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })
  await confirm([a, b, c])

  t.is(c.writable, false)
  await t.exception(c.append('fail'), /Not writable/)

  await t.execution(b.append('hello'))
  await t.execution(confirm([a, b, c]))

  t.is(b.linearizer.indexers.length, 2)
  t.is(b.view.indexedLength, 3)
  t.is(c.view.indexedLength, 3)

  const manifest2 = b.system.core.getBackingCore().session.manifest
  t.is(manifest2.signers.length, 2)

  t.not(manifest1.prologue.length, manifest2.prologue.length)
  t.unlike(manifest1.prologue.hash, manifest2.prologue.hash)
})

test('remove - remove an indexer when 2-of-2', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await addWriterAndSync(a, b)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  const manifest = b.system.core.getBackingCore().session.manifest

  t.is(manifest.signers.length, 2)
  t.is(b.system.indexers.length, 2)
  t.is(b.linearizer.indexers.length, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await confirm([a, b])

  t.is(b.writable, false)
  await t.exception(b.append('fail'), /Not writable/)

  await t.execution(a.append('hello'))
  await t.execution(confirm([a, b]))

  t.is(a.linearizer.indexers.length, 1)
  t.is(a.view.indexedLength, 2)

  t.is(b.linearizer.indexers.length, 1)
  t.is(b.view.indexedLength, 2)

  const finalManifest = b.system.core.getBackingCore().session.manifest

  t.is(finalManifest.signers.length, 1)
  t.not(finalManifest.prologue.length, 0)
})

test('remove - remove multiple indexers concurrently', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(b, c)

  await c.append('c1')

  await confirm([a, b, c])

  t.is(b.view.getBackingCore().session.manifest.signers.length, 3)

  a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await a.append({ remove: b4a.toString(c.local.key, 'hex') })

  await confirm([a, b, c])

  t.is(b.writable, false)
  t.is(c.writable, false)

  await t.exception(b.append('fail'), /Not writable/)
  await t.exception(c.append('fail'), /Not writable/)

  const length = a.view.indexedLength
  await t.execution(a.append('hello'))

  t.not(a.view.indexedLength, length) // 1 indexer

  t.is(b.linearizer.indexers.length, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 1)
})

test('remove - indexer removes themselves', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)
  await addWriterAndSync(b, c)

  await a.append('a1')

  await confirm([a, b, c])

  t.is(b.view.getBackingCore().session.manifest.signers.length, 3)

  await a.append({ remove: b4a.toString(a.local.key, 'hex') })

  await confirm([a, b, c])

  t.is(a.writable, false)
  t.is(a.view.getBackingCore().session.manifest.signers.length, 2)

  await t.exception(a.append('fail'), /Not writable/)

  const length = a.view.length
  const indexedLength = a.view.indexedLength

  await t.execution(b.append('b1'))
  await t.execution(c.append('c1'))

  await replicateAndSync([a, b, c])

  t.not(a.view.length, length) // can still read

  await confirm([b, c, a]) // a has to come last cause otherwise confirm add it to the maj peers

  t.not(a.view.indexedLength, indexedLength) // b,c can still index
})

test('remove - cannot remove last indexer', async t => {
  t.plan(7)

  const { bases } = await create(2, t, { apply: applyWithException })
  const [a, b] = bases

  await a.append('a1')

  await replicateAndSync([a, b])

  t.is(a.view.length, 1)
  t.is(b.view.length, 1)

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  t.is(a.view.getBackingCore().session.manifest.signers.length, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 1)

  await a.append({ remove: b4a.toString(a.local.key, 'hex') })

  async function applyWithException (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        await base.addWriter(b4a.from(value.add, 'hex'))
        continue
      }

      if (value.remove) {
        await t.exception(() => base.removeWriter(b4a.from(value.remove, 'hex')))
        continue
      }

      await view.append(value)
    }
  }
})

test('remove - promote writer to indexer', async t => {
  t.plan(9)

  const { bases } = await create(2, t)

  const [a, b] = bases

  // add writer
  await addWriter(a, b, false)
  await replicateAndSync([a, b])

  t.is(a.linearizer.indexers.length, 1)
  t.is(b.linearizer.indexers.length, 1)

  t.absent(b.isIndexer)
  t.absent(b.localWriter.isActiveIndexer)

  await b.append(null)
  await replicateAndSync([a, b])

  // promote writer
  await addWriter(a, b, true)

  t.is(a.linearizer.indexers.length, 2)

  const event = new Promise(resolve => b.on('is-indexer', resolve))

  await replicateAndSync([a, b])

  await t.execution(event)
  t.is(b.linearizer.indexers.length, 2)
  t.ok(b.isIndexer)
  t.ok(b.localWriter.isActiveIndexer)
})

test('remove - demote indexer to writer', async t => {
  t.plan(13)

  const { bases } = await create(2, t)

  const [a, b] = bases

  // add writer
  await addWriterAndSync(a, b)

  t.is(a.linearizer.indexers.length, 2)
  t.is(b.linearizer.indexers.length, 2)

  t.ok(b.isIndexer)
  t.ok(b.localWriter.isActiveIndexer)

  await b.append('message')
  await confirm([a, b])

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  const event = new Promise(resolve => b.on('is-non-indexer', resolve))

  // demote writer
  await addWriter(a, b, false)

  await confirm([a, b])

  t.is(a.linearizer.indexers.length, 1)
  t.is(b.linearizer.indexers.length, 1)

  await replicateAndSync([a, b])

  await t.execution(event)

  t.is(b.linearizer.indexers.length, 1)
  t.absent(b.isIndexer)
  t.absent(b.localWriter.isActiveIndexer)

  // flush active writer set
  await a.append(null)

  t.is(a.activeWriters.size, 1)
})

test('remove - add new indexer after removing', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })
  await confirm([a, b])

  t.is(b.writable, false)

  t.is(a.linearizer.indexers.length, 1)
  t.is(b.linearizer.indexers.length, 1)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 1)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 1)

  await t.execution(a.append('hello'))
  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 2)
  t.is(b.view.indexedLength, 2)

  await addWriterAndSync(a, c)

  await c.append('c1')

  t.is(b.writable, false)

  await confirm([a, b, c])
  await replicateAndSync([a, b, c])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)
  t.is(c.view.indexedLength, 3)

  t.is(a.linearizer.indexers.length, 2)
  t.is(b.linearizer.indexers.length, 2)
  t.is(c.linearizer.indexers.length, 2)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 2)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 2)
  t.is(c.system.core.getBackingCore().session.manifest.signers.length, 2)
})

test('remove - readd removed indexer', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  let added = false
  b.on('is-indexer', () => { added = true })
  b.on('is-non-indexer', () => { added = false })

  await addWriterAndSync(a, b)

  t.ok(added)
  t.is(b.isIndexer, true)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 2)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })

  await replicateAndSync([a, b])

  t.absent(added)
  t.is(b.isIndexer, false)

  await confirm([a, b])

  t.is(b.writable, false)
  await t.exception(b.append('fail'), /Not writable/)

  t.is(a.linearizer.indexers.length, 1)
  t.is(b.linearizer.indexers.length, 1)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 1)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 1)

  await t.execution(a.append('hello'))
  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 2)
  t.is(b.view.indexedLength, 2)

  await addWriterAndSync(a, b)

  t.ok(added)
  t.is(b.writable, true)
  t.is(b.isIndexer, true)

  await b.append('b1')

  await confirm([a, b])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)

  t.is(a.linearizer.indexers.length, 2)
  t.is(b.linearizer.indexers.length, 2)

  t.is(a.system.core.getBackingCore().session.manifest.signers.length, 2)
  t.is(b.system.core.getBackingCore().session.manifest.signers.length, 2)
})

// todo: this test is hard, probably have to rely on ff to fix
test('remove - writer adds a writer while being removed', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await addWriterAndSync(a, b, false)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 1)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })

  t.is(a.view.indexedLength, 1)
  t.is(a.view.length, 1)
  t.is(a.system.members, 1)

  t.is(b.writable, true)

  await b.append('b2')
  await b.append('b3')
  await b.append('b4')

  t.is(b.view.indexedLength, 1)
  t.is(b.view.length, 4)
  t.is(b.system.members, 2)

  await replicateAndSync([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.length, 1)
  t.is(b.system.members, 1)

  await a.append(null)
  await replicateAndSync([a, b])

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  const ainfo = await a.system.get(b.local.key)
  const binfo = await b.system.get(b.local.key)

  t.is(ainfo.length, b.local.length)
  t.is(ainfo.length, b.local.length)

  t.is(ainfo.isRemoved, true)
  t.is(binfo.isRemoved, true)
})

// todo: this test is hard, probably have to rely on ff to recover
test.solo('remove - writer adds a writer while being removed', async t => {
  const { bases } = await create(4, t, { apply })
  const [a, b, c, d] = bases

  await addWriterAndSync(a, b, false)

  await b.append('b1')

  await confirm([a, b])

  t.is(b.view.indexedLength, 1)
  t.is(b.view.getBackingCore().session.manifest.signers.length, 1)

  await addWriterAndSync(a, d, false)
  await a.append({ remove: b4a.toString(b.local.key, 'hex') })

  // b adds c while it's being removed
  t.is(b.writable, true)

  // a will never see this writer
  await addWriterAndSync(b, c, false)

  await c.append('c1')

  await replicateAndSync([b, c, d])

  t.is(c.writable, true)
  t.is(c.system.members, 4)

  await replicateAndSync([b, c, d])

  // d links back to c
  await d.append('d1')

  t.is(d.view.indexedLength, 1)
  t.is(d.view.length, 3)
  t.is(d.system.members, 4)

  a.debug = true

  // now sync so d sees b's removal
  await replicateAndSync([a, d])

  t.is(a.system.members, 2)
  t.is(d.system.members, 2)
  t.is(d.view.indexedLength, 1)
  t.is(d.view.length, 2)

  await a.append(null)

  await replicateAndSync([a, d])

  t.is(d.view.indexedLength, 2)

  t.is(await d.view.get(0), 'b1')
  t.is(await d.view.get(1), 'd1')
})

test('remove - writer state is consistent during removal', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriterAndSync(a, b, false)
  await addWriterAndSync(a, c, false)

  await b.append('b1')
  await c.append('c1')

  await confirm([a, b, c])

  t.is(b.view.indexedLength, 2)
  t.is(c.view.indexedLength, 2)

  await a.append({ remove: b4a.toString(c.local.key, 'hex') })

  await replicateAndSync([a, b, c])

  t.is(a.view.indexedLength, 2)
  t.is(a.view.length, 2)
  t.is(a.system.members, 2)

  t.is(b.writable, true)
  t.is(c.writable, false)

  await addWriterAndSync(b, c, false)

  t.is(c.writable, true)

  // load c into b.activeWriters
  await c.append(null)

  await replicateAndSync([b, c])

  t.is(b.activeWriters.size, 3)

  for (let i = 0; i < 10; i++) a.append('a' + i)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })

  await replicateAndSync([a, b, c])

  t.is(b.writable, false)
  t.is(c.writable, false)

  await t.exception(c.append('not writable'))

  await t.execution(replicateAndSync([a, b, c]))

  t.is(a.view.length, b.view.length)
  t.is(a.view.length, c.view.length)
})

async function apply (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(b4a.from(value.add, 'hex'), { indexer: value.indexer !== false })
      continue
    }

    if (value.remove) {
      await base.removeWriter(b4a.from(value.remove, 'hex'))
      continue
    }

    await view.append(value)
  }
}
