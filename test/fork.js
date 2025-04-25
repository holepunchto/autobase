const test = require('brittle')
const b4a = require('b4a')

const {
  create,
  createStores,
  createBase,
  addWriter,
  replicate,
  replicateAndSync,
  confirm
} = require('./helpers')

test('fork - one writer to another', async t => {
  let forked = false

  const { bases } = await create(2, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: async (batch, view, host) => {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await host.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (value.fork) {
          const indexers = value.fork.indexers.map(key => b4a.from(key, 'hex'))

          const system = {
            key: b4a.from(value.fork.system.key, 'hex'),
            length: value.fork.system.length
          }

          t.is(await host.fork(indexers, system), !forked)
          forked = true

          continue
        }

        if (view) await view.append(value)
      }
    }
  })

  const [a, b] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.append(null)

  t.is(a.view.signedLength, 3)
  t.is(b.view.signedLength, 3)

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, a.local.key)

  await b.append({
    fork: {
      indexers: [b4a.toString(b.local.key, 'hex')],
      system: {
        key: b4a.toString(b.system.core.key, 'hex'),
        length: b.indexedLength
      }
    }
  })

  t.is(b.view.length, 3)
  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, b.local.key)

  await t.execution(b.append('post fork'))

  t.is(b.view.length, 4)
  t.alike(b.system.indexers[0].key, b.local.key)

  t.is(await b.view.get(2), 'three')
  t.is(await b.view.get(3), 'post fork')
})

test('fork - with unindexed state', async t => {
  const { bases } = await create(3, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork
  })

  const [a, b, c] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  for (let i = 0; i < 100; i++) await b.append('b' + i)

  t.is(b.view.signedLength, 3)
  t.is(b.view.length, 103)

  // one way replicate
  await replicateAndSync([b, c])
  await replicateAndSync([a, c])

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, a.local.key)

  await fork(b, [b])

  t.is(b.view.length, 103)

  // check encryption has updated
  t.unlike(await a.view.get(50, { raw: true }), await b.view.get(50, { raw: true }))

  await t.execution(b.append('post fork'))

  t.is(b.view.length, 104)
  t.alike(b.system.indexers[0].key, b.local.key)

  t.is(await b.view.get(2), 'three')
  t.is(await b.view.get(3), 'b0')
  t.is(await b.view.get(103), 'post fork')
})

test('fork - migration after fork', async t => {
  const { bases } = await create(3, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork
  })

  const [a, b, c] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.append(null)

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, a.local.key)

  await fork(b, [b])

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, b.local.key)

  await t.execution(b.append('post fork'))

  await addWriter(b, c, true)
  await confirm([a, b, c], { checkHash: false })

  t.is(b.system.indexers.length, 2)
  t.alike(b.system.indexers[0].key, b.local.key)
  t.alike(b.system.indexers[1].key, c.local.key)

  t.is(b.core.manifest.signers.length, 2)
  t.alike(b.core.key, c.core.key)

  t.is(await b.view.get(2), 'three')
  t.is(await b.view.get(3), 'post fork')
})

test('fork - add old indexer back', async t => {
  const { bases } = await create(2, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork
  })

  const [a, b] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.append(null)

  t.is(a.view.signedLength, 3)
  t.is(b.view.signedLength, 3)

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, a.local.key)

  await fork(b, [b])

  t.is(b.view.length, 3)

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, b.local.key)

  await t.execution(b.append('post fork'))

  await addWriter(b, a, true)
  await replicateAndSync([a, b], { checkHash: false })

  t.is(b.view.length, 4)

  t.is(b.system.indexers.length, 2)
  t.alike(b.system.indexers[1].key, a.local.key)

  t.is(await b.view.get(3), 'post fork')
})

test('fork - fork to multiple indexers', async t => {
  const { bases } = await create(3, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork
  })

  const [a, b, c] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await addWriter(a, c, false)
  await confirm(bases)

  await b.append('b pre fork')
  await c.append('c pre fork')

  await replicateAndSync([b, c])

  t.is(b.view.length, 5)
  t.is(c.view.length, 5)

  await fork(b, [b, c])

  t.is(b.system.indexers.length, 2)
  t.alike(b.system.indexers[0].key, b.local.key)
  t.alike(b.system.indexers[1].key, c.local.key)

  await replicateAndSync([b, c], { checkHash: false })

  t.is(b.view.length, 5)

  t.is(c.system.indexers.length, 2)
  t.alike(c.system.indexers[0].key, b.local.key)
  t.alike(c.system.indexers[1].key, c.local.key)

  t.is(b.view.signedLength, 3)

  await confirm([b, c])

  t.is(b.view.signedLength, 5)
})

test('fork - invalid fork should fail', async t => {
  const { bases } = await create(3, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: async (batch, view, host) => {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await host.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (value.fork) {
          const indexers = value.fork.indexers.map(key => b4a.from(key, 'hex'))

          const system = {
            key: b4a.from(value.fork.system.key, 'hex'),
            length: value.fork.system.length
          }

          t.absent(await host.fork(indexers, system))
          continue
        }

        if (view) await view.append(value)
      }
    }
  })

  const [a, b, c] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await addWriter(a, c, false)
  await confirm(bases)

  await b.append('b pre fork')
  await replicateAndSync([b, c])

  // fork should fail as c is not in system
  await fork(b, [b, c])

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, a.local.key)
})

test('fork - competing forks', async t => {
  const { bases } = await create(3, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork
  })

  const [a, b, c] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await addWriter(a, c, false)
  await confirm(bases)

  await b.append('b pre fork')
  await c.append('c pre fork')

  await replicateAndSync([b, c])

  t.is(b.view.length, 5)
  t.is(c.view.length, 5)

  await fork(b, [b])
  await fork(c, [c])

  t.is(b.system.indexers.length, 1)
  t.is(c.system.indexers.length, 1)

  await confirm([b])
  t.is(b.view.signedLength, 5)
  t.is(c.view.signedLength, 3)

  await replicateAndSync([b, c], { checkHash: false })

  t.is(b.system.indexers.length, 1)
  t.unlike(b.system.indexers, c.system.indexers)

  // resolve fork
  await fork(c, [b])

  t.alike(b.system.indexers, c.system.indexers)

  t.is(b.view.signedLength, 5)
  t.is(c.view.signedLength, 5)
})

test('fork - initial fast forward', async t => {
  const { bases } = await create(2, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork
  })

  const [a, b] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.append('b pre fork')

  await fork(b, [b])

  for (let i = 0; i < 100; i++) await b.append('b' + i)

  t.is(b.view.signedLength, 104)

  const [store] = await createStores(1, t, { offset: 2 })

  const c = createBase(store.session(), a.key, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork,
    fastForward: { key: b.core.key }
  })

  await c.ready()

  await replicateAndSync([b, c])

  t.is(c.view.signedLength, 104)
  t.alike(b.system.indexers, c.system.indexers)
})

test('fork - fast forward after fork', async t => {
  const { bases } = await create(3, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork,
    fastForward: true
  })

  const [a, b, c] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await addWriter(a, c, false)

  await b.append(null)
  await c.append(null)

  await confirm(bases)

  await fork(b, [b, c])
  await replicateAndSync([b, c], { checkHash: false })

  t.alike(b.system.indexers, c.system.indexers)

  const unreplicate = replicate([b, c])
  for (let i = 0; i < 500; i++) {
    await b.append('b' + i)
    await c.append('c' + i)
  }

  await unreplicate()

  await confirm([b, c])

  t.is(b.view.signedLength, 1003)
  t.is(c.view.signedLength, 1003)

  const ff = new Promise(resolve => a.once('fast-forward', resolve))

  await replicateAndSync([a, b, c])

  await t.execution(ff)

  t.is(await ff, b.core.signedLength)
  t.alike(b.view.signedLength, a.view.signedLength)
  t.alike(b.system.indexers, a.system.indexers)
})

test('fork - migration after fork', async t => {
  const { bases } = await create(3, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: applyFork
  })

  const [a, b, c] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.append(null)

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, a.local.key)

  await fork(b, [b])

  t.is(b.system.indexers.length, 1)
  t.alike(b.system.indexers[0].key, b.local.key)

  await t.execution(b.append('post fork'))

  await addWriter(b, c, true)
  await confirm([a, b, c], { checkHash: false })

  t.is(b.system.indexers.length, 2)
  t.alike(b.system.indexers[0].key, b.local.key)
  t.alike(b.system.indexers[1].key, c.local.key)

  t.is(b.core.manifest.signers.length, 2)
  t.alike(b.core.key, c.core.key)

  t.is(await b.view.get(2), 'three')
  t.is(await b.view.get(3), 'post fork')

  for (let i = 0; i < 50; i++) {
    await c.append('in between forks')
  }

  t.is(c.view.signedLength, b.view.signedLength)
  t.is(c.view.length, c.view.signedLength + 50)

  const raw = await c.view.get(50, { raw: true })

  await fork(c, [c])

  await c.ack()

  t.unlike(await c.view.get(50, { raw: true }), raw, 'encryption updated')

  await replicateAndSync([b, c])
})

async function applyFork (batch, view, host) {
  for (const { value } of batch) {
    if (value.add) {
      const key = Buffer.from(value.add, 'hex')

      await host.addWriter(key, { indexer: value.indexer })
      continue
    }

    if (value.fork) {
      const indexers = value.fork.indexers.map(key => b4a.from(key, 'hex'))

      const system = {
        key: b4a.from(value.fork.system.key, 'hex'),
        length: value.fork.system.length
      }

      await host.fork(indexers, system)
      continue
    }

    if (view) await view.append(value)
  }
}

async function fork (base, indexers) {
  return base.append({
    fork: {
      indexers: indexers.map(idx => b4a.toString(idx.local.key, 'hex')),
      system: {
        key: b4a.toString(base.system.core.key, 'hex'),
        length: base.indexedLength
      }
    }
  })
}
