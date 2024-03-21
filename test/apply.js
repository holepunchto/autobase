const test = require('brittle')
const b4a = require('b4a')

const Autobase = require('..')

const {
  create,
  createStores,
  replicate,
  replicateAndSync,
  addWriter,
  encryptionKey,
  confirm
} = require('./helpers')

test('apply - simple', async t => {
  const { bases } = await create(1, t, { apply })
  const [a] = bases

  await a.append('a1')

  t.is(a.system.members, 1)
  t.is(a.view.length, 1)
  t.is(a.view.indexedLength, 1)

  await a.append('a2')

  t.is(a.view.length, 2)
  t.is(a.view.indexedLength, 2)

  async function apply (nodes, view, base) {
    for (const node of nodes) {
      await view.append(node.value)
    }
  }
})

test('apply - add writer', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await addWriter(a, b)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await a.append('a1')
  await b.append('b1')

  t.is(a.view.length, 1)
  t.is(b.view.length, 1)

  await replicateAndSync([a, b])

  t.is(a.view.length, 2)
  t.is(b.view.length, 2)

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  await confirm([a, b])

  t.is(a.view.indexedLength, 2)
  t.is(b.view.indexedLength, 2)

  async function apply (nodes, view, base) {
    for (const node of nodes) {
      if (node.value.add) {
        const { add, indexer } = node.value
        await base.addWriter(b4a.from(add, 'hex'), { indexer })
        continue
      }

      await view.append(node.value)
    }
  }
})

test('apply - multiple appends', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await addWriter(a, b)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await a.append('a1')
  await b.append('b1')

  t.is(a.view.length, 3)
  t.is(b.view.length, 3)

  await replicateAndSync([a, b])

  t.is(a.view.length, 6)
  t.is(b.view.length, 6)

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  await confirm([a, b])

  t.is(a.view.indexedLength, 6)
  t.is(b.view.indexedLength, 6)

  async function apply (nodes, view, base) {
    for (const node of nodes) {
      if (node.value.add) {
        const { add, indexer } = node.value
        await base.addWriter(b4a.from(add, 'hex'), { indexer })
        continue
      }

      for (let i = 0; i < 3; i++) {
        await view.append(node.value + ': ' + i)
      }
    }
  }
})

test('apply - simultaneous appends', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await addWriter(a, b)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await a.append('a1')
  await b.append('b1')

  t.is(a.view.length, 10)
  t.is(b.view.length, 10)

  await replicateAndSync([a, b])

  t.is(a.view.length, 20)
  t.is(b.view.length, 20)

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  await confirm([a, b])

  t.is(a.view.indexedLength, 20)
  t.is(b.view.indexedLength, 20)

  async function apply (nodes, view, base) {
    for (const node of nodes) {
      if (node.value.add) {
        const { add, indexer } = node.value
        await base.addWriter(b4a.from(add, 'hex'), { indexer })
        continue
      }

      const appends = []
      for (let i = 0; i < 10; i++) {
        appends.push(view.append(node.value + ': ' + i))
      }
      await Promise.all(appends)
    }
  }
})

test('apply - add writer and append', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await addWriter(a, b)

  t.is(a.view.length, 1)

  await confirm([a, b])

  t.is(b.view.length, 1)

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await a.append('a1')
  await b.append('b1')

  t.is(a.view.length, 2)
  t.is(b.view.length, 2)

  await replicateAndSync([a, b])

  t.is(a.view.length, 3)
  t.is(b.view.length, 3)

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  await confirm([a, b])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)

  async function apply (nodes, view, base) {
    for (const node of nodes) {
      if (node.value.add) {
        const { add, indexer } = node.value
        await base.addWriter(b4a.from(add, 'hex'), { indexer })
      }

      await view.append(node.value)
    }
  }
})

test('apply - simultaneous add writer and append', async t => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  await addWriter(a, b)

  t.is(a.view.length, 1)

  await confirm([a, b])

  t.is(b.view.length, 1)

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await a.append('a1')
  await b.append('b1')

  t.is(a.view.length, 2)
  t.is(b.view.length, 2)

  await replicateAndSync([a, b])

  t.is(a.view.length, 3)
  t.is(b.view.length, 3)

  t.is(a.view.indexedLength, 1)
  t.is(b.view.indexedLength, 1)

  await confirm([a, b])

  t.is(a.view.indexedLength, 3)
  t.is(b.view.indexedLength, 3)

  async function apply (nodes, view, base) {
    for (const node of nodes) {
      const appends = []

      if (node.value.add) {
        const { add, indexer } = node.value
        appends.push(base.addWriter(b4a.from(add, 'hex'), { indexer }))
      }

      appends.push(view.append(node.value))
      await Promise.all(appends)
    }
  }
})

test('apply - simultaneous append over entire batch', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriter(a, b)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await a.append('a1')
  await b.append('b1')

  t.is(a.view.length, 10)
  t.is(b.view.length, 10)

  await replicateAndSync([a, b])

  t.is(a.view.length, 20)
  t.is(b.view.length, 20)

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  await confirm([a, b])

  t.is(a.view.indexedLength, 20)
  t.is(b.view.indexedLength, 20)

  await replicateAndSync([a, b, c])

  t.is(c.view.indexedLength, 20)
  t.alike(
    await c.view.getBackingCore().treeHash(),
    await a.view.getBackingCore().treeHash()
  )

  async function apply (nodes, view, base) {
    const appends = []
    for (const node of nodes) {
      if (node.value.add) {
        const { add, indexer } = node.value
        await base.addWriter(b4a.from(add, 'hex'), { indexer })
        continue
      }

      for (let i = 0; i < 10; i++) {
        appends.push(view.append(node.value + ': ' + i))
      }
    }
    await Promise.all(appends)
  }
})

test('apply - simultaneous append and add over entire batch', async t => {
  const { bases } = await create(3, t, { apply })
  const [a, b, c] = bases

  await addWriter(a, b)

  await confirm([a, b])

  t.is(a.system.members, 2)
  t.is(a.system.members, b.system.members)

  await a.append('a1')
  await b.append('b1')

  t.is(a.view.length, 10)
  t.is(b.view.length, 10)

  await replicateAndSync([a, b])

  t.is(a.view.length, 20)
  t.is(b.view.length, 20)

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  await confirm([a, b])

  t.is(a.view.indexedLength, 20)
  t.is(b.view.indexedLength, 20)

  await replicateAndSync([a, b, c])

  t.is(c.view.indexedLength, 20)
  t.alike(
    await c.view.getBackingCore().treeHash(),
    await a.view.getBackingCore().treeHash()
  )

  async function apply (nodes, view, base) {
    const appends = []
    for (const node of nodes) {
      if (node.value.add) {
        const { add, indexer } = node.value
        appends.push(base.addWriter(b4a.from(add, 'hex'), { indexer }))
        continue
      }

      for (let i = 0; i < 10; i++) {
        appends.push(view.append(node.value + ': ' + i))
      }
    }
    await Promise.all(appends)
  }
})

// todo: this test can trigger an edge case when adding many writers concurrently
test.skip('apply - simultaneous appends with large batch', async t => {
  const { bases } = await create(10, t, { apply })
  const [a, b] = bases
  const last = bases[bases.length - 1]

  await addWriter(a, b)

  await confirm([a, b])

  const adds = []
  for (let i = 2; i < 9; i++) adds.push(addWriter(a, bases[i]))
  await Promise.all(adds)

  t.is(a.system.members, 9)
  t.is(b.system.members, 2)

  await replicateAndSync([a, b])

  t.is(b.system.members, 9)

  await confirm(bases.slice(0, 9))

  const appends = []
  for (let i = 0; i < 9; i++) {
    const base = bases[i]
    for (let j = 0; j < 10; j++) {
      appends.push(base.append(i + '/' + j))
    }
  }

  await Promise.all(appends)

  t.is(a.view.length, 100)
  t.is(b.view.length, 100)

  await replicateAndSync(bases.slice(0, 9))

  t.is(a.view.length, 900)
  t.is(b.view.length, 900)

  t.is(a.view.indexedLength, 0)
  t.is(b.view.indexedLength, 0)

  await confirm(bases.slice(0, 9))

  t.is(a.view.indexedLength, 900)
  t.is(b.view.indexedLength, 900)

  await replicateAndSync(bases)

  t.is(last.view.indexedLength, 900)
  t.alike(
    await last.view.getBackingCore().treeHash(),
    await a.view.getBackingCore().treeHash()
  )

  async function apply (nodes, view, base) {
    const appends = []
    for (const node of nodes) {
      if (node.value.add) {
        const { add, indexer } = node.value
        appends.push(base.addWriter(b4a.from(add, 'hex'), { indexer }))
        continue
      }

      for (let i = 0; i < 10; i++) {
        appends.push(view.append(node.value + ': ' + i))
      }
    }
    await Promise.all(appends)
  }
})

test('apply - catch exception', async t => {
  t.plan(1)

  const { bases } = await create(1, t)
  const [a] = bases

  const [store] = await createStores(1, t, { offset: 1 })

  const b = new Autobase(store, a.local.key, {
    apply: applyThrow,
    valueEncoding: 'json',
    ackInterval: 0,
    encryptionKey,
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

test('apply - uncaught exception', async t => {
  t.plan(2)

  const [store] = await createStores(1, t)

  const error = new Promise((resolve, reject) => {
    process.on('uncaughtException', reject)
  })

  const a = new Autobase(store.session(), null, {
    async apply (nodes, view, base) {
      throw new Error('Synthetic')
    },
    encryptionKey,
    valueEncoding: 'json'
  })

  a.append('trigger')

  // should throw uncaught exception
  await t.exception(error, /Synthetic/)

  const a2 = new Autobase(store.session(), a.bootstrap, {
    apply: () => {},
    valueEncoding: 'json'
  })

  // can reopen
  await t.execution(a2.ready())
})
