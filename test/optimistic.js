const test = require('brittle')
const b4a = require('b4a')

const {
  create,
  replicateAndSync
} = require('./helpers')

test('optimistic - two writer', async t => {
  const { bases } = await create(2, t, {
    optimistic: true,
    async apply (nodes, view, base) {
      for (const node of nodes) {
        if (node.value === 'optimistic') await base.ackWriter(node.from.key)
        await view.append(node.value)
      }
    }
  })
  const [a, b] = bases

  await a.append('hello')
  await replicateAndSync([a, b])

  await b.append('world', { optimistic: true }) // should be ignored
  await b.append('optimistic', { optimistic: true })

  await replicateAndSync([a, b])

  t.is(a.view.length, 2)
  t.is(await a.view.get(0), 'hello')
  t.is(await a.view.get(1), 'optimistic')

  t.is(b.view.length, 2)
  t.is(await b.view.get(0), 'hello')
  t.is(await b.view.get(1), 'optimistic')
})

test('optimistic - reorgs', async t => {
  const { bases } = await create(4, t, {
    optimistic: true,
    async apply (nodes, view, base) {
      for (const node of nodes) {
        if (node.length === 1 && !node.from.key.equals(base.key)) {
          await base.addWriter(node.from.key, { isIndexer: false })
        }
        await view.append(node.value)
      }
    }
  })

  const [a, b, c, d] = bases

  await a.append('root')

  await replicateAndSync(bases)

  await c.append('hello', { optimistic: true })
  await d.append('world', { optimistic: true })

  await replicateAndSync([a, c])
  await replicateAndSync([b, d])

  {
    const all = []
    for await (const data of a.view.createReadStream()) all.push(data)
    t.alike(all, ['root', 'hello'])
  }

  {
    const all = []
    for await (const data of b.view.createReadStream()) all.push(data)
    t.alike(all, ['root', 'world'])
  }

  await replicateAndSync(bases)

  {
    const allA = []
    const allB = []
    for await (const data of a.view.createReadStream()) allA.push(data)
    for await (const data of b.view.createReadStream()) allB.push(data)

    t.alike(allB, allA)
  }}
)

test('optimistic - no empty heads', async t => {
  const { bases } = await create(2, t, {
    optimistic: true,
    async apply (nodes, view, base) {
      for (const node of nodes) {
        if (node.value === 'optimistic' && view.length === 0) {
          await base.ackWriter(node.from.key)
        }

        await view.append(node.value)
      }
    }
  })

  bases[1].on('error', err => {
    t.pass()
    t.is(err.message, 'Invalid node: empty heads only allowed for genesis')
  })

  await bases[1].append('optimistic', { optimistic: true })
})

test('optimistic - write on top of rejected node', async t => {
  const { bases } = await create(3, t, {
    optimistic: true,
    async apply (nodes, view, base) {
      for (const node of nodes) {
        if (node.value.add) {
          await base.addWriter(b4a.from(node.value.add, 'hex'), { isIndexer: false })
          continue
        }

        if (node.value === 'optimistic' && view.length === 0) {
          await base.ackWriter(node.from.key)
        }

        await view.append(node.value)
      }
    }
  })

  const [a, b, c] = bases

  await a.append({ add: b4a.toString(b.local.key, 'hex') })
  await replicateAndSync([a, b, c])

  await a.append('reorg') // triggers reorg for other writers
  await c.append('optimistic', { optimistic: true })

  await replicateAndSync([b, c])
  await b.append('data')

  t.is(b.view.length, 2)
  t.is(await b.view.get(0), 'optimistic')
  t.is(await b.view.get(1), 'data')

  await replicateAndSync([a, b, c])

  t.is(a.view.length, 2)
  t.is(await a.view.get(0), 'reorg')
  t.is(await a.view.get(1), 'data')

  t.is(b.view.length, 2)
  t.is(await b.view.get(0), 'reorg')
  t.is(await b.view.get(1), 'data')
})
