const test = require('brittle')

const { create, replicateAndSync } = require('./helpers')

test('optimistic - two writer', async (t) => {
  const { bases } = await create(2, t, {
    optimistic: true,
    async apply(nodes, view, base) {
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

test('optimistic - truncate to 0', async (t) => {
  const { bases } = await create(2, t, {
    optimistic: true,
    async apply(nodes, view, base) {
      for (const node of nodes) {
        if (node.length === 1 && !node.from.key.equals(base.key)) {
          await base.addWriter(node.from.key, { isIndexer: false })
        }
        await view.append(node.value)
      }
    }
  })

  await bases[0].append('world', { optimistic: true })
  await bases[1].append('hello', { optimistic: true })
  await replicateAndSync(bases)

  {
    const all = []
    for await (const data of bases[0].view.createReadStream()) all.push(data)
    t.alike(all, ['world', 'hello'])
  }

  {
    const all = []
    for await (const data of bases[1].view.createReadStream()) all.push(data)
    t.alike(all, ['world', 'hello'])
  }
})
