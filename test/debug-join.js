const { on } = require('events')
const test = require('brittle')
const tmpDir = require('test-tmp')
const b4a = require('b4a')

const {
  addWriter,
  addWriterAndSync,
  replicateAndSync,
  sync,
  replicate,
  eventFlush,
  confirm,
  create,
  createStores,
  createBase
} = require('./helpers')

test.solo('fast-forward - multiple writers with view migration', async (t) => {
  t.plan(1)

  const MESSAGES_PER_ROUND = 40

  const { bases } = await create(4, t, {
    fastForward: true,
    storage: () => tmpDir(t)
  })

  const [a, b, c, d] = bases

  await a.append('a')
  await replicateAndSync(bases)

  await addMultipleAndSync(
    a,
    [
      { add: b.local.key.toString('hex'), indexer: true },
      { add: c.local.key.toString('hex'), indexer: true }
    ],
    [a, b, c]
  )

  await b.append('b')
  await c.append('c')

  await confirm([a, b, c])

  const online = [a, b, c]

  for (let i = 0; i < 10; i++) {
    const unreplicate = replicate(online)
    await eventFlush()

    const as = Math.random() * MESSAGES_PER_ROUND
    const bs = Math.random() * MESSAGES_PER_ROUND
    const cs = Math.random() * MESSAGES_PER_ROUND

    for (let j = 0; j < Math.max(as, bs, cs); j++) {
      if (j < as) a.append('a' + j)
      if (j < bs) b.append('b' + j)
      if (j < cs) c.append('c' + j)

      if (j % 2 === 0) await eventFlush()
    }

    await unreplicate()
    await confirm(online)

    if (i === 8) online.push(d)
  }

  const core = d.view

  t.is(d.linearizer.indexers.length, 3)

  async function addMultipleAndSync(base, add, bases) {
    await base.append({ add })
    await replicateAndSync(bases)
    await base.ack()
    await replicateAndSync(bases)
  }
})
