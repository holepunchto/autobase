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

test('fast-forward - signal preferred ff', { timeout: 999999999 }, async (t) => {
  t.plan(1)

  let slow = false

  const { bases } = await create(3, t, {
    fastForward: true,
    storage: () => tmpDir(t),
    async apply(nodes, view, host) {
      for (const node of nodes) {
        if (node.value.add) {
          await host.addWriter(b4a.from(node.value.add, 'hex'), { indexer: true })
          continue
        }

        await view.append(node.value)
        if (slow) {
          host.preferFastForward()
          await view.get(1000) // just a hack to pretend a network stall
          t.fail('should not get here')
        }
      }
    }
  })

  const [a, b, c] = bases

  await a.append('a0')

  await replicateAndSync([a, b])

  await a.append('a1')

  slow = true // pretend replication stalls

  await replicateAndSync([a, b])

  slow = false // pretend replication stalls

  await addWriter(a, c)
  await replicateAndSync([a, c])
  console.log(c.writable)
  await c.append('c')
  await replicateAndSync([a, c])

  console.log(a.linearizer.indexers.length)
  await replicateAndSync([a, b])

  t.is(b.core.length, a.core.length, 'did not get stuck, length is ' + b.core.length)
})
