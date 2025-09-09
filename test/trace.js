const test = require('brittle')

const {
  create,
  addWriter,
  confirm,
  replicate,
  replicateAndSync
} = require('./helpers')

test('trace - local block includes trace', async t => {
  const { bases } = await create(1, t, { apply })
  const [a] = bases

  await a.append('beep')
  await a.append('boop')

  {
    const node = await a.local.get(0)
    t.absent(node.trace, 'first append has no trace')
  }

  {
    const node = await a.local.get(1)
    t.alike(node.trace, [{ view: 1, blocks: [0] }], '2nd includes trace of view block from 1st append')
  }

  async function apply (batch, view, host) {
    for (const { value } of batch) {
      let str = ''
      if (view.length) str += await view.get(view.length - 1)
      str += value

      await view.append(str)
    }
  }
})

test('trace - local optimistic block includes trace', async t => {
  const { bases } = await create(2, t, { apply, optimistic: true })
  const [a, b] = bases

  await a.append('beep')

  await replicateAndSync(bases)

  t.absent(b.writable, 'b isnt writable')

  await b.append('optmistic', { optimistic: true })

  {
    const node = await a.local.get(0)
    t.absent(node.trace, 'first append has no trace')
  }

  {
    const node = await b.local.get(0)
    t.alike(node.trace, [{ view: 1, blocks: [0] }], '2nd includes trace of view block from 1st append')
  }

  async function apply (batch, view, host) {
    for (const node of batch) {
      const { value } = node
      let str = ''

      if (node.optimistic) await host.ackWriter(node.from.key)
      if (view.length) str += await view.get(view.length - 1)
      str += value

      await view.append(str)
    }
  }
})

test('trace - gets blocks needed from view before apply', async t => {
  t.plan(5)
  const { bases } = await create(3, t, { apply, fastForward: true })
  const [a, b, c] = bases

  let checkWarmup = false

  // Create view entries that will be sparse
  const primingLength = 200
  for (let i = 0; i < primingLength; i++) {
    await a.append({ value: 'a', prev: a.view.length - 1 })
  }

  c.once('fast-forward', () => {
    t.pass('c ffed from a')
  })

  await addWriter(a, b, false)
  await confirm(bases)

  // Replicate while appending so b can get the block
  const targetIndex = 100
  const done = replicate([a, b])
  await b.append({ value: 'b', prev: targetIndex })
  await done

  t.absent(await c.view.has(targetIndex))

  t.comment('checking warmup')
  checkWarmup = true

  await replicateAndSync([b, c])

  t.ok(await c.view.has(targetIndex), 'loaded block!')

  async function apply (batch, view, host) {
    for (const node of batch) {
      const { value } = node
      if (value.add) {
        const key = Buffer.from(value.add, 'hex')
        await host.addWriter(key, { indexer: value.indexer })
        continue
      }

      let str = ''
      if (view.length && value.prev >= 0) {
        const index = value.prev

        if (checkWarmup) {
          const haveBlock = await view.has(index)
          const blockIsReplicating = view.replicator._blocks.has(index)
          t.ok(haveBlock || blockIsReplicating, 'already warmed up : ' + host.base.name)
        }

        str += await view.get(index)
      }
      str += value.value

      await view.append(str)
    }
  }
})

test('trace - MAX_TRACE_PER_VIEW', async t => {
  const { bases } = await create(1, t, { apply })
  const [a] = bases

  const attemptedTrace = 300

  const batch = []
  for (let i = 0; i < attemptedTrace; i++) {
    batch.push('a' + i)
  }
  await a.append(batch)

  {
    const node = await a.local.get(0)
    t.absent(node.trace, 'first append has no trace')
  }

  {
    const node = await a.local.get(299)
    const viewTrace = node.trace.find((trace) => trace.view === 1)
    t.alike(viewTrace.blocks.length, 256, 'later append has max traced blocks')
  }

  async function apply (batch, view, host) {
    for (const { value } of batch) {
      if (view.length && view.length > 256) {
        // Request more than 256 (MAX_TRACE_PER_VIEW) blocks
        for (let i = 0; i < Math.min(view.length, attemptedTrace); i++) {
          await view.get(i)
        }
      }

      await view.append(value)
    }
  }
})

test('trace - writer references non-existent block in trace still apply', async t => {
  const { bases } = await create(2, t)
  const [a, b] = bases

  await a.append('a1')

  await addWriter(a, b, false)
  await confirm(bases)

  const futureIndex = 1e6

  t.teardown(replicate([a, b]))
  await t.execution(b._applyState._flush([
    {
      value: 'beep',
      heads: [],
      batch: 1,
      optimistic: false,
      trace: [{ view: 1, blocks: [futureIndex] }]
    }
  ]), 'peer writes block with non-existent block in trace')
  await b.append('something else')

  // a acks to update view
  await a.append(null)

  for (const peer of a.view.replicator.peers) {
    t.is(peer.inflight, 0, 'peer has no inflights')
  }

  t.is(await a.view.get(a.view.length - 2), 'beep')
  t.absent(await a.view.has(futureIndex), '"future" index is absent')
})
