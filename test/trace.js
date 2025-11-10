const test = require('brittle')

const {
  create,
  createBase,
  addWriter,
  confirm,
  compare,
  replicate,
  replicateAndSync,
  sync
} = require('./helpers')
const { replicateAndSyncDebugStream } = require('./helpers/networking.js')

test('trace - local block includes trace', async (t) => {
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
    t.alike(
      node.trace.user,
      [{ view: 0, blocks: [0] }],
      '2nd includes trace of view block from 1st append'
    )
  }

  async function apply(batch, view, host) {
    for (const { value } of batch) {
      let str = ''
      if (view.length) str += await view.get(view.length - 1)
      str += value

      await view.append(str)
    }
  }
})

test('trace - local optimistic block includes trace', async (t) => {
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
    t.alike(
      node.trace.user,
      [{ view: 0, blocks: [0] }],
      '2nd includes trace of view block from 1st append'
    )
  }

  async function apply(batch, view, host) {
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

test('trace - gets blocks needed from view before apply', async (t) => {
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

  async function apply(batch, view, host) {
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

test('trace - skips unindex view blocks', async (t) => {
  const { bases } = await create(1, t, { apply })
  const [a] = bases

  const batch = []
  for (let i = 0; i < 100; i++) {
    batch.push('a' + i)
  }
  await a.append(batch)

  const postIndexingIndex = a.local.length
  await a.append('a' + postIndexingIndex)

  {
    const node = await a.local.get(0)
    t.absent(node.trace, 'first append has no trace')
  }

  {
    const node = await a.local.get(1)
    t.absent(node.trace, '2nd append also has no trace')
  }

  {
    const node = await a.local.get(postIndexingIndex - 1)
    t.absent(node.trace, 'last block before index still not traced')
  }

  {
    const node = await a.local.get(postIndexingIndex)
    t.alike(node.trace.user, [{ view: 0, blocks: [99] }], 'block after indexing has trace')
  }

  async function apply(batch, view, host) {
    for (const { value } of batch) {
      if (view.length) await view.get(view.length - 1)

      await view.append(value)
    }
  }
})

test('trace - MAX_TRACE_PER_VIEW', async (t) => {
  const { bases } = await create(1, t, { apply })
  const [a] = bases

  const attemptedTrace = 300

  const batch = []
  for (let i = 0; i < 256; i++) {
    batch.push('a' + i)
  }
  await a.append(batch)

  for (let i = 256; i < attemptedTrace; i++) {
    await a.append('a' + i)
  }

  {
    const node = await a.local.get(0)
    t.absent(node.trace, 'first append has no trace')
  }

  {
    const node = await a.local.get(299)
    const viewTrace = node.trace.user.find((trace) => trace.view === 0)
    t.alike(viewTrace.blocks.length, 256, 'later append has max traced blocks')
  }

  async function apply(batch, view, host) {
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

test('trace - writer references non-existent block in trace still apply', async (t) => {
  const { bases } = await create(2, t)
  const [a, b] = bases

  await a.append('a1')

  await addWriter(a, b, false)
  await confirm(bases)

  const futureIndex = 1e6

  const done = replicate([a, b])
  await t.execution(
    b._applyState._flush([
      {
        value: 'beep',
        heads: [],
        batch: 1,
        optimistic: false,
        trace: {
          system: [],
          encryption: [],
          user: [{ view: 0, blocks: [futureIndex] }]
        }
      }
    ]),
    'peer writes block with non-existent block in trace'
  )
  await b.append('something else')

  await sync(bases)

  for (const peer of a.view.replicator.peers) {
    t.is(peer.inflight, 0, 'peer has no inflights')
  }

  await done

  t.is(await a.view.get(a.view.length - 2), 'beep')
  t.absent(await a.view.has(futureIndex), '"future" index is absent')
})

test('trace - non-indexed views arent traced', async (t) => {
  const { bases, stores } = await create(2, t, {
    open(store) {
      return { v1: store.get('view1', { valueEncoding: 'json' }) }
    },
    async apply(batch, view, base) {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await base.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (view && view.v1) {
          if (view.v1.length) await view.v1.get(0)

          await view.v1.append(value)
        }
      }
    }
  })
  const [a, b] = bases

  await a.append('a1')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.close()

  // Open with new view core
  const b2 = createBase(stores[1], a.local.key, t, {
    open(store) {
      return {
        v1: store.get('view1', { valueEncoding: 'json' }),
        v2: store.get('view2', { valueEncoding: 'json' })
      }
    },
    async apply(batch, view, base) {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await base.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (view && view.v1) {
          if (view.v1.length) await view.v1.get(0)

          await view.v1.append(value)
        }
        if (view && view.v2) {
          if (view.v2.length) {
            await view.v2.get(0)
          }

          await view.v2.append(value)
        }
      }
    }
  })
  await b2.ready()
  await b2.update()

  await b2.append('b2 1') // will be first block on v2 view core
  await b2.append('b2 2')

  {
    const node = await b2.local.get(1)
    t.alike(node.trace.user, [{ view: 0, blocks: [0] }], 'no trace w/ new view core')
  }
})

test('trace - reduces sync time - 1 writer', async (t) => {
  const { bases } = await create(4, t, { apply, fastForward: true })
  const [a, b, c, d] = bases
  const indexers = [a, b, c]

  await addWriter(a, b, true)
  await addWriter(a, c, true)
  await confirm(indexers)

  t.alike(
    a.system.indexers.map((i) => i.key),
    indexers.map((i) => i.local.key)
  )

  await addWriter(a, d, false)

  t.comment('confirming non indexer')
  await confirm(bases)

  const tipLength = 3000
  const traceableLength = tipLength

  for (let i = 0; i < traceableLength; i++) {
    await a.append(a.name + i)
  }
  await confirm(indexers)
  t.comment('tracable indexed')

  t.is(a.view.signedLength, traceableLength, 'traceable section is signed')

  for (let i = 0; i < tipLength; i++) {
    await a.append(a.name + (i + traceableLength))
  }
  t.comment('tip appended')

  // Assert that `d` cannot already have blocks as safeguard
  for (let i = 0; i < a.view.length; i++) {
    if (await d.view.has(i)) throw Error('`d` already has blocks!')
  }

  t.comment('joining')
  const latency = 50

  const joiningStart = Date.now()
  await replicateAndSyncDebugStream([a, d], t, { latency })
  const joiningEnd = Date.now()
  const joiningTime = joiningEnd - joiningStart

  // Estimate processing time
  const roundTripLatency = 2 * latency
  const idealBatches = tipLength / 64
  const netRequests = idealBatches * 2 // 1 request & 1 data
  const timeEstimate =
    (netRequests * roundTripLatency) / 2 + // Assume half-max batches
    tipLength // Assume each block takes 1ms to process

  t.comment('joiningTime', `${joiningTime / 1000}s`, 'timeEstimate', `${timeEstimate / 1000}s`)
  t.ok(joiningTime < timeEstimate * 2.0, 'joining time within double of estimate')

  await compare(a, d, true)
  t.pass('a & d match')

  async function apply(batch, view, host) {
    for (const { value } of batch) {
      if (value.add) {
        const key = Buffer.from(value.add, 'hex')
        await host.addWriter(key, { indexer: value.indexer })
        continue
      }

      let str = ''
      if (view.length >= traceableLength) {
        const viewIndex = (view.length - traceableLength) % traceableLength
        str += await view.get(viewIndex)
      }
      str += value

      await view.append(str)
    }
  }
})

test(
  'trace - reduces sync time for well behaved non-indexed blocks',
  { timeout: 2 * 60 * 1000 },
  async (t) => {
    const { bases, stores } = await create(5, t, { apply, fastForward: true })
    const [a, b, c, d, e] = bases
    const indexers = [a, b, c]

    await addWriter(a, b, true)
    await addWriter(a, c, true)
    await confirm(indexers)

    t.alike(
      a.system.indexers.map((i) => i.key),
      indexers.map((i) => i.local.key)
    )

    await addWriter(a, d, false)
    await addWriter(a, e, false)

    t.comment('confirming non indexer')
    await confirm(bases)

    const initialLength = 100
    const tipLength = 100

    for (let i = 0; i < initialLength; i++) {
      await a.append(a.name + i)
    }
    t.comment('reference blocks appended')

    await confirm([a, d]) // Sync tip writer before quorum
    await confirm(indexers) // Reach quorum

    t.is(a.view.signedLength, a.view.length, 'A view is all signed')
    t.is(d.view.signedLength, 0, 'D doesnt know about signed view blocks')

    for (let i = 0; i < tipLength; i++) {
      await d.append(d.name + i)
    }

    t.comment('tip w/ unindex trace appended')

    await confirm([d, a]) // d updates a so initial view blocks are now indexed

    t.is(d.view.length, a.view.length, 'A & D views match')
    t.not(a.view.signedLength, a.view.length, 'A has unsigned view blocks')

    e.once('fast-forward', () => {
      t.pass('e ffed from a and didnt run apply for initial blocks')
    })

    // Assert that `e` cannot already have blocks as safeguard
    for (let i = 0; i < a.view.length; i++) {
      if (await e.view.has(i)) throw Error('`e` already has blocks!')
    }

    t.comment('joining')
    const latency = 100

    const joiningStart = Date.now()
    await replicateAndSyncDebugStream([a, e], t, { latency })
    const joiningEnd = Date.now()
    const joiningTime = joiningEnd - joiningStart

    // Estimate processing time
    const roundTripLatency = 2 * latency
    const idealBatches = tipLength / 64
    const netRequests = idealBatches * 2 // 1 request & 1 data
    const timeEstimate =
      netRequests * roundTripLatency * 2 + // Assume half-max batches
      tipLength // Assume each block takes 1ms to process

    t.comment('joiningTime', `${joiningTime / 1000}s`, 'timeEstimate', `${timeEstimate / 1000}s`)
    t.ok(joiningTime < timeEstimate * 2.0, 'joining time within double of estimate')

    await compare(a, d, true)
    t.pass('a & d match')

    async function apply(batch, view, host) {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await host.addWriter(key, { indexer: value.indexer })
          continue
        }

        let str = ''
        if (view.length > 0) {
          const index = (view.length - 1) % initialLength
          str += await view.get(index)
        }
        str += value

        await view.append(str)
      }
    }
  }
)
