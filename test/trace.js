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
