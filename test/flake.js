const test = require('brittle')
const tmpDir = require('test-tmp')

const {
  addWriterAndSync,
  replicate,
  create,
  createBase
} = require('./helpers')

for (let i = 0; i < 1000; i++) {
  test('fast-forward - open with no remote io', async t => {
    const { bases, stores } = await create(2, t, {
      apply: applyOldState,
      fastForward: true,
      storage: () => tmpDir(t)
    })

    const [a, b] = bases

    await b.ready()

    for (let i = 0; i < 1000; i++) {
      await a.append('a' + i)
    }

    await addWriterAndSync(a, b)
    const unreplicate = replicate([a, b])

    const core = b.view.getBackingCore()
    const sparse = await isSparse(core)

    t.ok(sparse > 0)
    t.comment('sparse blocks: ' + sparse)

    await b.append('b1')
    await b.append('b2')
    await b.append('b3')

    await unreplicate()

    await a.append('a1001')

    await b.close()

    const local = a.local
    const remote = stores[1].get({ key: local.key })

    const s1 = local.replicate(true)
    const s2 = remote.replicate(false)

    s1.pipe(s2).pipe(s1)

    await remote.download({ end: local.length }).downloaded()

    s1.destroy()
    await new Promise(resolve => s2.on('close', resolve))

    const b2 = await createBase(stores[1].session(), a.local.key, t, {
      apply: applyOldState
    })

    b2.debug = true
    await b2.ready()
    console.log('READY')
    await t.execution(b2.ready())

    async function applyOldState (batch, view, base) {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await base.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (view) await view.append(value)
        const core = view._source.core.session

        // get well distributed unique index
        const index = (view.length * 67 + view.length * 89) % core.length
        if (core.length) await core.get(index)
      }
    }
  })
}

async function isSparse (core) {
  let n = 0
  for (let i = 0; i < core.length; i++) {
    if (!await core.has(i)) n++
  }
  return n
}
