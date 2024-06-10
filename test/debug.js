const test = require('brittle')
const tmpDir = require('test-tmp')

const {
  addWriterAndSync,
  replicateAndSync,
  confirm,
  create
} = require('./helpers')

for (let i = 0; i < 1000; i++) {
  test('fast-forward - unindexed cores should migrate', async t => {
    const { bases } = await create(4, t, {
      fastForward: true,
      storage: () => tmpDir(t)
    })

    const [a, b, c, d] = bases

    await addWriterAndSync(a, b)
    await addWriterAndSync(a, c, false)

    await b.append('b')

    // c opens view but never indexes
    await c.append('c')

    await confirm([a, b, c])
    await replicateAndSync([a, b, c, d])

    if (a.system.core.signedLength !== c.system.core.signedLength) {
      console.log(a.system.core.indexedLength, a.system.getIndexedInfo())
      console.log(c.system.core.indexedLength, c.system.getIndexedInfo())
    }

    t.is(a.system.core.signedLength, c.system.core.signedLength)
  })
}
