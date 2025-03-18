const test = require('brittle')
const b4a = require('b4a')

const {
  create,
  addWriter,
  confirm
} = require('./helpers')

test('fork - one writer to another', async t => {
  let forked = false

  const { bases } = await create(2, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: async (batch, view, host) => {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await host.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (value.fork) {
          const indexers = value.fork.indexers.map(key => b4a.from(key, 'hex'))
          const encryptionKey = b4a.from(value.fork.encryptionKey, 'hex')
          value.fork.system.key = b4a.from(value.fork.system.key, 'hex')

          t.is(await host.fork(indexers, value.fork.system, encryptionKey), !forked)
          forked = true
        }

        if (view) await view.append(value)
      }
    }
  })

  const [base1, base2] = bases

  await base1.append('one')
  await base1.append('two')
  await base1.append('three')

  await addWriter(base1, base2, false)
  await confirm(bases)

  t.is(base1.view.signedLength, 3)
  t.is(base2.view.signedLength, 3)

  t.is(base2.system.indexers.length, 1)
  t.alike(base2.system.indexers[0].key, base1.local.key)

  await base2.append({
    fork: {
      indexers: [b4a.toString(base2.local.key, 'hex')],
      system: {
        key: b4a.toString(base2.system.core.key, 'hex'),
        length: base2.indexedLength
      },
      encryptionKey: b4a.alloc(32, 1)
    }
  })

  t.is(base2.view.length, 4)

  await t.execution(base2.append('post fork'))

  t.is(base2.view.length, 5)
  t.is(base2.encryption.core.signedLength, 2)
  t.alike(base2.system.indexers[0].key, base2.local.key)

  t.is(await base2.view.get(4), 'post fork')
})
