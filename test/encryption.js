const test = require('brittle')
const tmpDir = require('test-tmp')
const Corestore = require('corestore')
const b4a = require('b4a')

const Autobase = require('..')

test('encryption - basic', async t => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)
  const base = new Autobase(store, { apply, open, ackInterval: 0, ackThreshold: 0, encryptionKey: b4a.alloc(32).fill('secret') })

  t.ok(base.encryptionKey)

  await base.append('you should not see me')

  t.alike(await base.view.get(0), 'you should not see me')
  t.is(base.view.signedLength, 1)
  t.is(base.system.core.signedLength, 3)

  let found = false
  for (const core of store.sessions) {
    for (let i = 0; i < core.length; i++) {
      const session = core.session()
      await session.setEncryptionKey(null) // ensure no auto decryption
      const buf = await session.get(i, { valueEncoding: 'ascii' })
      if (buf.indexOf('you should not see me') > -1) found = true
      await session.close()
    }
  }

  t.absent(found)

  await base.close()
})

test('encryption - restart', async t => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)
  const base = new Autobase(store, { apply, open, ackInterval: 0, ackThreshold: 0, encryptionKey: b4a.alloc(32).fill('secret') })

  t.ok(base.encryptionKey)

  await base.append('you should still not see me')
  await base.close()

  t.is(store.cores.size, 0)

  const store2 = new Corestore(tmp)
  const base2 = new Autobase(store2, { apply, open, ackInterval: 0, ackThreshold: 0 })

  t.alike(await base2.view.get(0), 'you should still not see me')
  t.ok(base2.encryptionKey)

  let found = false

  for (const core of store2.sessions) {
    for (let i = 0; i < core.length; i++) {
      const session = core.session()
      await session.setEncryptionKey(null) // ensure no auto decryption
      const buf = await session.get(i, { valueEncoding: 'ascii' })
      if (buf.indexOf('you should still not see me') > -1) found = true
      await session.close()
    }
  }

  t.absent(found)

  await base2.close()
})

test('encryption - expect encryption key', async t => {
  const storage = await tmpDir(t)
  const store = new Corestore(storage)
  const base = new Autobase(store, { apply, open, ackInterval: 0, ackThreshold: 0, encrypted: true })

  try {
    await base.ready()
    t.fail()
  } catch (err) {
    t.is(err.message, 'Encryption key is expected')
  }

  const closing = base.close()
  await store.close()

  await closing
})

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply (batch, view, base) {
  for (const { value } of batch) {
    await view.append(value.toString())
  }
}
