const test = require('brittle')
const tmpDir = require('test-tmp')
const Corestore = require('corestore')
const b4a = require('b4a')
const HypercoreEncryption = require('hypercore-encryption')

const Autobase = require('..')
const { replicateAndSync, createStores, create } = require('./helpers')

test('encryption - basic', async t => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const base = new Autobase(store, { apply, open, ackInterval: 0, ackThreshold: 0, encryptionKey: b4a.alloc(32).fill('secret') })
  await base.ready()

  t.ok(base.encryptionKey)

  await base.append('you should not see me')

  t.alike(await base.view.get(0), 'you should not see me')
  t.is(base.view.signedLength, 1)
  t.is(base.system.core.signedLength, 3)

  let found = false

  for (const core of store.cores) {
    const session = store.get(core.key)
    await session.setEncryptionKey(null) // ensure no auto decryption

    for (let i = 0; i < core.length; i++) {
      const buf = await session.get(i, { valueEncoding: 'ascii' })
      if (buf.indexOf('you should not see me') > -1) found = true
    }

    await session.close()
  }

  t.absent(found)

  await base.close()
})

test('encryption - restart', async t => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const base = new Autobase(store, { apply, open, ackInterval: 0, ackThreshold: 0, encryptionKey: b4a.alloc(32).fill('secret') })
  await base.ready()

  t.ok(base.encryptionKey)

  await base.append('you should still not see me')
  await base.close()

  t.is(store.cores.size, 0)

  const store2 = new Corestore(tmp)
  const base2 = new Autobase(store2, { apply, open, ackInterval: 0, ackThreshold: 0 })

  t.alike(await base2.view.get(0), 'you should still not see me')
  t.ok(base2.encryptionKey)

  let found = false

  for (const core of store2.cores) {
    const session = store2.get(core.key)
    await session.setEncryptionKey(null) // ensure no auto decryption
    for (let i = 0; i < session.length; i++) {
      const buf = await session.get(i, { valueEncoding: 'ascii' })
      if (buf.indexOf('you should still not see me') > -1) found = true
    }
    await session.close()
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

test('encryption - pass as promise', async t => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const key = b4a.alloc(32).fill('secret')
  const encryptionKey = new Promise(resolve => setTimeout(resolve, 1000, key))

  const base = new Autobase(store, { apply, open, ackInterval: 0, ackThreshold: 0, encryptionKey })
  await base.ready()

  t.alike(base.encryptionKey, key)

  await base.append('you should not see me')

  t.alike(await base.view.get(0), 'you should not see me')
  t.is(base.view.signedLength, 1)
  t.is(base.system.core.signedLength, 3)

  let found = false

  for (const core of store.cores) {
    const session = store.get(core.key)
    await session.setEncryptionKey(null) // ensure no auto decryption

    for (let i = 0; i < core.length; i++) {
      const buf = await session.get(i, { valueEncoding: 'ascii' })
      if (buf.indexOf('you should not see me') > -1) found = true
    }

    await session.close()
  }

  t.absent(found)

  await base.close()
})

test('encryption - rotate key', async t => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const encryptionKey = b4a.alloc(32).fill('secret')

  const base = new Autobase(store, null, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    valueEncoding: 'json'
  })

  await base.ready()

  t.alike(base.encryptionKey, encryptionKey)

  await base.append('encryption key 1')

  const entropy = b4a.alloc(32, 2)
  const recipients = await base.listMemberPublicKeys()

  await base.append({
    encryption: HypercoreEncryption.broadcastEncrypt(entropy, recipients)
  })

  await base.append('encryption key 2')

  t.alike(await base.view.get(0), 'encryption key 1')
  t.alike(await base.view.get(1), 'encryption key 2')

  t.is(base.view.signedLength, 2)
  t.is(base.system.core.signedLength, 7)

  await base.close()
})

test('encryption - rotate key with replication', async t => {
  const stores = await createStores(2, t)

  const encryptionKey = b4a.alloc(32).fill('secret')

  const a = new Autobase(stores[0], null, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    valueEncoding: 'json'
  })

  await a.ready()

  const b = new Autobase(stores[1], a.key, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    valueEncoding: 'json'
  })

  await b.ready()

  await a.append({ add: { key: b.local.key.toString('hex'), indexer: false } })
  await a.append('encryption key 1:0')

  await replicateAndSync([a, b])

  const entropy = b4a.alloc(32, 2)
  const recipients = await b.listMemberPublicKeys()

  await b.append({
    encryption: HypercoreEncryption.broadcastEncrypt(entropy, recipients)
  })

  await b.append('encryption key 2:0')
  await a.append('encryption key 1:1')

  await replicateAndSync([a, b])

  await b.append('encryption key 2:2')
  await a.append('encryption key 2:1')

  await replicateAndSync([a, b])

  t.alike(await a.view.get(0), 'encryption key 1:0')
  t.alike(await a.view.get(1), 'encryption key 1:1')
  t.alike(await a.view.get(2), 'encryption key 2:0')
  t.alike(await a.view.get(3), 'encryption key 2:1')
  t.alike(await a.view.get(4), 'encryption key 2:2')

  t.is(a.view.length, 5)

  await a.close()
  await b.close()
})

test('encryption - rotate key with writer removal', async t => {
  const encryptionKey = b4a.alloc(32).fill('secret')

  const { bases } = await create(3, t, {
    apply,
    open,
    encryptionKey
  })

  const [a, b, c] = bases

  await add(a, b, false)
  await add(a, c, false)

  await replicateAndSync([a, b, c])

  await b.append(null)
  await c.append(null)

  await replicateAndSync([a, b, c])

  await a.append('c can see me')

  await remove(a, c, false)
  await rotate(a, b4a.alloc(32, 1))

  await a.append('c cannot see me')

  await t.execution(replicateAndSync([a, b]))
  await t.exception(replicateAndSync([a, c]))

  t.alike(await a.view.get(0), 'c can see me')
  t.alike(await b.view.get(0), 'c can see me')

  t.alike(await a.view.get(1), 'c cannot see me')
  t.alike(await b.view.get(1), 'c cannot see me')

  await t.exception(c.view.get(1))

  await a.close()
  await b.close()
})

test('encryption - fast forward', async t => {
  const encryptionKey = b4a.alloc(32).fill('secret')

  const { bases } = await create(2, t, {
    apply,
    open,
    encryptionKey,
    fastForward: true
  })

  const [a, b] = bases

  await add(a, b, false)

  await replicateAndSync([a, b])
  await b.append(null)
  await replicateAndSync([a, b])

  await rotate(a, b4a.alloc(32, 1))
  for (let i = 0; i < 200; i++) {
    await a.append('interval 1 - ' + i)
  }

  await rotate(a, b4a.alloc(32, 2))
  for (let i = 0; i < 200; i++) {
    await a.append('interval 2 - ' + i)
  }

  await rotate(a, b4a.alloc(32, 3))
  for (let i = 0; i < 200; i++) {
    await a.append('interval 3 - ' + i)
  }

  await rotate(a, b4a.alloc(32, 4))
  for (let i = 0; i < 200; i++) {
    await a.append('interval 4 - ' + i)
  }
  await rotate(a, b4a.alloc(32, 5))
  for (let i = 0; i < 200; i++) {
    await a.append('interval 5 - ' + i)
  }

  const ff = t.execution(new Promise((resolve, reject) => {
    b.on('fast-forward', pass)
    const t = setTimeout(cleanup, 1000, false)

    function pass () {
      return cleanup(true)
    }

    function cleanup (ffed) {
      b.removeListener('fast-foward', pass)
      clearTimeout(t)

      if (ffed) resolve()
      else reject(new Error('timeout'))
    }
  }))

  await replicateAndSync([a, b])
  await ff

  t.is(a.view.length, 1000)
  t.is(b.view.length, 1000)

  t.alike(await a.view.get(999), 'interval 5 - 199')
  t.alike(await b.view.get(999), 'interval 5 - 199')

  await a.close()
  await b.close()
})

test('encryption - rotate writer encryption', async t => {
  const encryptionKey = b4a.alloc(32).fill('secret')

  const { bases } = await create(2, t, {
    apply,
    open,
    encryptionKey
  })

  const [a, b] = bases

  await add(a, b, false)
  await replicateAndSync([a, b])

  await b.append(null)
  await replicateAndSync([a, b])

  await a.append('c can see me')

  const newLocal = b.store.get({
    manifest: {
      version: 2,
      signers: [{
        publicKey: b.local.keyPair.publicKey
      }]
    },
    keyPair: b.local.keyPair
  })

  await newLocal.ready()

  // rotate b writer
  await remove(a, b, false)
  await b.setLocal(newLocal.key)
  await add(a, b, false)

  await replicateAndSync([a, b])

  await b.append('encrypted!')

  await rotate(a, b4a.alloc(32, 1))

  await a.append('c cannot see me')

  await t.execution(replicateAndSync([a, b]))

  await b.append('rotate and encrypted!')

  await replicateAndSync([a, b])

  t.alike(await a.view.get(0), 'c can see me')
  t.alike(await b.view.get(0), 'c can see me')

  t.alike(await a.view.get(1), 'c cannot see me')
  t.alike(await b.view.get(1), 'c cannot see me')

  await a.close()
  await b.close()
})

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply (batch, view, base) {
  for (const { value } of batch) {
    if (value.encryption) {
      await base.updateEncryption(Buffer.from(value.encryption))
      continue
    }

    if (value.add) {
      await base.addWriter(Buffer.from(value.add.key, 'hex'), { indexer: value.add.indexer })
      continue
    }

    if (value.remove) {
      await base.removeWriter(Buffer.from(value.remove.key, 'hex'))
      continue
    }

    await view.append(value.toString())
  }
}

function add (base, peer, indexer) {
  return base.append({ add: { key: peer.local.key.toString('hex'), indexer } })
}

function remove (base, peer, index) {
  return base.append({ remove: { key: peer.local.key.toString('hex') } })
}

async function rotate (base, entropy) {
  const recipients = await base.listMemberPublicKeys()
  return base.append({ encryption: HypercoreEncryption.broadcastEncrypt(entropy, recipients) })
}
