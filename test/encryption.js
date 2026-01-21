const test = require('brittle')
const tmpDir = require('test-tmp')
const Corestore = require('corestore')
const b4a = require('b4a')
const BlindEncryptionSodium = require('blind-encryption-sodium')

const Autobase = require('..')

test('encryption - basic', async (t) => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const base = new Autobase(store, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey: b4a.alloc(32).fill('secret')
  })
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

test('encryption - restart', async (t) => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const base = new Autobase(store, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey: b4a.alloc(32).fill('secret')
  })
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

test('encryption - expect encryption key', async (t) => {
  const storage = await tmpDir(t)
  const store = new Corestore(storage)
  const base = new Autobase(store, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encrypted: true
  })

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

test('encryption - pass as promise', async (t) => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const key = b4a.alloc(32).fill('secret')
  const encryptionKey = new Promise((resolve) => setTimeout(resolve, 1000, key))

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

test('encryption - encrypt/decrypt - rotate', async (t) => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const encryptionKey = b4a.alloc(32, 'secret')
  const password = b4a.alloc(32, 'mypassword')
  const newPassword = b4a.alloc(32, 'myNewPassword')

  const blindEncryption = new BlindEncryptionSodium([{ key: password, type: 0 }])
  const blindEncryptionNew = new BlindEncryptionSodium([
    { key: password, type: 0 },
    { key: newPassword, type: 1 }
  ])
  const b = t.test('blindEncryption')
  b.plan(3)

  const base = new Autobase(store, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    blindEncryption: {
      encrypt: (...args) => {
        b.pass('called encrypt')
        return blindEncryption.encrypt(...args)
      },
      decrypt: (...args) => {
        b.fail('called decrypt')
        return blindEncryption.decrypt(...args)
      }
    }
  })

  await base.ready()

  {
    const [encryptionKeyBuffer, encryptionKeyEncryptedBuffer] = await Promise.all([
      base.local.getUserData('autobase/encryption'),
      base.local.getUserData('autobase/blind-encryption')
    ])

    t.absent(encryptionKeyBuffer)
    t.ok(encryptionKeyEncryptedBuffer)
  }

  t.ok(base.encryptionKey)

  await base.append('you should still not see me')
  await base.close()

  t.is(store.cores.size, 0)

  const store2 = new Corestore(tmp)
  const base2 = new Autobase(store2, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    blindEncryption: {
      encrypt: (...args) => {
        b.pass('rotate')
        return blindEncryptionNew.encrypt(...args)
      },
      decrypt: (...args) => {
        b.pass('called decrypt before rotate')
        return blindEncryptionNew.decrypt(...args)
      }
    }
  })

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

  const b2 = t.test('blindEncryption new password')
  b2.plan(1)

  const store3 = new Corestore(tmp)
  const base3 = new Autobase(store3, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    blindEncryption: {
      encrypt: (...args) => {
        b2.fail('called encrypt after already rotated')
        return blindEncryptionNew.encrypt(...args)
      },
      decrypt: (...args) => {
        b2.pass('called decrypt with rotated')
        return blindEncryptionNew.decrypt(...args)
      }
    }
  })

  t.alike(await base3.view.get(0), 'you should still not see me')
  t.ok(base3.encryptionKey)

  await base3.close()
})

test('encryption - encrypt/decrypt', async (t) => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const encryptionKey = b4a.alloc(32, 'secret')
  const password = b4a.alloc(32, 'mypassword')

  const blindEncryption = new BlindEncryptionSodium([{ key: password, type: 0 }])
  const b = t.test('blindEncryption')
  b.plan(2)

  const base = new Autobase(store, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    blindEncryption: {
      encrypt: (...args) => {
        b.pass('called encrypt')
        return blindEncryption.encrypt(...args)
      },
      decrypt: (...args) => {
        b.fail('called decrypt')
        return blindEncryption.decrypt(...args)
      }
    }
  })

  await base.ready()

  {
    const [encryptionKeyBuffer, encryptionKeyEncryptedBuffer] = await Promise.all([
      base.local.getUserData('autobase/encryption'),
      base.local.getUserData('autobase/blind-encryption')
    ])

    t.absent(encryptionKeyBuffer)
    t.ok(encryptionKeyEncryptedBuffer)
  }

  t.ok(base.encryptionKey)

  await base.append('you should still not see me')
  await base.close()

  t.is(store.cores.size, 0)

  const store2 = new Corestore(tmp)
  const base2 = new Autobase(store2, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    blindEncryption: {
      encrypt: (...args) => {
        b.fail('called encrypt on reboot')
        return blindEncryption.encrypt(...args)
      },
      decrypt: (...args) => {
        b.pass('called decrypt on reboot')
        return blindEncryption.decrypt(...args)
      }
    }
  })

  t.alike(await base2.view.get(0), 'you should still not see me')
  t.ok(base2.encryptionKey)

  {
    const [encryptionKeyBuffer, encryptionKeyEncryptedBuffer] = await Promise.all([
      base2.local.getUserData('autobase/encryption'),
      base2.local.getUserData('autobase/blind-encryption')
    ])

    t.absent(encryptionKeyBuffer)
    t.ok(encryptionKeyEncryptedBuffer)
  }

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

test('encryption - encrypt/decrypt - compat', async (t) => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)

  const encryptionKey = b4a.alloc(32, 'secret')
  const password = b4a.alloc(32, 'mypassword')

  const blindEncryption = new BlindEncryptionSodium([{ key: password, type: 0 }])
  const b = t.test('blindEncryption')
  b.plan(2)

  const base = new Autobase(store, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey
  })

  await base.ready()

  {
    const [encryptionKeyBuffer, encryptionKeyEncryptedBuffer] = await Promise.all([
      base.local.getUserData('autobase/encryption'),
      base.local.getUserData('autobase/blind-encryption')
    ])

    t.ok(encryptionKeyBuffer)
    t.absent(encryptionKeyEncryptedBuffer)
  }

  t.ok(base.encryptionKey)

  await base.append('you should still not see me')
  await base.close()

  t.is(store.cores.size, 0)

  const store2 = new Corestore(tmp)
  const base2 = new Autobase(store2, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    blindEncryption: {
      encrypt: (...args) => {
        // will encrypt the old plain text one
        b.pass('called encrypt on reboot')
        return blindEncryption.encrypt(...args)
      },
      decrypt: (...args) => {
        b.fail('called decrypt on reboot')
        return blindEncryption.decrypt(...args)
      }
    }
  })

  t.alike(await base2.view.get(0), 'you should still not see me')
  t.ok(base2.encryptionKey)

  // replaced encryption with blind-encryption!
  {
    const [encryptionKeyBuffer, encryptionKeyEncryptedBuffer] = await Promise.all([
      base2.local.getUserData('autobase/encryption'),
      base2.local.getUserData('autobase/blind-encryption')
    ])

    t.absent(encryptionKeyBuffer)
    t.ok(encryptionKeyEncryptedBuffer)
  }

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

  const store3 = new Corestore(tmp)
  const base3 = new Autobase(store3, {
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    blindEncryption: {
      encrypt: (...args) => {
        b.fail('called encrypt on reboot#2')
        return blindEncryption.encrypt(...args)
      },
      decrypt: (...args) => {
        b.pass('called decrypt on reboot#2')
        return blindEncryption.decrypt(...args)
      }
    }
  })
  await base3.ready()

  {
    const [encryptionKeyBuffer, encryptionKeyEncryptedBuffer] = await Promise.all([
      base3.local.getUserData('autobase/encryption'),
      base3.local.getUserData('autobase/blind-encryption')
    ])

    t.absent(encryptionKeyBuffer)
    t.ok(encryptionKeyEncryptedBuffer)
  }

  await base3.close()
})

function open(store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply(batch, view, base) {
  for (const { value } of batch) {
    await view.append(value.toString())
  }
}
