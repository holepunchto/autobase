const c = require('compact-encoding')
const messages = require('./messages.js')

module.exports = async function boot (corestore, key, { encrypt, encryptionKey, keyPair } = {}) {
  const result = {
    key: null,
    local: null,
    bootstrap: null,
    encryptionKey: null,
    boot: null
  }

  const manifest = keyPair
    ? { version: corestore.manifestVersion, signers: [{ publicKey: keyPair.publicKey }] }
    : null

  if (key) {
    result.key = key

    const bootstrap = corestore.get({ key, active: false, valueEncoding: messages.OplogMessage })
    await bootstrap.ready()

    const localKey = await bootstrap.getUserData('autobase/local')

    if (keyPair) {
      result.local = corestore.get({ keyPair, active: false, exclusive: true, valueEncoding: messages.OplogMessage, manifest })
    } else {
      if (bootstrap.writable && !localKey) {
        result.local = bootstrap.session({ active: false, exclusive: true, valueEncoding: messages.OplogMessage })
      } else {
        const local = localKey
          ? corestore.get({ key: localKey, active: false, exclusive: true, valueEncoding: messages.OplogMessage })
          : corestore.get({ name: 'local', active: false, exclusive: true, valueEncoding: messages.OplogMessage })

        await local.ready()
        result.local = local
      }
    }

    result.bootstrap = bootstrap
  } else {
    result.local = keyPair
      ? corestore.get({ keyPair, manifest, active: false, exclusive: true, valueEncoding: messages.OplogMessage })
      : corestore.get({ name: 'local', active: false, exclusive: true, valueEncoding: messages.OplogMessage })
    await result.local.ready()

    const key = await result.local.getUserData('referrer')
    if (key) {
      result.key = key
      result.bootstrap = corestore.get({ key, active: false, valueEncoding: messages.OplogMessage })
      await result.bootstrap.ready()
    } else {
      result.key = result.local.key
      result.bootstrap = result.local.session({ active: false, valueEncoding: messages.OplogMessage })
      await result.bootstrap.setUserData('autobase/local', result.local.key)
    }
  }

  if (key || keyPair) {
    await result.bootstrap.setUserData('referrer', result.key)
    await result.bootstrap.setUserData('autobase/local', result.local.key)
    await result.local.setUserData('referrer', result.key)
  }

  const [encryptionKeyBuffer, pointer] = await Promise.all([
    result.local.getUserData('autobase/encryption'),
    result.local.getUserData('autobase/boot')
  ])

  if (pointer) {
    result.boot = c.decode(messages.BootRecord, pointer)
  }

  if (encryptionKeyBuffer) {
    result.encryptionKey = encryptionKeyBuffer
  }

  if (!result.encryptionKey && (encryptionKey || encrypt)) {
    if (!encryptionKey) encryptionKey = (await corestore.createKeyPair('autobase/encryption')).secretKey.subarray(0, 32)
    await result.bootstrap.setUserData('autobase/encryption', encryptionKey) // legacy support
    await result.local.setUserData('autobase/encryption', encryptionKey)
    result.encryptionKey = encryptionKey
  }

  return result
}
