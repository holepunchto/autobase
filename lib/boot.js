const c = require('compact-encoding')
const messages = require('./messages.js')

module.exports = async function boot (corestore, key, { encrypt, encryptionKey, keyPair, local } = {}) {
  const result = {
    key: null,
    local: null,
    bootstrap: null,
    encryptionKey: null,
    boot: null
  }

  if (key) {
    result.key = key

    const bootstrap = corestore.get({ key, active: false, valueEncoding: messages.OplogMessage })
    await bootstrap.ready()

    const localKey = await bootstrap.getUserData('autobase/local')

    if (local) {
      result.local = local.session({ active: false, exclusive: true, valueEncoding: messages.OplogMessage })
    } else {
      if (bootstrap.writable && !localKey) {
        result.local = bootstrap.session({ active: false, exclusive: true, valueEncoding: messages.OplogMessage })
      } else {
        const local = localKey
          ? corestore.get({ key: localKey, active: false, exclusive: true, valueEncoding: messages.OplogMessage })
          : getLocalCore(corestore, keyPair)

        await local.ready()
        result.local = local
      }
    }

    if (!localKey || local) {
      await bootstrap.setUserData('referrer', key)
      await bootstrap.setUserData('autobase/local', result.local.key)
      await result.local.setUserData('referrer', key)
    }

    result.bootstrap = bootstrap
  } else {
    result.local = local ? local.session({ active: false, exclusive: true, valueEncoding: messages.OplogMessage }) : getLocalCore(corestore, keyPair)
    result.local.ready()

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

  if (result.encryptionKey) {
    await result.local.setEncryptionKey(result.encryptionKey)
    await result.bootstrap.setEncryptionKey(result.encryptionKey)
  }

  return result
}

function getLocalCore (corestore, keyPair) {
  if (keyPair) return corestore.get({ keyPair, active: false, exclusive: true, valueEncoding: messages.OplogMessage })
  return corestore.get({ name: 'local', active: false, exclusive: true, valueEncoding: messages.OplogMessage })
}
