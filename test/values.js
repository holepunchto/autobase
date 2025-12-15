const test = require('brittle')
const HypercoreEncryption = require('hypercore-encryption')
const Hypercore = require('hypercore')
const cc = require('compact-encoding')

const Autobase = require('../')
const { OplogMessage } = require('../lib/messages.js')
const { encodeValue, decodeValue } = require('../lib/values.js')

const { create } = require('./helpers')

test('decodeValue - decodes local writer block', async (t) => {
  const encryptionKey = Buffer.alloc(32).fill('secret')
  const { bases } = await create(1, t, { encryptionKey, valueEncoding: 'binary' })
  const [base] = bases

  await base.append('beep')

  const node = await getBlock(base.local, 0)

  const manifest = base.local.manifest
  t.alike(
    await decodeValue(node, {
      autobase: {
        isEncrypted: true,
        encryptionKey,
        bootstrap: base.key,
        local: { keyPair: base.local.keyPair }
      },
      key: base.local.key,
      manifest
    }),
    Buffer.from('beep')
  )
})

test('encode & decode value - unencrypted', async (t) => {
  const node = encodeValue(Buffer.from('beep'), { optimistic: true, encrypted: false })

  t.alike(
    await decodeValue(node, {
      autobase: {
        isEncrypted: false
      }
    }),
    Buffer.from('beep')
  )
})

test('encode & decode value - w/ passthrough encryption (type 0)', async (t) => {
  const encryptionKey = Buffer.alloc(32).fill('secret')
  const { bases } = await create(1, t, {
    encryptionKey,
    valueEncoding: 'binary',
    manifestVersion: 2
  })
  const [base] = bases

  const node = encodeValue(Buffer.from('encrypted'), { optimistic: true, encrypted: true })

  const manifest = base.local.manifest
  t.alike(
    await decodeValue(node, {
      autobase: {
        isEncrypted: true,
        encryptionKey,
        bootstrap: base.key,
        local: { keyPair: base.local.keyPair }
      },
      key: base.local.key,
      manifest
    }),
    Buffer.from('encrypted')
  )
})

test('decodeValue - w/ block encryption (type 1)', async (t) => {
  const encryptionKey = Buffer.alloc(32).fill('secret')
  const { bases } = await create(1, t, {
    encryptionKey,
    valueEncoding: 'binary',
    manifestVersion: 2
  })
  const [base] = bases

  await base.append('encrypted')

  const node = await getBlock(base.local, 0)

  const manifest = base.local.manifest
  t.alike(
    await decodeValue(node, {
      autobase: {
        isEncrypted: true,
        encryptionKey,
        bootstrap: base.key,
        local: { keyPair: base.local.keyPair },
        encryptionCore: base._viewStore.get({ name: '_encryption' })
      },
      key: base.local.key,
      manifest
    }),
    Buffer.from('encrypted')
  )
})

function getBlock(core, index) {
  const batch = core.core.storage.read()
  const b = batch.getBlock(index)
  batch.tryFlush()
  return b
}
