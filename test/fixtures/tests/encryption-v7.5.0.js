const Corestore = require('corestore')
const test = require('brittle')
const tmpDir = require('test-tmp')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')

const Autobase = require('../../..')

const fixture = require('../data/encryption/v7.5.0.json')

test('encryption - v7.5.0', async t => {
  const keyPair = crypto.keyPair(b4a.alloc(32, 1))
  const storage = await tmpDir()
  const store = new Corestore(storage)

  const base = new Autobase(store, {
    keyPair,
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey: b4a.alloc(32).fill('secret')
  })

  await base.append('encrypted data')
  await base.append('that should be')
  await base.append('determinstically')
  await base.append('encrypted')

  // oplog comparison disabled
  // t.comment('local')
  // await compareRawBlocks(base.local, fixture.local)

  t.comment('system')
  await compareRawBlocks(base.core, fixture.system)

  t.comment('view')
  await compareRawBlocks(base.view, fixture.view)

  await base.close()
  await store.close()

  async function compareRawBlocks (core, fixture) {
    t.is(core.length, fixture.length)
    for (let i = 0; i < core.length; i++) {
      const block = await core.get(i, { raw: true })
      t.is(b4a.toString(block, 'hex'), fixture[i], 'index ' + i)
    }
  }
})

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply (batch, view, base) {
  for (const { value } of batch) {
    await view.append(value.toString())
  }
}
