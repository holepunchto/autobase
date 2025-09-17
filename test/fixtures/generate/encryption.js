// Generate suspend fixtures

const fs = require('fs/promises')
const path = require('path')
const Corestore = require('corestore')
const crypto = require('hypercore-crypto')
const tmpDir = require('test-tmp')
const b4a = require('b4a')

const Autobase = require('../../../')
const { version } = require('../../../package.json')

main()

const DATA = [
  'encrypted data',
  'that should be',
  'determinstically',
  'encrypted'
]

async function main () {
  const closing = []
  const t = { teardown }

  const testPath = path.resolve(__dirname, '..', `tests/encryption-v${version}.js`)
  const fixturePath = path.join(__dirname, '..', `data/encryption/v${version}.json`)

  const keyPair = crypto.keyPair(b4a.alloc(32, 1))

  const storage = await tmpDir(t)
  const store = new Corestore(storage)

  const base = new Autobase(store, {
    keyPair,
    apply,
    open,
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey: b4a.alloc(32).fill('secret')
  })

  for (const data of DATA) await base.append(data)

  const [local, system, view] = await Promise.all([
    getBlocks(base.local),
    getBlocks(base.core),
    getBlocks(base.view)
  ])
  const fixture = {
    local,
    system,
    view
  }

  await fs.writeFile(fixturePath, JSON.stringify(fixture))

  await base.close()
  await store.close()

  await shutdown()

  await fs.writeFile(testPath, generate(version))

  console.log('Test was written to:', testPath)

  function teardown (fn) {
    closing.push(fn)
  }

  function shutdown () {
    return Promise.all(closing.map(fn => fn()))
  }
}

async function getBlocks (core) {
  const blocks = []
  for (let i = 0; i < core.length; i++) {
    blocks.push(b4a.toString(await core.get(i, { raw: true }), 'hex'))
  }
  return blocks
}

function generate (version, n, exp) {
  return `const Corestore = require('corestore')
const test = require('brittle')
const tmpDir = require('test-tmp')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')

const Autobase = require('../../..')

const fixture = require('../data/encryption/v${version}.json')

test('encryption - v${version}', async t => {
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

${DATA.map(d => `  await base.append('${d}')`).join('\n')}

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
`
}

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply (batch, view, base) {
  for (const { value } of batch) {
    await view.append(value.toString())
  }
}
