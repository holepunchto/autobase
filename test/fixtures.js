const fs = require('fs/promises')
const path = require('path')
const Corestore = require('corestore')
const test = require('brittle')
const tmpDir = require('test-tmp')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')

const Autobase = require('..')

const fixture = require('./fixtures/encryption.js')

const { createBase, replicateAndSync } = require('./helpers')

test('encryption - fixture', async t => {
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

  t.comment('system')
  await compareFixture(base.core, fixture.system)

  t.comment('view')
  await compareFixture(base.view, fixture.view)

  const closing = base.close()
  await store.close()

  await closing

  async function compareFixture (core, fixture) {
    t.is(core.length, fixture.length)
    for (let i = 0; i < core.length; i++) {
      const block = await core.get(i, { raw: true })
      t.is(b4a.toString(block, 'hex'), fixture[i], 'index ' + i)
    }
  }
})

test('suspend - restart from v7.5.0 fixture', async t => {
  const fixturePath = path.join(__dirname, './fixtures/suspend/stores/v7.5.0')

  const bdir = await tmpDir(t)
  const cdir = await tmpDir(t)

  await fs.cp(path.join(fixturePath, 'b'), bdir, { recursive: true })
  await fs.cp(path.join(fixturePath, 'c'), cdir, { recursive: true })

  const bstore = new Corestore(bdir, { allowBackup: true })
  const cstore = new Corestore(cdir, { allowBackup: true })

  const b = await createBase(bstore.session(), null, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  const c = await createBase(cstore.session(), null, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  await b.ready()
  await c.ready()

  // invariant
  const exp = {
    key: b4a.from('365fca8bf0e9e567d74efd5c28c4acb912dbbd227e0b9855882fa79ed94880ec', 'hex'),
    length: 83
  }

  await c.append({ index: 1, data: 'c' + 300 })

  const last = await c.local.get(c.local.length - 1)
  t.alike(last.node.heads, [exp])

  await replicateAndSync([b, c])

  t.is(await c.view.first.get(c.view.first.length - 1), 'c' + 300)
  t.is(await c.view.second.get(c.view.second.length - 1), 'b' + 299)
})

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply (batch, view, base) {
  for (const { value } of batch) {
    await view.append(value.toString())
  }
}

function openMultiple (store) {
  return {
    first: store.get('first', { valueEncoding: 'json' }),
    second: store.get('second', { valueEncoding: 'json' })
  }
}

async function applyMultiple (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'))
      continue
    }

    if (value.index === 1) {
      await view.first.append(value.data)
    } else if (value.index === 2) {
      await view.second.append(value.data)
    }
  }
}
