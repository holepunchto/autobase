// Generate suspend fixtures

const fs = require('fs/promises')
const path = require('path')
const Corestore = require('corestore')
const b4a = require('b4a')

const { version } = require('../../../package.json')

const {
  create,
  createBase,
  addWriterAndSync,
  replicateAndSync,
  confirm
} = require('../../helpers')

main().catch(console.error)

async function main () {
  const closing = []
  const t = { teardown }

  const { bases } = await create(1, t, {
    apply: applyMultiple,
    open: openMultiple,
    encryptionKey: b4a.alloc(32).fill('secret')
  })

  const [a] = bases

  const testPath = path.resolve(__dirname, '..', `tests/suspend-v${version}.js`)
  const fixturePath = path.join(__dirname, '..', `data/suspend/corestore-v${version}`)

  const bstore = new Corestore(path.join(fixturePath, 'b'))
  const cstore = new Corestore(path.join(fixturePath, 'c'))

  const b = await createBase(bstore.session(), a.local.key, t, {
    apply: applyMultiple,
    open: openMultiple,
    encryptionKey: b4a.alloc(32).fill('secret')
  })

  const c = await createBase(cstore.session(), a.local.key, t, {
    apply: applyMultiple,
    open: openMultiple,
    encryptionKey: b4a.alloc(32).fill('secret')
  })

  await b.ready()
  await c.ready()

  await addWriterAndSync(a, b, false)
  await addWriterAndSync(a, c, false)

  await replicateAndSync([a, b, c])

  let n = 0

  // writer has indexed nodes
  for (let i = 0; i < 100; i++) await c.append({ index: (i % 2) + 1, data: 'c' + n++ })

  await confirm([a, b, c])

  // create cross linked tip bigger than autobase max batch size
  for (let i = 0; i < 40; i++) await c.append({ index: (i % 2) + 1, data: 'c' + n++ })
  for (let i = 0; i < 40; i++) await b.append({ index: (i % 2) + 1, data: 'b' + n++ })
  await replicateAndSync([b, c])
  for (let i = 0; i < 40; i++) await c.append({ index: (i % 2) + 1, data: 'c' + n++ })
  await replicateAndSync([b, c])
  for (let i = 0; i < 40; i++) await b.append({ index: (i % 2) + 1, data: 'b' + n++ })
  await replicateAndSync([b, c])

  const exp = { key: b4a.toString(b.local.key, 'hex'), length: b.local.length }

  for (let i = 0; i < 40; i++) await b.append({ index: 2, data: 'b' + n++ })

  await c.close()
  await b.close()

  await bstore.close()
  await cstore.close()

  await shutdown

  await fs.writeFile(testPath, generate(version, n, exp))

  console.log('Test was written to:', testPath)

  function teardown (fn) {
    closing.push(fn)
  }

  function shutdown () {
    return Promise.all(closing.map(fn => fn()))
  }
}

function generate (version, n, exp) {
  return `const fs = require('fs/promises')
const path = require('path')
const Corestore = require('corestore')
const test = require('brittle')
const tmpDir = require('test-tmp')
const b4a = require('b4a')

const { createBase, replicateAndSync } = require('../../helpers')

test('suspend - restart from v${version} fixture', async t => {
  const fixturePath = path.join(__dirname, '../data/suspend/corestore-v${version}')

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
    key: b4a.from('${exp.key}', 'hex'),
    length: ${exp.length}
  }

  await c.append({ index: 1, data: 'c' + ${n} })

  const last = await c.local.get(c.local.length - 1)
  t.alike(last.node.heads, [exp])

  await replicateAndSync([b, c])

  t.is(await c.view.first.get(c.view.first.length - 1), 'c' + ${n})
  t.is(await c.view.second.get(c.view.second.length - 1), 'b' + ${n - 1})
})

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
`
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
