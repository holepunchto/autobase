const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')

const Autobase = require('..')

const {
  apply,
  createStores,
  replicateAndSync,
  addWriterAndSync,
  confirm
} = require('./helpers')

test('upgrade - do not proceed', async t => {
  const [s1, s2] = await createStores(2, t)

  const a0 = new Autobase(s1.session(), null, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await b0.ready()

  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })

  await replicateAndSync([a0, b0])

  t.is(a0.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await a1.ready()

  t.is(a1.view.data.indexedLength, 3)

  await a1.append({ version: 1, data: '3' })

  const error = new Promise((resolve, reject) => b0.on('error', reject))

  replicateAndSync([a1, b0])

  await t.exception(error, /Upgrade required/)

  t.is(a1.view.data.indexedLength, 4)
  t.is(b0.view.data.indexedLength, 3)
})

test('upgrade - proceed', async t => {
  const [s1, s2] = await createStores(2, t)

  const a0 = new Autobase(s1.session(), null, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await b0.ready()

  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })

  await replicateAndSync([a0, b0])

  t.is(a0.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await a1.ready()

  t.is(a1.view.data.indexedLength, 3)

  await a1.append({ version: 1, data: '3' })

  const error = new Promise((resolve, reject) => b0.on('error', reject))

  replicateAndSync([a1, b0])

  await t.exception(error, /Upgrade required/)

  t.is(a1.view.data.indexedLength, 4)
  t.is(b0.view.data.indexedLength, 3)

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await b1.ready()
  await b1.update()

  t.is(b1.view.data.indexedLength, 4)
})

test('upgrade - consensus', async t => {
  const [s1, s2] = await createStores(2, t)

  const a0 = new Autobase(s1.session(), null, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await b0.ready()

  await addWriterAndSync(a0, b0)

  await confirm([a0, b0])

  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })

  await confirm([a0, b0])

  t.is(a0.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await a1.ready()

  t.is(a1.view.data.indexedLength, 3)

  await a1.append({ version: 1, data: '3' })

  const error = new Promise((resolve, reject) => b0.on('error', reject))

  replicateAndSync([a1, b0])

  t.is(a1.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)

  t.is(a1.view.data.length, 4)
  t.is(b0.view.data.length, 3)

  await t.exception(error, /Upgrade required/)

  t.is(b0.view.data.indexedLength, 3) // should not advance

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await b1.ready()

  await confirm([a1, b1])

  t.is(b1.view.data.indexedLength, 4)
})

test('upgrade - consensus 3 writers', async t => {
  const [s1, s2, s3] = await createStores(3, t)

  const a0 = new Autobase(s1.session(), null, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await b0.ready()

  const c0 = new Autobase(s3.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await c0.ready()

  await addWriterAndSync(a0, b0)
  await addWriterAndSync(a0, c0)

  await confirm([a0, b0, c0])

  await a0.append({ version: 0, data: '1' })
  await a0.append({ version: 0, data: '2' })
  await a0.append({ version: 0, data: '3' })

  await confirm([a0, b0, c0])

  t.is(a0.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await a1.ready()

  t.is(a1.view.data.indexedLength, 3)

  await b0.append({ version: 0, data: '4' })
  await replicateAndSync([a1, b0])

  const berror = new Promise((resolve, reject) => b0.once('error', reject))
  const cerror = new Promise((resolve, reject) => c0.once('error', reject))

  await a1.append({ version: 1, data: '5' })
  await replicateAndSync([a1, b0, c0])

  t.is(a1.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)
  t.is(c0.view.data.indexedLength, 3)

  await b0.append(null)
  await replicateAndSync([b0, c0])
  await c0.append(null)
  await replicateAndSync([b0, c0])
  await b0.append(null)
  await replicateAndSync([a1, b0, c0])

  t.is(a1.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)
  t.is(c0.view.data.indexedLength, 3)

  await c0.append(null)
  await replicateAndSync([a1, b0, c0])
  await b0.append(null)
  await replicateAndSync([a1, b0, c0])

  t.is(await b0.view.version.get(b0.view.version.indexedLength - 1), 0)

  await a1.append(null)
  replicateAndSync([a1, b0, c0])

  await t.exception(berror, /Upgrade required/)
  await t.exception(cerror, /Upgrade required/)

  t.is(b0.view.data.indexedLength, 3) // should not advance
  t.is(b0.view.data.length, 4)

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await b1.ready()
  await b1.update()

  t.is(b1.view.data.length, 5) // can update
  t.is(b1.view.data.signedLength, 3)

  await confirm([a1, b1])

  t.is(b1.view.data.signedLength, 5) // majority can continue
  t.is(await b1.view.version.get(b1.view.version.indexedLength - 1), 1)
})

test('upgrade - writer cannot append while behind', async t => {
  const [s1, s2, s3] = await createStores(3, t)

  const a0 = new Autobase(s1.session(), null, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await b0.ready()

  const c0 = new Autobase(s3.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await c0.ready()

  await addWriterAndSync(a0, b0)
  await addWriterAndSync(a0, c0)

  await confirm([a0, b0, c0])

  await a0.append({ version: 0, data: '1' })
  await a0.append({ version: 0, data: '2' })
  await a0.append({ version: 0, data: '3' })

  await confirm([a0, b0, c0])

  t.is(a0.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await a1.ready()

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await b1.ready()

  await a1.append({ version: 1, data: '4' })
  await confirm([a1, b1], 3)

  t.is(a1.view.data.indexedLength, 4)
  t.is(b1.view.data.indexedLength, 4)

  const error = new Promise((resolve, reject) => c0.on('error', reject))

  replicateAndSync([a1, c0])

  await t.exception(error, /Upgrade required/)

  const len = c0.local.length
  await c0.append({ version: 0, data: '5' })

  t.is(c0.local.length, len) // did not append
  t.is(c0.view.data.indexedLength, 3)

  await b1.append({ version: 1, data: '5' })
  await confirm([a1, b1], 3)

  // majority can continue
  t.is(a1.view.data.indexedLength, 5)
  t.is(b1.view.data.indexedLength, 5)
})

test('upgrade - onindex hook', async t => {
  const [s1, s2] = await createStores(2, t)

  let aversion = 0
  let bversion = 0

  const a0 = new Autobase(s1.session(), null, {
    apply: applyv0,
    open,
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply: applyv0,
    open,
    onindex: async () => {
      const view = b0.view.version
      if (!view.indexedLength) return
      bversion = await view.get(view.indexedLength - 1)
    },
    valueEncoding: 'json'
  })

  await b0.ready()

  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })
  await a0.append({ version: 0, data: '3' })

  await replicateAndSync([a0, b0])

  t.is(a0.view.data.indexedLength, 3)
  t.is(b0.view.data.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply: applyv1,
    open,
    onindex: async () => {
      const view = a1.view.version
      if (!view.indexedLength) return
      aversion = await view.get(view.indexedLength - 1)
    },
    valueEncoding: 'json'
  })

  await a1.ready()

  t.is(a1.view.data.indexedLength, 3)

  await a1.append({ version: 1, data: '3' })

  const error = new Promise((resolve, reject) => b0.on('error', reject))

  replicateAndSync([a1, b0])

  await t.exception(error, /Upgrade required/)

  t.is(a1.view.data.indexedLength, 4)
  t.is(b0.view.data.indexedLength, 3)

  t.is(aversion, 1)
  t.is(bversion, 0) // closed before onindex is called

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply: applyv1,
    open,
    onindex: async () => {
      const view = b1.view.version
      if (!view.indexedLength) return
      bversion = await view.get(view.indexedLength - 1)
    },
    valueEncoding: 'json'
  })

  await b1.ready()
  await b1.update()

  t.is(bversion, 1) // closed before onindex is called
})

test('autobase upgrade - do not proceed', async t => {
  const [s1, s2] = await createStores(2, t)

  const a0 = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await b0.ready()

  await a0.append({ data: '3' })
  await a0.append({ data: '3' })
  await a0.append({ data: '3' })

  await replicateAndSync([a0, b0])

  t.is(a0.view.indexedLength, 3)
  t.is(b0.view.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  a1.maxSupportedVersion++

  await a1.ready()

  t.is(a1.view.indexedLength, 3)

  await a1.append({ data: '3' })

  const error = new Promise((resolve, reject) => b0.on('error', reject))

  replicateAndSync([a1, b0])

  await t.exception(error, /Autobase upgrade required/)

  t.is(a1.view.indexedLength, 4)
  t.is(b0.view.indexedLength, 3)
})

test('autobase upgrade - proceed', async t => {
  const [s1, s2] = await createStores(2, t)

  const a0 = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await b0.ready()

  await a0.append({ data: '3' })
  await a0.append({ data: '3' })
  await a0.append({ data: '3' })

  await replicateAndSync([a0, b0])

  t.is(a0.view.indexedLength, 3)
  t.is(b0.view.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  a1.maxSupportedVersion++

  await a1.ready()

  t.is(a1.view.indexedLength, 3)

  await a1.append({ data: '3' })

  const error = new Promise((resolve, reject) => b0.on('error', reject))

  replicateAndSync([a1, b0])

  await t.exception(error, /Autobase upgrade required/)

  t.is(a1.view.indexedLength, 4)
  t.is(b0.view.indexedLength, 3)

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  b1.maxSupportedVersion++

  await b1.ready()
  await b1.update()

  t.is(b1.view.indexedLength, 4)
})

test('autobase upgrade - consensus', async t => {
  const [s1, s2] = await createStores(2, t)

  const a0 = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await b0.ready()

  await addWriterAndSync(a0, b0)

  await confirm([a0, b0])

  await a0.append({ data: '3' })
  await a0.append({ data: '3' })
  await a0.append({ data: '3' })

  await confirm([a0, b0])

  t.is(a0.view.indexedLength, 3)
  t.is(b0.view.indexedLength, 3)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.local.key, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  a1.maxSupportedVersion++

  await a1.ready()

  t.is(a1.view.indexedLength, 3)

  await a1.append({ data: '3' })

  await confirm([a1, b0])

  t.is(a1.view.indexedLength, 4)
  t.is(b0.view.indexedLength, 4)

  t.is(a1.system.version, 0)
  t.is(b0.system.version, 0)

  t.is(b0.view.indexedLength, 4) // should not advance

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  b1.maxSupportedVersion++

  await b1.ready()

  await confirm([a1, b1])

  t.is(a1.system.version, 1)
  t.is(b1.system.version, 1)
})

test('autobase upgrade - consensus 3 writers', async t => {
  const [s1, s2, s3] = await createStores(3, t)

  const a0 = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await b0.ready()

  const c0 = new Autobase(s3.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await c0.ready()

  await addWriterAndSync(a0, b0)
  await addWriterAndSync(a0, c0)

  await confirm([a0, b0, c0])

  await a0.append({ data: '1' })
  await a0.append({ data: '2' })
  await a0.append({ data: '3' })

  await confirm([a0, b0, c0])

  t.is(a0.view.indexedLength, 3)
  t.is(b0.view.indexedLength, 3)

  await a0.close()
  await c0.close()

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  const c1 = new Autobase(s3.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  a1.maxSupportedVersion++
  c1.maxSupportedVersion++

  await a1.ready()

  t.is(a1.view.indexedLength, 3)

  await a1.append({ data: '5' })
  await c1.append({ data: '6' })

  const berror = new Promise((resolve, reject) => b0.once('error', reject))

  confirm([a1, b0, c1])

  await t.exception(berror, /Autobase upgrade required/)

  t.is((await b0.system.getIndexedInfo()).version, 0)
  t.ok(b0.closed)

  t.is(b0.view.indexedLength, 3) // should not advance

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.local.key, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  b1.maxSupportedVersion++

  await b1.ready()
  await b1.update()

  t.is(b1.view.length, 5) // can update
  t.is(b1.view.signedLength, a1.view.signedLength)

  await confirm([a1, b1])

  t.is(b1.view.signedLength, 5) // majority can continue
  t.is((await b1.system.getIndexedInfo()).version, b1.version)
})

function open (store) {
  return {
    data: store.get('data', { valueEncoding: 'json' }),
    version: store.get('version', { valueEncoding: c.uint })
  }
}

async function applyv0 (batch, view, base) {
  for (const { value } of batch) {
    await view.version.append(value.version)

    if (value.version > 0) {
      throw new Error('Upgrade required')
    }

    if (value.add) {
      await base.addWriter(b4a.from(value.add, 'hex'), { indexer: value.indexer })
      continue
    }

    await view.data.append({ version: 'v0', data: value.data })
  }
}

async function applyv1 (batch, view, base) {
  for (const { value } of batch) {
    await view.version.append(value.version)

    if (value.version > 1) {
      throw new Error('Upgrade required')
    }

    if (value.add) {
      await base.addWriter(b4a.from(value.add, 'hex'), { indexer: value.indexer })
      continue
    }

    await view.data.append({ version: 'v1', data: value.data })
  }
}
