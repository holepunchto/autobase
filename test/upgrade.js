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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
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

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
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

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
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

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
    apply: applyv1,
    open,
    valueEncoding: 'json'
  })

  await a1.ready()

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
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

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
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

  const version = a0.maxSupportedVersion

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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  a1.maxSupportedVersion = version + 1

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

  const version = a0.maxSupportedVersion

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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  a1.maxSupportedVersion = version + 1

  await a1.ready()

  t.is(a1.view.indexedLength, 3)

  await a1.append({ data: '3' })

  const error = new Promise((resolve, reject) => b0.on('error', reject))

  replicateAndSync([a1, b0])

  await t.exception(error, /Autobase upgrade required/)

  t.is(a1.view.indexedLength, 4)
  t.is(b0.view.indexedLength, 3)

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
    apply,
    open: store => store.get('view', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  b1.maxSupportedVersion = version + 1

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

  const version = a0.maxSupportedVersion

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

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  a1.maxSupportedVersion = version + 1

  await a1.ready()

  t.is(a1.view.indexedLength, 3)

  await a1.append({ data: '3' })

  await confirm([a1, b0])

  t.is(a1.view.indexedLength, 4)
  t.is(b0.view.indexedLength, 4)

  t.is(a1.system.version, version)
  t.is(b0.system.version, version)

  t.is(b0.view.indexedLength, 4) // should not advance

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  b1.maxSupportedVersion = version + 1

  await b1.ready()

  await confirm([a1, b1])

  t.is(a1.system.version, version + 1)
  t.is(b1.system.version, version + 1)
})

test('autobase upgrade - consensus 3 writers', async t => {
  const [s1, s2, s3] = await createStores(3, t)

  const a0 = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await a0.ready()

  const version = a0.maxSupportedVersion

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

  a1.maxSupportedVersion = version + 1
  c1.maxSupportedVersion = version + 1

  await a1.ready()

  t.is(a1.view.indexedLength, 3)

  await a1.append({ data: '5' })
  await c1.append({ data: '6' })

  const berror = new Promise((resolve, reject) => b0.once('error', reject))

  confirm([a1, b0, c1])

  await t.exception(berror, /Autobase upgrade required/)

  t.is((await b0.system.getIndexedInfo()).version, version)
  t.ok(b0.closed)

  t.is(b0.view.indexedLength, 3) // should not advance

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  b1.maxSupportedVersion = version + 1

  await b1.ready()
  await b1.update()

  t.is(b1.view.length, 5) // can update
  t.is(b1.view.signedLength, a1.view.signedLength)

  await confirm([a1, b1])

  t.is(b1.view.signedLength, 5) // majority can continue
  t.is((await b1.system.getIndexedInfo()).version, version + 1)
})

test('autobase upgrade - downgrade', async t => {
  const [s1] = await createStores(1, t)

  const a0 = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  const version = a0.maxSupportedVersion

  await a0.ready()

  await a0.append({ data: 'version 0' })

  t.is(a0.view.indexedLength, 1)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  a1.maxSupportedVersion = version + 1

  await a1.ready()

  t.is(a1.view.indexedLength, 1)

  await a1.append({ data: 'version 1' })

  t.is(a1.view.indexedLength, 2)

  t.is(a1.version, version + 1)
  t.is((await a1.system.getIndexedInfo()).version, version + 1)

  await a1.close()

  t.not(await a1.local.getUserData('autobase/boot'), null)

  // go back to previous version
  const fail = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await t.exception(fail.ready())

  t.is(await fail.local.getUserData('autobase/boot'), null)
})

test('autobase upgrade - downgrade then restart', async t => {
  const [s1] = await createStores(1, t)

  const a0 = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  const version = a0.maxSupportedVersion

  await a0.ready()

  await a0.append({ data: 'version 0' })

  t.is(a0.view.indexedLength, 1)

  await a0.close()

  const a1 = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  a1.maxSupportedVersion = version + 1

  await a1.ready()

  t.is(a1.view.indexedLength, 1)

  await a1.append({ data: 'version 1' })

  t.is(a1.view.indexedLength, 2)

  t.is((await a1.system.getIndexedInfo()).version, version + 1)

  await a1.close()

  t.not(await a1.local.getUserData('autobase/boot'), null)

  // go back to previous version
  const fail = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await t.exception(fail.ready())

  t.is(await fail.local.getUserData('autobase/boot'), null)

  // go back to previous version
  const failAgain = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // ready passes since we unset pointer
  await t.execution(failAgain.ready())

  const updateFail = new Promise((resolve, reject) => {
    failAgain.on('error', reject)
    failAgain.update().then(resolve, reject)
  })

  // update should fail as we get to version upgrade
  await t.exception(updateFail)

  // go back to previous version
  const succeed = new Autobase(s1.session(), a0.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  succeed.maxSupportedVersion = version + 1

  await t.execution(succeed.ready())

  const update = new Promise((resolve, reject) => {
    succeed.on('error', reject)
    succeed.update().then(resolve, reject)
  })

  // update should succeed now
  await t.execution(update)
})

test('autobase upgrade - upgrade before writer joins', async t => {
  const [s1, s2] = await createStores(2, t)

  const opts = {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  }

  const a = new Autobase(s1.session(), null, opts)
  a.maxSupportedVersion++

  await a.ready()
  await a.append('zero')

  t.is(a.version, a.maxSupportedVersion)

  const b = new Autobase(s2.session(), a.bootstrap, opts)
  await b.ready()

  const fail = new Promise((resolve, reject) => {
    b.on('error', reject)
    replicateAndSync([a, b]).then(resolve, reject)
  })

  t.is((await b.system.getIndexedInfo()).version, -1)

  await t.exception(fail, /Autobase upgrade required/)
})

test('autobase upgrade - fix borked version', async t => {
  const [s1, s2] = await createStores(2, t)

  const opts = {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  }

  const a0 = new Autobase(s1.session(), null, opts)
  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, opts)
  await b0.ready()

  const version = a0.maxSupportedVersion

  await addWriterAndSync(a0, b0)

  await confirm([a0, b0])

  await a0.append('zero')

  await confirm([a0, b0])

  t.is(a0.view.indexedLength, 1)
  t.is(b0.view.indexedLength, 1)

  await a0.close()

  // borked version
  opts.apply = applyHalts

  const a1 = new Autobase(s1.session(), a0.bootstrap, opts)

  // simulate version upgrade
  a1.maxSupportedVersion = version + 1

  await a1.ready()

  await confirm([a1, b0])

  t.is(a1.system.version, version)
  t.is(b0.system.version, version)

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.bootstrap, opts)

  // simulate version upgrade
  b1.maxSupportedVersion = version + 1

  await b1.ready()

  await confirm([a1, b1])

  t.is(a1.system.version, version + 1)
  t.is(b1.system.version, version + 1)

  await a1.append('one')
  await b1.append('two')

  await replicateAndSync([a1, b1])

  t.is(a1.view.length, 3)
  t.is(b1.view.length, 3)

  const aerr = new Promise((resolve, reject) => a1.once('error', reject))
  const berr = new Promise((resolve, reject) => b1.once('error', reject))

  a1.append('three', /Block/)
  b1.append('three', /Block/)

  await t.exception(aerr)
  await t.exception(berr)

  await a1.close()
  await b1.close()

  // unbork apply
  opts.apply = apply

  const a2 = new Autobase(s1.session(), a0.bootstrap, opts)
  const b2 = new Autobase(s2.session(), a0.bootstrap, opts)

  // can go forward
  a2.maxSupportedVersion = version + 2
  b2.maxSupportedVersion = version + 2

  await t.execution(a2.ready())
  await t.execution(b2.ready())

  await t.execution(a2.append('three'))
  await t.execution(b2.append('four'))

  async function applyHalts (nodes, view) {
    for (const node of nodes) {
      if (view.length >= 3) throw new Error('Block')
      await view.append(node.value)
    }
  }
})

test('autobase upgrade - downgrade then fix bork', async t => {
  const [s1, s2] = await createStores(2, t)

  const opts = {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  }

  const a0 = new Autobase(s1.session(), null, opts)
  await a0.ready()

  const b0 = new Autobase(s2.session(), a0.bootstrap, opts)
  await b0.ready()

  const version = a0.maxSupportedVersion

  await addWriterAndSync(a0, b0)

  await confirm([a0, b0])

  await a0.append('zero')

  await confirm([a0, b0])

  t.is(a0.view.indexedLength, 1)
  t.is(b0.view.indexedLength, 1)

  await a0.close()

  // borked version
  opts.apply = applyHalts

  const a1 = new Autobase(s1.session(), a0.bootstrap, opts)

  // simulate version upgrade
  a1.maxSupportedVersion = version + 1

  await a1.ready()

  await confirm([a1, b0])

  t.is(a1.system.version, version)
  t.is(b0.system.version, version)

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.bootstrap, opts)

  // simulate version upgrade
  b1.maxSupportedVersion = version + 1

  await b1.ready()

  await confirm([a1, b1])

  t.is(a1.system.version, version + 1)
  t.is(b1.system.version, version + 1)

  await a1.append('one')
  await b1.append('two')

  await replicateAndSync([a1, b1])

  t.is(a1.view.length, 3)
  t.is(b1.view.length, 3)

  const aerr = new Promise((resolve, reject) => a1.once('error', reject))
  const berr = new Promise((resolve, reject) => b1.once('error', reject))

  a1.append('three', /Block/)
  b1.append('three', /Block/)

  await t.exception(aerr)
  await t.exception(berr)

  await a1.close()
  await b1.close()

  // unbork apply
  opts.apply = apply

  // downgrade to version 0
  const fail = new Autobase(s1.session(), a0.bootstrap, opts)

  await t.exception(fail.ready())
  t.is(await fail.local.getUserData('autobase/boot'), null)

  const a2 = new Autobase(s1.session(), a0.bootstrap, opts)
  const b2 = new Autobase(s2.session(), a0.bootstrap, opts)

  // can go forward
  a2.maxSupportedVersion = version + 2
  b2.maxSupportedVersion = version + 2

  await t.execution(a2.ready())
  await t.execution(b2.ready())

  await t.execution(a2.append('three'))
  await t.execution(b2.append('four'))

  async function applyHalts (nodes, view) {
    for (const node of nodes) {
      if (view.length >= 3) throw new Error('Block')
      await view.append(node.value)
    }
  }
})

test('autobase upgrade - 3 writers always increasing', async t => {
  const [s1, s2, s3] = await createStores(3, t)

  const opts = {
    apply,
    ackInterval: 0,
    ackThreshold: 0,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  }

  const a0 = new Autobase(s1.session(), null, opts)
  await a0.ready()

  const version = a0.maxSupportedVersion

  const b0 = new Autobase(s2.session(), a0.bootstrap, opts)
  const c0 = new Autobase(s3.session(), a0.bootstrap, opts)

  await b0.ready()
  await c0.ready()

  await addWriterAndSync(a0, b0)
  await addWriterAndSync(a0, c0)

  await confirm([a0, b0, c0])

  await a0.append('v0')

  await confirm([a0, b0, c0])

  t.is(a0.view.indexedLength, 1)
  t.is(b0.view.indexedLength, 1)

  await a0.close()
  await c0.close()

  const a1 = new Autobase(s1.session(), a0.bootstrap, opts)
  const c1 = new Autobase(s3.session(), a0.bootstrap, opts)

  a1.maxSupportedVersion = version + 1
  c1.maxSupportedVersion = version + 2

  await a1.ready()
  await c1.ready()

  t.is(a1.view.indexedLength, 1)

  await a1.append('v1')
  await replicateAndSync([a1, c1])

  await c1.append('v2')
  await replicateAndSync([a1, c1])

  await a1.close()
  await c1.close()

  const a2 = new Autobase(s1.session(), a0.bootstrap, opts)
  const c2 = new Autobase(s3.session(), a0.bootstrap, opts)

  a2.maxSupportedVersion = version + 3
  c2.maxSupportedVersion = version + 4

  await a2.ready()
  await c2.ready()
  await replicateAndSync([a2, c2])

  await a2.append('v3')
  await replicateAndSync([a2, c2])

  await c2.append('v4')
  await replicateAndSync([a2, c2])

  t.is(c2.version, version + 1)

  await a2.append('flush')
  await replicateAndSync([a2, c2])

  t.is(c2.version, version + 2)

  await t.exception(new Promise((resolve, reject) => {
    b0.on('error', reject)
    replicateAndSync([a2, c2, b0]).then(resolve, reject)
  }))

  t.is((await b0.system.getIndexedInfo()).version, version)
  t.ok(b0.closing)

  await b0.close()

  const b1 = new Autobase(s2.session(), a0.bootstrap, opts)
  b1.maxSupportedVersion = version + 1

  await b1.ready()

  await t.exception(new Promise((resolve, reject) => {
    b1.on('error', reject)
    replicateAndSync([a2, b1]).then(resolve, reject)
  }))

  t.is((await b1.system.getIndexedInfo()).version, version + 1)
  t.ok(b1.closing)

  const b2 = new Autobase(s2.session(), a0.bootstrap, opts)
  b2.maxSupportedVersion = a2.maxSupportedVersion

  await b2.ready()
  await t.execution(replicateAndSync([a2, b2]))

  t.not(b2.view.signedLength, 6)

  await confirm([a2, b2])
  t.is(a2.version, b2.version)

  t.is(b2.view.signedLength, 6) // majority can continue
  t.is((await b2.system.getIndexedInfo()).version, b2.version)
})

test('autobase upgrade - non monotonic version', async t => {
  const [s1, s2] = await createStores(2, t)

  const a = new Autobase(s1.session(), null, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await a.ready()

  const version = a.maxSupportedVersion

  const b = new Autobase(s2.session(), a.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await b.ready()

  await addWriterAndSync(a, b)
  await confirm([a, b])

  await a.append('0')
  await confirm([a, b])

  await a.close()
  const a1 = new Autobase(s1.session(), a.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  // simulate version upgrade
  a1.maxSupportedVersion = version + 1

  await a1.ready()

  await a1.append('2')
  await confirm([a1, b])

  t.is(a1.view.indexedLength, 2)
  t.is(b.view.indexedLength, 2)

  t.is(a1.system.version, version)
  t.is(b.system.version, version)

  await a1.close()

  const a2 = new Autobase(s1.session(), a.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  await t.exception(a2.ready())
  await t.exception(a2.append('3'))

  // can recover
  const a3 = new Autobase(s1.session(), a.bootstrap, {
    apply,
    open: store => store.get('test', { valueEncoding: 'json' }),
    valueEncoding: 'json'
  })

  a3.maxSupportedVersion = version + 1

  await a3.ready()
  await a3.append('3')

  await confirm([a3, b])

  t.is(a3.view.indexedLength, 3)
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
