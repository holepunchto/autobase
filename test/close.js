const test = require('brittle')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')

const Autobase = require('..')

const { apply, open, encryptionKey } = require('./helpers')

// the local core is closed early in the teardown, which used to drop its
// exclusive lock while the rest of the instance was still closing
test('local core lock is held until the store session is torn down', async function (t) {
  const store = new Corestore(await tmpDir(t), { encryptionKey, unsafe: true })
  t.teardown(() => store.close())

  const base = new Autobase(store.session(), null, {
    apply,
    open,
    valueEncoding: 'json',
    ackInterval: 0,
    ackThreshold: 0,
    encryptionKey,
    fastForward: false
  })
  await base.ready()
  await base.append('a')

  // contend for the lock the way a second instance on the same store would
  const local = store.get({ key: base.local.key, exclusive: true })
  const locked = local.ready().then(() => base.store.closed)
  t.teardown(() => local.close())

  await base.close()

  t.is(await locked, true, 'lock granted only after the store session was torn down')
})
