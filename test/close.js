const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')

const Autobase = require('..')

const { apply, open, create, addWriterAndSync, encryptionKey } = require('./helpers')

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

test('wakeup does not install a preupdate hook after closing', async function (t) {
  const { bases } = await create(2, t, { fastForward: false })
  const [a, b] = bases

  await addWriterAndSync(a, b)

  const core = [...b.store.cores].find((c) => c.opened && b4a.equals(c.key, a.local.key))
  t.ok(core, 'found a remote writer core')

  const wakeup = b._wakeup
  wakeup.remove(core)
  t.is(core.preupdate, null)

  // close while the add is still reading the core's user data
  const adding = wakeup.add(core)
  await wakeup.close()
  await adding

  t.is(core.preupdate, null, 'closed wakeup left no hook on the core')
})
