const test = require('brittle')
const tmpDir = require('test-tmp')
const b4a = require('b4a')
const assert = require('nanoassert')

const {
  create,
  createBase,
  addWriter,
  // replicate,
  replicateAndSync,
  confirm,
  open: defaultOpen
  // defaultApply: apply
} = require('./helpers')

test('fork - assertion no recovery', async t => {
  let assertions = 0

  const { bases } = await create(2, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: async (batch, view, host) => {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await host.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (value.assert) {
          const self = b4a.toString(host.self, 'hex')
          if (value.assert === self) {
            assert(false, 'assert #' + assertions++)
          }
        }

        if (view) await view.append(value)
      }
    }
  })

  const [a, b] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, true)
  await confirm(bases)

  // unindexed so it is always reapplied
  await a.append({ assert: b4a.toString(b.local.key, 'hex') })

  await t.exception(replicateAndSync([a, b]), 'assert #1')

  t.is(assertions, 2)
  t.is(a.view.length, 4)
  t.is(b.view.length, 3)
})

test('fork - assertion recovery', async t => {
  let assertions = 0

  const { bases } = await create(2, t, {
    encryptionKey: b4a.alloc(32, 0),
    apply: async (batch, view, host) => {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await host.addWriter(key, { indexer: value.indexer })
          continue
        }

        // only assert once
        if (value.assert && assertions === 0) {
          const self = b4a.toString(host.self, 'hex')
          assert(value.assert !== self, 'assert #' + assertions++)
        }

        if (view) await view.append(value)
      }
    }
  })

  const [a, b] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.append(null)

  await a.append({ assert: b4a.toString(b.local.key, 'hex') })

  for (let i = 0; i < 200; i++) {
    await a.append('data ' + i)
  }

  await t.execution(replicateAndSync([a, b]))

  t.is(a.view.signedLength, 204)
  t.is(b.view.signedLength, 204)
})

test('fork - recover during boot', async t => {
  t.plan(3)
  const { bases, stores } = await create(2, t, { storage: () => tmpDir(t) })

  const [a, b] = bases

  for (let i = 0; i < 100; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])

  t.is(b.activeWriters.size, 2)
  t.is(b.view.length, 100)

  await b.close()

  let assertions = 0

  const b2 = createBase(stores[1], a.local.key, t, {
    open: (store, host) => {
      if (!host.public && !assertions++) {
        throw new Error('throw once on view boot')
      }
      return defaultOpen(store, host)
    }
  })

  await b2.ready()

  t.is(assertions, 1)
})

test('fork - recovery fails during boot', async t => {
  t.plan(6)

  const { bases, stores } = await create(2, t, { storage: () => tmpDir(t) })

  const [a, b] = bases

  for (let i = 0; i < 100; i++) {
    await a.append('a' + i)
  }

  await replicateAndSync([a, b])

  t.is(b.activeWriters.size, 2)
  t.is(b.view.length, 100)

  await b.close()

  let assertions = 0

  const b2 = createBase(stores[1], a.local.key, t, {
    open: (store, host) => {
      if (!host.public) throw new Error('assert #' + assertions++)
      return defaultOpen(store, host)
    }
  })

  await t.execution(b2.ready())

  const assertion = new Promise((resolve, reject) => {
    process.once('uncaughtException', reject)
  })

  await t.exception(assertion, 'assert #1')
  t.is(assertions, 1)

  await new Promise(setImmediate) // allow tick to close

  t.is(b2.closed, true)
})

test('fork - update hook on recovery', async t => {
  let assertions = 0
  let from = -1

  const { bases } = await create(2, t, {
    encryptionKey: b4a.alloc(32, 0),
    update: (view, changes, host) => {
      const sys = changes.get('_system')
      if (sys.from === from) t.is(sys.to, a.core.signedLength, 'recovery triggered update')
    },
    apply: async (batch, view, host) => {
      for (const { value } of batch) {
        if (value.add) {
          const key = Buffer.from(value.add, 'hex')
          await host.addWriter(key, { indexer: value.indexer })
          continue
        }

        if (value.assert) {
          const self = b4a.toString(host.self, 'hex')

          assert(value.assert !== self && (assertions++ & 1) === 0,
            'assert #' + assertions)
        }

        if (view) await view.append(value)
      }
    }
  })

  const [a, b] = bases

  await a.append('one')
  await a.append('two')
  await a.append('three')

  await addWriter(a, b, false)
  await confirm(bases)

  await b.append(null)

  await a.append({ assert: b4a.toString(b.local.key, 'hex') })

  for (let i = 0; i < 200; i++) {
    await a.append('data ' + i)
  }

  from = b.core.signedLength

  await t.execution(replicateAndSync([a, b]))

  t.is(a.view.signedLength, 204)
  t.is(b.view.signedLength, 204)
})
