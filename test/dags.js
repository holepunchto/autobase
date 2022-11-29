const test = require('brittle')
const b4a = require('b4a')
const Corestore = require('corestore')
const RAM = require('random-access-memory')

const Autobase = require('../index.js')

function tester (store, label, genesis, opts = {}) {
  const base = new Autobase(store, genesis, { apply, open })

  setDebug(!!opts.debug)

  let index = 0
  let last = 0

  return new Proxy(base, {
    get (target, prop) {
      switch (prop) {
        case 'sync':
          return syncTo
        case 'values':
          return values
        case 'append':
          return append
        case 'tails':
          return tails
        case 'setDebug':
          return setDebug
        default:
          return Reflect.get(...arguments)
      }
    }
  })

  function setDebug (debug = true, lbl = label) {
    base.debug = !!opts.debug
    base.linearizer.debug = !!opts.debug

    base.name = lbl
    base.linearizer.name = lbl
  }

  function open (store) {
    return store.get('double')
  }

  async function apply (nodes, view, base) {
    // if (!base.debug) {
    //   console.log('apply for', label)
    // }

    for (const node of nodes) {
      if (node.value.add) {
        base.system.addWriter(b4a.from(node.value.add, 'hex'))
      }

      await view.append(node.value)
    }
  }

  function syncTo (remote, oneway) {
    if (Array.isArray(remote)) return Promise.all(remote.map(b => syncTo(b, oneway)))
    return sync(base, remote, oneway)
  }

  function append () {
    return base.append({
      debug: label + index++
    })
  }

  function tails () {
    return base.linearizer.tails.map(t => t.value)
  }

  async function * values () {
    for (let i = 0; i < base.view.length; i++) {
      yield await base.view.get(i)
    }
  }
}

/*

c - b - a - c - b - a

*/

test.solo('simple 3', async t => {
  const [a, b, c] = await getWriters(3)

  a.setDebug()

  const destroy = replicateMany(a, b, c)

  // --- loop ---

  await c.append()
  await sleep()

  await a.update()
  await b.update()
  await c.update()

  await b.append()
  await sleep()

  await a.update()
  await b.update()
  await c.update()

  await a.append()

  await sleep()

  await a.update()
  await b.update()
  await c.update()

  await c.append()

  await sleep()

  await a.update()
  await b.update()
  await c.update()

  await b.append()

  await sleep()

  await a.update()
  await b.update()
  await c.update()

  await a.append()

  await sleep()

  await a.update()
  await b.update()
  await c.update()

  // --- loop ---

  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  const av = await collect(a.values())
  const bv = await collect(b.values())
  const cv = await collect(c.values())

  t.alike(av, bv)
  t.alike(av, cv)

  t.is(a.linearizer.tails.length, 1)

  destroy()
  t.end()
})

/*

a   b
| / |
b   c
| / |
c   a
| / |
a   b
| / |
b   c
| / |
c   a

*/

test('non-convergence', async t => {
  const [a, b, c] = await getWriters(3)

  // --- loop ---

  await a.append()
  await b.append()

  await c.sync(b, true)
  await b.sync(a, true)

  await b.append()
  await c.append()

  await a.sync(c, true)
  await c.sync(b, true)
  await b.sync(a, true)

  await a.append()
  await c.append()

  await a.sync(c, true)
  await c.sync(b, true)
  await b.sync(a, true)

  await a.append()
  await b.append()

  await c.sync(b, true)
  await b.sync(a, true)

  await b.append()
  await c.append()

  await a.sync(c, true)
  await c.sync(b, true)
  await b.sync(a, true)

  await a.append()
  await c.append()

  // --- loop ---

  t.alike(a.view.indexedLength, b.view.indexedLength)
  t.alike(c.view.indexedLength, b.view.indexedLength)
  t.alike(a.view.indexedLength, c.view.indexedLength)

  const av = await collect(a.values())
  const bv = await collect(b.values())
  const cv = await collect(c.values())

  t.alike(av, bv)
  t.alike(av, cv)

  t.is(a.linearizer.tails.length, 1)

  t.end()
})

/*

    b   c   d
  / | x | x | \
 a  b   c   d  e
  \ | x | x | /
    b   c   d
    | /
    b

*/

test('inner majority', async t => {
  const [a, b, c, d, e] = await getWriters(5)

  // --- write ---

  await b.append()
  await c.append()
  await d.append()

  await a.sync(b, true)
  await e.sync(d, true)
  await b.sync(c, true)
  await d.sync(c, true)
  await c.sync([b, d], true)

  await a.append()
  await e.append()
  await b.append()
  await c.append()
  await d.append()

  await b.sync(c, true)
  await d.sync(c, true)
  await c.sync([b, d], true)

  await b.append()
  await c.append()
  await d.append()

  await b.sync(c, true)

  await b.append()

  const bv = await collect(b.values())

  t.is(bv.indexedLength, 3)
  t.is(b.linearizer.tails.length, 1)

  t.end()
})

/*

  b - c - d - b - c - d

*/

test('majority alone - convergence', async t => {
  const [a, b, c, d, e] = await getWriters(5)

  // --- write ---

  await b.append()

  await b.sync([c, d])
  await c.sync([b, d])
  await b.sync([d, c])

  await c.append()

  await b.sync([c, d])
  await c.sync([b, d])
  await b.sync([d, c])

  await d.append()

  await b.sync([c, d])
  await c.sync([b, d])
  await b.sync([d, c])

  await b.append()

  await b.sync([c, d])
  await c.sync([b, d])
  await b.sync([d, c])

  await c.append()

  await b.sync([c, d])
  await c.sync([b, d])
  await b.sync([d, c])

  await d.append()

  await b.sync([c, d])
  await c.sync([b, d])
  await b.sync([d, c])

  const bv = await collect(b.values())
  const cv = await collect(c.values())
  const dv = await collect(d.values())

  t.alike(bv, cv)
  t.alike(bv, dv)

  t.is(b.indexedLength, 3)
  t.is(c.indexedLength, 3)
  t.is(d.indexedLength, 3)

  t.is(b.linearizer.tails.length, 1)

  t.end()
})

/*

  b   c   d
  | x | x |
  b   c   d
  | x | x |
  b   c   d
  | /
  b

*/

test('majority alone - non-convergence', async t => {
  const [a, b, c, d, e] = await getWriters(5)

  // --- write ---

  await b.append()
  await c.append()
  await d.append()

  await b.sync(c, true)
  await d.sync(c, true)
  await c.sync([b, d], true)

  await b.append()
  await c.append()
  await d.append()

  await b.sync(c, true)
  await d.sync(c, true)
  await c.sync([b, d], true)

  await b.append()
  await c.append()
  await d.append()

  await b.sync(c, true)

  await b.append()

  console.log(b.linearizer.tip.map(v => v.value))

  t.end()
})

test('example.mjs', async t => {
  const [a, b, c] = await getWriters(5)

  await a.append()
  await b.append()

  await syncAll()

  await a.append()
  await a.append()
  await b.append()
  await b.append()

  await syncAll()

  await c.append()
  await a.append()

  await syncAll()

  await b.append()

  await syncAll()

  await a.append()
  await a.append()

  await syncAll()

  console.log('checkity checkpoint', await a.checkpoint())

  await syncAll()

  await a.append()

  await syncAll()

  await b.append()

  console.log(b._writersQuorum.size, b._writers.length)

  console.log(await a.latestCheckpoint())
  console.log(await b.latestCheckpoint())

  console.log('*** indexed ***')
  for await (const val of a.values()) console.log(val)
  console.log('***************\n')

  async function syncAll () {
    console.log('**** sync all ****')
    await sync(a, b)

    console.log('**** synced a and b ****')
    // await sync(a, c)
    // await sync(b, a)
    console.log('**** sync all done ****')
    console.log()
  }
})

async function sync (a, b, oneway = false) {
  const s1 = a.store.replicate(true)
  const s2 = b.store.replicate(false)

  s1.on('error', () => {})
  s2.on('error', () => {})

  s1.pipe(s2).pipe(s1)

  await a.update()
  if (!oneway) await b.update()

  s1.destroy()
  s2.destroy()
}

function replicate (a, b) {
  const s1 = a.store.replicate(true)
  const s2 = b.store.replicate(false)

  s1.on('error', () => {})
  s2.on('error', () => {})

  s1.pipe(s2).pipe(s1)

  return function teardown () {
    s1.destroy()
    s2.destroy()
  }
}

async function getWriters (n) {
  const stores = []
  for (let i = 0; i < n; i++) {
    const store = makeStore(i)
    const local = store.get({ name: 'local', valueEncoding: 'json' })
    stores.push({ store, local })
  }

  await Promise.all(stores.map(s => s.local.ready()))
  stores.sort((a, b) => Buffer.compare(a.local.key, b.local.key))

  const genesis = stores.map(s => b4a.toString(s.local.key, 'hex'))

  const writers = stores.map((s, i) => tester(s.store, i, genesis))
  await Promise.all(writers.map(w => w.ready()))

  return writers
}

function replicateMany (...writers) {
  const destroy = []

  for (let i = 0; i < writers.length; i++) {
    for (let j = i; i < writers.length; i++) {
      destroy.push(replicate(writers[i], writers[j]))
    }
  }

  return function teardown () {
    destroy.forEach(d => d())
  }
}

async function syncAll (...writers) {
  for (let i = 0; i < writers.length; i++) {
    await writers[i].sync(writers.filter(w => w !== writers[i]))
  }
}

function makeStore (seed) {
  return new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill(seed) })
}

function sleep () {
  return new Promise(resolve => setImmediate(resolve))
}

async function collect (ite) {
  const res = []
  for await (const v of ite) res.push(v)
  return res
}