const test = require('brittle')
const Corestore = require('corestore')
const RAM = require('random-access-memory')

const Autobase = require('../index.js')

const GENESIS = {
  n: [
    7, 4, 10, 15, 16, 2, 11,
    12, 8, 18, 3, 9, 0, 13,
    14, 6, 1, 5, 19, 17
  ],
  key: [
    '16e94bbf48ec0c913884e3b742e033008adc0c1a91c679448c3873f2ec2791d4',
    '19144faa7527c9ab6e743df9a384511d1dc7e52aa824fa1767968d1bc591bf75',
    '1abdfb0d2bee9810e74671402267d5088ba29934d0386d6225c213788df92efc',
    '36d977e235501a25786003642b53b26f2d3697f6bdb4e624f29a6d06910bd6c9',
    '43cf68c3fe304ab7d22ce5122d9c26660a2b64042c332e47cbfa2514f0d661b6',
    '515f8b21578f1b343d551e890cc63eea720d424ca3a01eb47fe39087be7a3b3e',
    '5a7cec13a883277065e5bbfed79cd30b0c71a611fd004075cf3d0b1e81b81f05',
    '5d9cfd1f469a568836d31137a07a6a6ef064003b6c2ae870836637f9a039f094',
    '61b0de7ac026fda0bc416b609ee797b727491cd7c46290096d6f2565ac2e3673',
    '6389e20c5b402b94025c7543240ad26fa34236cd3637cddd39f63cc7f8dfdd86',
    '762179664d8741a9e744a107fe06251a88ce5325f8f4793b177a0087de643fb7',
    '8d2472c40adef9c0f0aa5931d780cd6c01039c2f88228d19a83d81cad5b6cd84',
    'b54d1cb0587a130b2197b8d1c28d7870c6fdb21a04724364fbd499ea9c281156',
    'b5c395ce67cd8a32be1daa873e9d57aa756281050a059c134566bfed97baee30',
    'bbe79d21c855c0689067ffa33f6d9043749b9378c00da663e8c4366c0f76a259',
    'd3ce747379dc4be4d813261580351861d0f88212ae7a875bb71fe0152344fceb',
    'd9c0b8bbd8c71aed943f0142f9afc346fe28da0501d39c994525beb063e99ab8',
    'e0596504a57556495a51e388d9094370329d9487749ecedbc69ed42acd0402be',
    'fba939d9e2c3ce975284b5a8c1263c37168ffd2dd72c9801ee97025ac2a726ed',
    'fc109ecba89ffd8ec2b232235b95046ecc05988e3d1fd2485891750448c6bbd5'
  ]
}

function tester (seed, label, genesis, opts = {}) {
  const store = makeStore(seed)
  const base = new Autobase(store, genesis, { apply: opts.apply && apply, open: opts.open && open })

  base.debug = !!opts.debug
  base.name = label

  base.linearizer.debug = !!opts.debug
  base.linearizer.name = label

  let index = 0
  let last = 0

  return new Proxy(base, {
    get (target, prop) {
      switch (prop) {
        case 'sync':
          return syncTo
        case 'results':
          return results
        case 'list':
          return list
        case 'append':
          return append
        case 'tails':
          return tails
        case 'getView':
          return getView
        default:
          return Reflect.get(...arguments)
      }
    }
  })

  function open (store) {
    return store.get('double')
  }

  async function apply (nodes, view, base) {
    if (!base.debug) {
      console.log('apply for', label)
    }

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

  async function results () {
    const result = []
    while (last < base.length) {
      result.push((await base.get(last++)).value)
    }
    return result
  }

  async function list () {
    console.log('**** list ' + label + ' ****')
    for (let i = 0; i < base.length; i++) {
      console.log(i, (await base.get(i)).value)
    }
    console.log('')
  }

  function tails () {
    return base.linearizer.tails.map(t => t.value)
  }

  async function * getView () {
    for (let i = 0; i < base.view.length; i++) {
      yield await base.view.get(i)
    }
  }
}

/*

c - b - a - c - b - a

*/

test.solo('simple 3', async t => {
  const seed = GENESIS.n.slice(0, 3)
  const genesis = GENESIS.key.slice(0, 3)

  let writer = 0
  const a = tester(seed[writer++], 'a', genesis, { apply: true, open: true, debug: true })
  const b = tester(seed[writer++], 'b', genesis)
  const c = tester(seed[writer++], 'c', genesis)

  await a.ready()
  await b.ready()
  await c.ready()

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

  await a.list()

  console.log('----- begin ----')
  console.log('view.length', a.view.length)
  for await (const block of a.getView()) console.log(block)
  console.log('----- tails ----')
  for (const tail of a.tails()) console.log(tail)
  console.log('------ end -----')

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
  const seed = GENESIS.n.slice(0, 3)
  const genesis = GENESIS.key.slice(0, 3)

  let writer = 0
  const a = tester(seed[writer++], 'a', genesis, { apply: true, open: true })
  await a.ready()

  const b = tester(seed[writer++], 'b', genesis)
  const c = tester(seed[writer++], 'c', genesis)

  await b.ready()
  await c.ready()

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

  console.log(a.linearizer.tip.map(v => v.value))

  await a.list()

  console.log('----- begin ----')
  console.log('view.length', a.view.length)
  for await (const block of a.getView()) console.log(block)
  console.log('----- tails ----')
  for (const tail of a.tails()) console.log(tail)
  console.log('------ end -----')

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
  const seed = GENESIS.n.slice(0, 3)
  const genesis = GENESIS.key.slice(0, 3)

  let writer = 0

  const a = tester(seed[writer++], 'a', genesis, {})
  await a.ready()

  const b = tester(seed[writer++], 'b', genesis, { apply: true, open: true })
  const c = tester(seed[writer++], 'c', genesis, {})
  const d = tester(seed[writer++], 'd', genesis, {})
  const e = tester(seed[writer++], 'e', genesis, {})

  await b.ready()
  await c.ready()
  await d.ready()
  await e.ready()

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

  console.log(b.linearizer.tip.map(v => v.value))

  await b.list()

  console.log('----- begin ----')
  console.log('view.length', b.view.length)
  for await (const block of b.getView()) console.log(block)
  console.log('----- tails ----')
  for (const tail of b.tails()) console.log(tail)
  console.log('------ end -----')

  t.end()
})

/*

  b - c - d - b - c - d

*/

test('majority alone - convergence', async t => {
  const seed = GENESIS.n.slice(0, 3)
  const genesis = GENESIS.key.slice(0, 3)

  let writer = 0

  const a = tester(seed[writer++], 'a', genesis, {})
  const b = tester(seed[writer++], 'b', genesis, { apply: true, open: true })
  const c = tester(seed[writer++], 'c', genesis, {})
  const d = tester(seed[writer++], 'd', genesis, { apply: true, open: true })
  const e = tester(seed[writer++], 'e', genesis, {})

  await a.ready()
  await b.ready()
  await c.ready()
  await d.ready()
  await e.ready()

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

  console.log(b.linearizer.tip.map(v => v.value))

  await d.list()

  console.log('----- begin ----')
  console.log('view.length', d.view.length)
  for await (const block of d.getView()) console.log(block)
  console.log('----- tails ----')
  for (const tail of d.tails()) console.log(tail)
  console.log('------ end -----')

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
  const seed = GENESIS.n.slice(0, 3)
  const genesis = GENESIS.key.slice(0, 3)

  let writer = 0

  const a = tester(seed[writer++], 'a', genesis, {})
  const b = tester(seed[writer++], 'b', genesis, { apply: true, open: true })
  const c = tester(seed[writer++], 'c', genesis, {})
  const d = tester(seed[writer++], 'd', genesis, {})
  const e = tester(seed[writer++], 'e', genesis, {})

  await a.ready()
  await b.ready()
  await c.ready()
  await d.ready()
  await e.ready()

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

  await b.list()

  console.log('----- begin ----')
  console.log('view.length', b.view.length)
  for await (const block of b.getView()) console.log(block)
  console.log('----- tails ----')
  for (const tail of b.tails()) console.log(tail)
  console.log('------ end -----')

  t.end()
})

test('example.mjs', async t => {
  const seed = GENESIS.n.slice(0, 3)
  const genesis = GENESIS.key.slice(0, 3)

  let writer = 0
  const a = tester(seed[writer++], 'a', genesis, { apply: true, open: true })
  await a.ready()

  const b = tester(seed[writer++], 'b', genesis, { apply: true, open: true })
  const c = tester(seed[writer++], 'c', genesis, { apply: true, open: true })

  await b.ready()
  await c.ready()

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

  await a.list()

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
