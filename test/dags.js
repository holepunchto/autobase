const test = require('brittle')
const Corestore = require('corestore')
const RAM = require('random-access-memory')

const Autobase = require('../index.js')

const GENESIS = {
  n: [
     7, 4, 10, 15, 16,  2, 11,
    12, 8, 18,  3,  9,  0, 13,
    14, 6,  1,  5, 19, 17
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

class Tester {
  constructor (base, label) {
    this.base = base
    this.label = label
    this.index = 0
    this.last = 0
  }

  ready () {
    return this.base.ready()
  }

  sync (base, oneway) {
    if (Array.isArray(base)) return Promise.all(base.map(b => this.sync(b, oneway)))
    return sync(this.base, base.base, oneway)
  }

  append () {
    return this.base.append({
      debug: this.label + this.index++
    })
  }

  async results () {
    const result = []
    while (this.last < this.base.length) {
      result.push((await this.base.get(this.last++)).value)
    }
    return result
  }

  async list () {
    console.log('**** list ' + this.label + ' ****')
    for (let i = 0; i < this.base.length; i++) {
      console.log(i, (await this.base.get(i)).value)
    }
    console.log('')
  }
}

function open (store) {
  return store.get('double')
}

async function apply (nodes, view, base) {
  if (!base.debug) {
    console.log('apply for a')
  }

  for (const node of nodes) {
    if (node.value.add) {
      base.system.addWriter(b4a.from(node.value.add, 'hex'))
    }

    await view.append(node.value)
    await view.append(node.value)
  }
}

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

  const a = new Tester(new Autobase(makeStore(seed[0]), genesis, { apply, open }), 'a')
  await a.ready()

  const b = new Tester(new Autobase(makeStore(seed[1]), genesis, {}), 'b')
  const c = new Tester(new Autobase(makeStore(seed[2]), genesis, {}), 'c')

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

  console.log(a.base.linearizer.tip.map(v => v.value))

  await a.list()

  console.log('----- begin ----')
  console.log('view.length', a.base.view.length)
  for (let i = 0; i < a.base.view.length; i++) {
    console.log(await a.base.view.get(i))
  }
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

  const a = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'a')
  await a.ready()

  const b = new Tester(new Autobase(makeStore(seed[writer++]), genesis, { apply, open }), 'b')
  const c = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'c')
  const d  = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'b')
  const e = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'c')

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

  console.log(b.base.linearizer.tip.map(v => v.value))

  await a.list()

  console.log('----- begin ----')
  console.log('view.length', b.base.view.length)
  for (let i = 0; i < b.base.view.length; i++) {
    console.log(await b.base.view.get(i))
  }
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

test('majority alone', async t => {
  const seed = GENESIS.n.slice(0, 3)
  const genesis = GENESIS.key.slice(0, 3)

  let writer = 0

  const a = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'a')
  await a.ready()

  const b = new Tester(new Autobase(makeStore(seed[writer++]), genesis, { apply, open }), 'b')
  const c = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'c')
  const d  = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'b')
  const e = new Tester(new Autobase(makeStore(seed[writer++]), genesis, {}), 'c')

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

  console.log(b.base.linearizer.tip.map(v => v.value))

  await a.list()

  console.log('----- begin ----')
  console.log('view.length', b.base.view.length)
  for (let i = 0; i < b.base.view.length; i++) {
    console.log(await b.base.view.get(i))
  }
  console.log('------ end -----')

  t.end()
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

async function syncAll () {
  console.log('**** sync all ****')
  await sync(a, b)

  console.log('**** synced a and b ****')
  await sync(a, c)
  await sync(b, a)
  console.log('**** sync all done ****')
  console.log()
}

function makeStore (seed) {
  return new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill(seed) })
}
