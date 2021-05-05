const test = require('tape')
const ram = require('random-access-memory')
const Hypercore = require('hypercore-x')

const Autobase = require('..')
const Autobee = require('../examples/autobee-simple')

test('simple autobee', async t => {
  const firstUser = new Hypercore(ram)
  const firstIndex = new Hypercore(ram)
  const secondUser = new Hypercore(ram)
  const secondIndex = new Hypercore(ram)

  const base1 = new Autobase([firstUser, secondUser], { indexes: firstIndex })
  const base2 = new Autobase([firstUser, secondUser], { indexes: secondIndex })

  const bee1 = new Autobee(base1, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  const bee2 = new Autobee(base2, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })

  await bee1.put('a', 'b')
  await bee2.put('c', 'd')

  {
    const node = await bee2.get('a')
    t.true(node)
    t.same(node.value, 'b')
  }

  {
    const node = await bee1.get('c')
    t.true(node)
    t.same(node.value, 'd')
  }

  t.end()
})

/*
// TODO: Wrap Hyperbee extension to get this working
test.skip('autobee extension', async t => {
  const NUM_RECORDS = 5

  const store1 = await Corestore.fromStorage(ram)
  const store2 = await Corestore.fromStorage(ram)
  const store3 = await Corestore.fromStorage(ram)
  // Replicate both corestores
  replicateWithLatency(store1, store2)
  replicateWithLatency(store1, store3)

  const { user: firstUser } = await Autobase.createUser(store1)
  const manifest = [firstUser]

  const bee1 = new Autobee(store1, manifest, firstUser, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  const bee2 = new Autobee(store2, Manifest.deflate(manifest), null, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  const bee3 = new Autobee(store3, Manifest.deflate(manifest), null, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8',
    extension: false
  })

  for (let i = 0; i < NUM_RECORDS; i++) {
    await bee1.put('' + i, '' + i)
  }
  await bee1.refresh()

  console.log('after put')
  await new Promise(resolve => setTimeout(resolve, 1000))

  console.log(await collect(bee2.createReadStream()))

  const t0 = process.hrtime()
  const first = await collect(bee2.createReadStream())
  const t1 = process.hrtime(t0)

  const second = await collect(bee3.createReadStream())
  const t2 = process.hrtime(t0)

  t.same(first.length, NUM_RECORDS)
  t.same(second.length, NUM_RECORDS)
  console.log('t1:', t1, 't2:', t2, 't0:', t0)
  t.true(t1[1] < (t2[1] - t1[1]) / 2)

  console.log('first:', first)
  console.log('second:', second)

  t.end()
})

function replicate (store1, store2) {
  const s1 = store1.replicate(true)
  const s2 = store2.replicate(false)
  s1.pipe(s2).pipe(s1)
}

function replicateWithLatency (store1, store2, latency = 10) {
  const s1 = store1.replicate(true)
  const s2 = store2.replicate(false)
  s1.pipe(new LatencyStream(latency / 2)).pipe(s2).pipe(new LatencyStream(latency / 2)).pipe(s1)
}

async function collect (s) {
  const buf = []
  for await (const n of s) {
    buf.push(n)
  }
  return buf
}
*/
