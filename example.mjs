import Autobase from './index2.js'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import util from 'util'

// util.inspect.defaultOptions.depth = 10

async function apply (batch, base) {
  // console.log('apply batch...', batch)
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'))
    }
  }
}

const genesis = [
  'f0978c6d7a9ff36cdf600d85134550a4e1ebaef541203c2f119ed53b6f294990',
  '34431d2aa1a0474a6c8e9e96b114b589ef74b94917fa52064c4aadad942bda1f',
  '10b2d42ea4e3933450d1fb204bd3f33debebffd81b4020631623cce8260d3c15'
]

const a = new Autobase(new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill('a') }), genesis, { apply })

await a.ready()

const b = new Autobase(new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill('b') }), genesis, { apply })

const c = new Autobase(new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill('c') }), genesis, { apply })

await b.ready()
await c.ready()

// console.log(b.local.key.toString('hex'))

// console.log('a', a.local)
// console.log('b', b.local)

console.log('appending...', c.local.key.toString('hex'))

await a.append({
  debug: 'a0'
})

await a.append({
  debug: 'a1'
})

await b.append({
  debug: 'b0'
})

await b.append({
  debug: 'b1'
})

await syncAll()

console.log('------------------------\n\n\n')

console.log('a', a.pending.unindexed.map(v => v.value))
console.log('b', b.pending.unindexed.map(v => v.value))

// await c.append({
//   debug: 'c0'
// })

process.exit()

// console.log(b.pending.tails[0].writer.core)

await syncAll()

// console.log('preappend')

// console.log(a.pending.heads.length)
await a.append({
  debug: 'a1'
})

process.exit()

// console.log(a.pending.indexers[1].getCached(0))

await syncAll()

await b.append({
  debug: 'b1'
})

console.log('after append')

await syncAll()

console.log('nununununu')
process.exit()

await b.append({
  add: c.local.key.toString('hex'),
  debug: 'this is b adding c'
})

console.log(b._writersQuorum.size, b._writers.length)
console.log('whatsup')

console.log(await a.latestCheckpoint())
console.log(await b.latestCheckpoint())


// a.addWriter(b.local.key)

// a.setWriters([a.local.key, b.local.key])
// b.setWriters([a.local.key, b.local.key])

// a.setWriters([a.local.key])

// await a.append('a0')
// await a.append('a1')

// await syncAll()

// console.log('appending b0')

// await b.append('b0')

// console.log('appending b1')

// await b.append('b1')

// await syncAll()

// await a.append('a3')

// await syncAll()

// await b.append('b2')

// await a.append('a4')

// await list('a', a)
// await list('b', b)

// await syncAll()

// await list('a', a)
// await list('b', b)
// await a.update()

async function list (name, base) {
  console.log('**** list ' + name + ' ****')
  for (let i = 0; i < base.length; i++) {
    console.log(i, (await base.get(i)).value)
  }
  console.log('')
}

async function sync (a, b) {
  const s1 = a.store.replicate(true)
  const s2 = b.store.replicate(false)

  s1.on('error', () => {})
  s2.on('error', () => {})

  s1.pipe(s2).pipe(s1)

  await a.update()
  await b.update()

  s1.destroy()
  s2.destroy()
}

async function syncAll () {
  console.log('**** sync all ****')
  await sync(a, b)

  console.log('**** synced a and b ****')
  // await sync(b, a)
  // await sync(a, c)
  console.log('**** sync all done ****')
  console.log()
}
