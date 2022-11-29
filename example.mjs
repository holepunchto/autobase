import Autobase from './index.js'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import util from 'util'
import b4a from 'b4a'

// util.inspect.defaultOptions.depth = 10

// async function apply (batch, base) {
//   // console.log('apply batch...', batch)
//   for (const { value } of batch) {
//     if (value.add) {
//       await base.addWriter(Buffer.from(value.add, 'hex'))
//     }
//   }
// }

// const genesis = [
//   'f0978c6d7a9ff36cdf600d85134550a4e1ebaef541203c2f119ed53b6f294990',
//   '34431d2aa1a0474a6c8e9e96b114b589ef74b94917fa52064c4aadad942bda1f',
//   '10b2d42ea4e3933450d1fb204bd3f33debebffd81b4020631623cce8260d3c15'
// ]

function open (store) {
  return store.get('double')
}

async function apply (nodes, view, base) {
  for (const node of nodes) {
    if (node.value.add) {
      base.system.addWriter(b4a.from(node.value.add, 'hex'))
    }

    await view.append(node.value)
    await view.append(node.value)
  }
}

const a = new Autobase(makeStore('a'), [], {
  open,
  apply
})

a.name = 'a'
a.linearizer.name = 'a'

await a.ready()

const genesis = [
  a.local.key
]

// a.linearizer.ontruncate = function (len, old) {
//   console.log('a is truncating...', len, old)
//   console.log(a.linearizer.tip.slice(0).map(v => v.value))
//   console.log(a.linearizer.tip.slice(len).map(v => v.value))
// }

a.debug = true
a.linearizer.debug = true

const b = new Autobase(makeStore('b'), genesis, {
  open,
  apply
})

b.linearizer.name = b.name = 'b'


const c = new Autobase(makeStore('c'), genesis, {
  open,
  apply
})

c.name = 'c'

await b.ready()
await c.ready()

await a.append({
  add: b4a.toString(b.local.key, 'hex'),
  debug: 'a0'
})

await a.append({
  add: b4a.toString(c.local.key, 'hex'),
  debug: 'a1'
})


console.log('----- begin ----')
console.log('a.view.length', a.view.length)
for (let i = 0; i < a.view.length; i++) {
  console.log(await a.view.get(i))
}
console.log('------ end -----')

// console.log(a.system)

// process.exit()

await syncAll()

// await b.append({ debug: 'b0' })

// process.exit()

console.log()
console.log()
console.log()
console.log()

console.log('----- begin ----')
console.log('b.view.length', b.view.length)
for (let i = 0; i < b.view.length; i++) {
  console.log(i, await b.view.get(i))
}
console.log('------ end -----')

// console.log('----- begin ----')
// console.log('a.view.length', a.view.length)
// for (let i = 0; i < a.view.length; i++) {
//   console.log(i, await a.view.get(i))
// }
// console.log('------ end -----')

// console.log(a.system.length)
console.log(b.system.length)

process.exit()

// console.log(b.local.key.toString('hex'))

// console.log('a', a.local)
// console.log('b', b.local)

console.log('appending... a', a.local.key.toString('hex'))
console.log('appending... b', b.local.key.toString('hex'))
console.log('appending... c', c.local.key.toString('hex'))

// a.debug = true
// a.linearizer.debug = true


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

await c.append({
  debug: 'c0'
})

await a.append({
  debug: 'a2'
})

await syncAll()

await b.append({
  debug: 'b2'
})

await syncAll()

console.log('---------------')

await a.append({
  debug: 'a3'
})

await a.append({
  debug: 'a4'
})

await syncAll()

// console.log(a.linearizer.tip)
// console.log(a.linearizer._next(a.linearizer.tails))

// console.log('a', a.linearizer.unindexed.map(v => v.value))
// console.log('b', b.linearizer.unindexed.map(v => v.value))
// console.log('c', c.linearizer.unindexed.map(v => v.value))

console.log('----- begin ----')
console.log('view.length', a.view.length)
for (let i = 0; i < a.view.length; i++) {
  console.log(await a.view.get(i))
}
console.log('------ end -----')

console.log('----- begin ----')
console.log('view.length', b.view.length)
for (let i = 0; i < b.view.length; i++) {
  console.log(await b.view.get(i))
}
console.log('------ end -----')

console.log('checkity checkpoint', await a.checkpoint())

process.exit()

// console.log(b.linearizer.tails[0].writer.core)

await syncAll()

// console.log('preappend')

// console.log(a.linearizer.heads.length)
await a.append({
  debug: 'a1'
})

await syncAll()

process.exit()

// console.log(a.linearizer.indexers[1].getCached(0))

await syncAll()

await b.append({
  debug: 'b1'
})

await syncAll()

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
  // await sync(a, c)
  // await sync(b, a)
  console.log('**** sync all done ****')
  console.log()
}

function makeStore (seed) {
  return new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill(seed) })
}
