import Autobase from './index.js'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import util from 'util'

util.inspect.defaultOptions.depth = 10

const a = new Autobase(new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill('a') }))
const b = new Autobase(new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill('b') }))

await a.ready()
await b.ready()

console.log('a', a.local)
console.log('b', b.local)

a.setWriters([a.local.key, b.local.key])
b.setWriters([a.local.key, b.local.key])

// a.setWriters([a.local.key])

await a.append('a0')
await a.append('a1')

await syncAll()

console.log('appending b0')

await b.append('b0')

console.log('appending b1')

await b.append('b1')

await syncAll()

await a.append('a3')

await syncAll()

await b.append('b2')

await a.append('a4')

await list('a', a)
await list('b', b)

await syncAll()

await list('a', a)
await list('b', b)
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
  console.log()
  await sync(a, b)
  // await sync(b, a)
  // await sync(a, c)
}
