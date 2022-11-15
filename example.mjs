import Autobase from './index.js'
import Corestore from 'corestore'
import RAM from 'random-access-memory'

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

await b.bump()

console.log('appending b0')

await b.append('b0')

console.log('appending b1')

await b.append('b1')

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
  await sync(a, b)
  // await sync(b, a)
  // await sync(a, c)
}
