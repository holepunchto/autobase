import Autobase from './index.js'
import Corestore from 'corestore'
import RAM from 'random-access-memory'

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply (nodes, view, base) {
  for (const node of nodes) {
    if (node.value.add) {
      console.log('add', base.addWriter.toString())
      base.addWriter(Buffer.from(node.value.add, 'hex'))
    }

    await view.append(node.value)
  }
}

const a = new Autobase(makeStore('a'), {
  valueEncoding: 'json',
  open,
  apply
})

await a.ready()

const b = new Autobase(makeStore('b'), a.key, {
  valueEncoding: 'json',
  open,
  apply
})

await b.ready()

a.debug = true
await a.append({
  add: b.local.key.toString('hex')
})

await sync(a, b)

console.log('pre append...')
await b.append('sup')

console.log('nu')

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

  await new Promise(r => setImmediate(r))

  await a.update()
  await b.update()

  s1.destroy()
  s2.destroy()
}

async function syncAll () {
  console.log('**** sync all ****')
  await sync(a, b)

  console.log('**** synced a and b ****')
  await sync(a, c)
  // await sync(b, a)
  console.log('**** sync all done ****')
  console.log()
}

function makeStore (seed) {
  return new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill(seed) })
}
