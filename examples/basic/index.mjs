import Autobase from '../../index.js'
import Corestore from 'corestore'
import { replicateAndSync } from 'autobase-test-helpers'

function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

async function apply (nodes, view, base) {
  for (const node of nodes) {
    if (node.value.add) {
      await base.addWriter(Buffer.from(node.value.add, 'hex'))
    }

    await view.append(node.value)
  }
}

const a = new Autobase(makeStore('A'), {
  valueEncoding: 'json',
  open,
  apply
})

await a.ready()

const b = new Autobase(makeStore('B'), a.key, {
  valueEncoding: 'json',
  open,
  apply
})

await b.ready()

await a.append({
  add: b.local.key.toString('hex')
})

await replicateAndSync([a, b])

console.log('- B appends -')
await b.append('sup')

// B appended but hasn't synced with 'a'
await list('A', a)
await list('B', b)

// Outputs:
// - B appends -
// - list A -
// 0 add writer 3c059a578e217790630a5454d33f254bda36b96beb33e2d664cee8302ff7d329

// - list B -
// 0 add writer 3c059a578e217790630a5454d33f254bda36b96beb33e2d664cee8302ff7d329
// 1 sup

console.log('== Sync A & B ==\n')
await replicateAndSync([a, b])

// Both synced
await list('A', a)
await list('B', b)

// Outputs:
// == Sync A & B ==

// - list A -
// 0 add writer 3c059a578e217790630a5454d33f254bda36b96beb33e2d664cee8302ff7d329
// 1 sup

// - list B -
// 0 add writer 3c059a578e217790630a5454d33f254bda36b96beb33e2d664cee8302ff7d329
// 1 sup

async function list (name, base) {
  console.log('- list ' + name + ' -')
  for (let i = 0; i < base.view.length; i++) {
    const node = await base.view.get(i)
    if (typeof node === 'object' && 'add' in node) {
      // Print pretty version of 'add' block
      console.log(i, 'add writer', node.add)
    } else {
      console.log(i, node)
    }
  }
  console.log('')
}

function makeStore (seed) {
  return new Corestore('./example-corestore-peer-' + seed, { primaryKey: Buffer.alloc(32).fill(seed) })
}
