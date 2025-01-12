import Autobase from './index.js'
import Corestore from 'corestore'
import RAM from 'random-access-memory'

// In the open function we're getting an AutocoreSession instance from the store.
// An AutocoreSession instance provides a subset of the hypercore api.
// It's common to use a higher level data structure like hyperbee here.
// The return value is later used in the apply function as the `view` argument
// and can be accessed as the `view` property on an autobase instance.
function open (store) {
  return store.get('view', { valueEncoding: 'json' })
}

// The apply function is where autobase takes the inputs
// from all writers and applies them to the view.
async function apply (nodes, view, base) {
  for (const node of nodes) {
    // We need to call `base.addWriter` to successfully add the `b` base
    // so that it can append entries. We also need to turn the key back
    // into a buffer as that is what base.addWriter expects.
    if (node.value.type === 'addWriter') {
      await base.addWriter(Buffer.from(node.value.key, 'hex'))
    }

    // We don't need to do anything special with the `message` type
    // so it gets passed straight to view.append.
    await view.append(node.value)
  }
}

// Create the first base
const a = new Autobase(makeStore('a'), {
  valueEncoding: 'json',
  open,
  apply
})
await a.ready()

// Create the second base
// Note that we're passing in the key from the first base
// and using the same open and apply functions as the first base.
// Passing a.key is necessary to instruct this instance that
// it will be using the associated hypercore key to replicate data
// and join as a writer with its own local input core.
const b = new Autobase(makeStore('b'), a.key, {
  valueEncoding: 'json',
  open,
  apply
})
await b.ready()

// Add b as a writer.
// In this case we are using json as the valueEncoding.
// If we didn't set a valueEncoding we would need to pass a buffer here.
// We're using a `type` property so that the apply function
// can perform different operations based on the type.
await a.append({
  // Specify the type of entry being appended.
  type: 'addWriter',

  // Note that this uses b.local.key and turns the buffer to a hex string which is
  // needed because we're using a json valueEncoding that would inappropriately
  // stringify and parse the key buffer turning it into an object in the apply function.
  key: b.local.key.toString('hex')
})

// Sync the entry that adds b as a writer.
await sync(a, b)

// Append an entry to both bases.
await b.append({ type: 'message', text: 'sup' })
await a.append({ type: 'message', text: 'hi' })

// Sync the new message entries.
await sync(a, b)

// list the entries in each base
// you should see the same entries in both bases
// including the `add` entry that adds b as a writer
// and the message from each base
await list('a', a)
await list('b', b)

// list() accepts a base name and a base instance, logs the index, type and value
// for each item in the view, logs the index and the value.
async function list (name, base) {
  console.log('**** list ' + name + ' ****')

  // Note that we're accessing data using `base.view` rather than `base`.
  // This is the same `view` we created in the open function and
  // used in the apply function.
  for (let i = 0; i < base.view.length; i++) {
    const node = await base.view.get(i)
    console.log(i, node.type, node.key || node.text)
  }
  console.log('')
}

// This sync helper function creates a 1:1 replication stream between two Autobase instances, `a` and `b` long enough to fully sync the data and then destroys the streams.
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

// This makeStore helper function takes a seed and returns a new Corestore using RAM storage with a 32-byte primaryKey initialized to the seed
function makeStore (seed) {
  return new Corestore(RAM, { primaryKey: Buffer.alloc(32).fill(seed) })
}
