# Autobase

## Install

`npm i autobase`

## Usage

```js
const RAM = require('random-access-memory')
const Corestore = require('corestore')
const Autobase = require('autobase')

const store = new Corestore(RAM.reusable())
const local = new Autobase(store, remote.key, { apply, open })
await local.ready()

// on remote base
// remote.append({ addWriter: local.local.key })

await local.append('local 0')

// remote.append('remote 0')
// remote.append('remote 1')

await local.update()
await local.append('local 1')

for (let i = 0; i < base1.view.length; i++) {
  console.log(await base1.view.get(i))

  /*
  local 0
  remote 0
  remote 1
  local 1
  */
}

// create the view
async function open (store) {
  return store.get('test')
}

// use apply to handle to updates
async function apply (nodes, view, base) {
  for (const { value } of nodes) {
    if (value.addWriter) {
      await base.addWriter(value.addWriter, { isIndexer: true })
      continue
    }

    await view.append(value)
  }
}
```

### Ordering

Autobase nodes explicitly reference previous nodes in the graph. The nodes are linearized by analyzing the causal references.

### Reordering

As new causal information comes in, existing nodes may be reordered. Any changes to the view will be undone and reapplied on top of the new ordering.

### Indexed Length

The linearizing algorithm is able to define a point at which the ordering of the graph below will never change.

### Views

An indexed view may be created on top of an Autobase. This view can be updated to reflect the messages of within the base.

Autobase accepts an `open` function for creating views and an `apply` function that can be used to update a view.

```js
async function open (store) {
  return store.get('my-view')
}
```

```js
async function apply (nodes, view, base) {
  for (const n of nodes) await view.append(nodes)
}
```

*IMPORTANT*: Autobase messages may be reordered as new data becomes available. Updates will be undone and reapplied internally, but this can _only_ work if the view is built with an instance of an `Autocore`. It is important that any data structures touched by the `apply` function have been derived from the `store` object passed to the `open` handler. If any external data structures are used, these updates will not be correctly undone.

## API

### Autobase

#### `const base = new Autobase(store, bootstrap, opts)`

Instantiate an Autobase.

If loading an existing Autobase then set `bootstrap` to `base.key`, otherwise pass `bootstrap` as null.

`opts` takes the following options:

```js
{
  open: store => { ... }, // create the view
  apply: (nodes, view, base) => { ... }, // handle nodes
  close: view => { ... }, // close the view
  valueEncoding, // encoding
  ackInterval: 1000 // enable auto acking with the interval
}
```

An `ackInterval` may be set to enable automatic acknowledgements. When enabled, in cases where it would help the linearizer converge the base shall eagerly append `null` values to the oplog.

#### `base.key`

The primary key of the autobase.

#### `base.discoveryKey`

The discovery key associated with the autobase.

#### `await base.append(value)`

Append a new entry to the autobase.

#### `await base.update({ wait: false })`

Fetch all available data and update the linearizer.

Setting `wait` option will wait for latest writer blocks to be fetched.

#### `const checkpoint = await base.checkpoint()`

Fetch a static checkpoint of the autobase state.

#### `const core = Autobase.getLocalCore(store, handlers, encryptionKey)`

Generate a local core to be used for an Autobase.

#### `const userData = Autobase.getUserData(core)`

Get user data associated with the autobase core.

### `AutoStore`

Each autobase creates a `AutoStore` which is used to create views. The store is passed to the `open` function.

#### `const core = await store.get(name || { name, valueEncoding })`

Load a `Autocore` by name (passed as `name`) and return a `AutocoreSession` for it. `name` should be passed as a string.

#### `await store.ready()`

Wait until all cores are ready.

### `AutocoreSession`

#### `const core = await store.get(name || { name, valueEncoding })`

Create or load a core and return a session for it. A string may be passed directly or otherwise an object can be passed to define `valueEncoding`

#### `core.indexedLength`

The ordering of blocks before this index is guaranteed to be consisted for all writers.

#### `core.length`

The number of blocks currently in the core

#### `await core.update()`

Ensure the core is at the latest state.

#### `await core.get(seq)`

Get an entry from a core.

#### `await core.append(buffers)`

Append data to the core
