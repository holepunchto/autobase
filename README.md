# Autobase

## Install

`npm i autobase`

## Usage

```js
const Corestore = require('corestore')
const Autobase = require('autobase')

const store = new Corestore('./some-dir')
const local = new Autobase(store, remote.key, { apply, open })
await local.ready()

// on remote base
// remote.append({ addWriter: local.local.key })

await local.append('local 0')

// remote.append('remote 0')
// remote.append('remote 1')

await local.update()
await local.append('local 1')

for (let i = 0; i < local.view.length; i++) {
  console.log(await local.view.get(i))

  /*
  local 0
  remote 0
  remote 1
  local 1
  */
}

// create the view
function open (store) {
  return store.get('test')
}

// use apply to handle to updates
async function apply (nodes, view, hostcalls) {
  for (const { value } of nodes) {
    if (value.addWriter) {
      await hostcalls.addWriter(value.addWriter, { isIndexer: true })
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

The linearizing algorithm is able to define a point at which the ordering of the graph below will never change. This point advances continually, so long as a majority set of indexers are writing messages.

### Views

A linearized view may be created on top of an Autobase. This view can be updated to reflect the messages of within the base.

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

*IMPORTANT*: Autobase messages may be reordered as new data becomes available. Updates will be undone and reapplied internally. It is important that any data structures touched by the `apply` function have been derived from the `store` object passed to the `open` handler and that its fully deterministic. If any external data structures are used, these updates will not be correctly undone.

## API

### Autobase

#### `const base = new Autobase(store, bootstrap, opts)`

Instantiate an Autobase.

If loading an existing Autobase then set `bootstrap` to `base.key`, otherwise pass `bootstrap` as null or omit.

`opts` takes the following options:

```js
{
  open: store => { ... }, // create the view
  apply: (nodes, view, hostcalls) => { ... }, // handle nodes
  optimistic: false, // Autobase supports optimistic appends
  close: view => { ... }, // close the view
  valueEncoding, // encoding
  ackInterval: 1000 // enable auto acking with the interval
}
```

An `ackInterval` may be set to enable automatic acknowledgements. When enabled, in cases where it would help the linearizer converge the base shall eagerly append `null` values to the oplog.

Setting an autobase to be `optimistic` means that writers can append an `optimistic` block even when they are not a writer. For a block to be optimistically applied to the view, the writer must be acknowledge via `hostcall.ackWriter(key)`.

```js
const base = new Autobase(store, bootstrap, {
  optimistic: true,
  async apply (nodes, view, hostcalls) {
    for (const node of nodes) {
      // Acknowledge only even numbers
      if (node.value % 2 === 0) await hostcalls.ackWriter(node.from.key)
      await view.append(node.value)
    }
  }
})

await base.append(3, { optimistic: true }) // will not be applied
await base.append(2, { optimistic: true }) // will be applied
```

#### `base.key`

The primary key of the autobase.

#### `base.discoveryKey`

The discovery key associated with the autobase.

#### `base.isIndexer`

Whether the instance is an indexer.

#### `base.writable`

Whether the instance is a writer for the autobase.

#### `base.length`

The length of the system core. This is neither the length of the local writer nor the length of the view.

#### `base.signedLength`

The index of the system core that has been signed by a quorum of indexers. The system up until this point will not change.

#### `base.paused`

Whether the autobase is currently paused.

#### `await base.append(value, opts)`

Append a new entry to the autobase.

Options include:

```
{
  optimistic: false // Allow appending on an optimistic autobase while not a writer
}
```

#### `await base.update()`

Fetch all available data and update the linearizer.


#### `await base.pause()`

Pauses the autobase prevent the next apply from running.

#### `await base.resume()`

Resumes a paused autobase and will check for an update.

#### `const core = Autobase.getLocalCore(store, handlers, encryptionKey)`

Generate a local core to be used for an Autobase.

#### `const userData = Autobase.getUserData(core)`

Get user data associated with the autobase core.

### `AutoStore`

Each autobase creates a `AutoStore` which is used to create views. The store is passed to the `open` function.

#### `const core = await store.get(name || { name, valueEncoding })`

Load a `Hypercore` by name (passed as `name`). `name` should be passed as a string.

#### `await store.ready()`

Wait until all cores are ready.

### `AutobaseHostCalls`

An instance of this is passed to apply and can be used in apply to invoke the following side effects on the base itself.

#### `await host.addWriter(key, { isIndexer = true })`

Add a writer with the given `key` to the autobase allowing their local core to append. If `isIndexer` is `true`, it will be added as an indexer.

#### `await host.removeWriter(key)`

Remove a writer from the autobase. This will throw if the writer cannot be removed.

#### `await host.ackWriter(key)`

Acknowledge a writer even if they haven't been added before. This is most useful for applying `optimistic` blocks from writers that are not currently a writer.

#### `host.interrupt()`

#### `host.removeable(key)`

Returns whether the writer for the given `key` can be removed. The last indexer cannot be removed.
