# Autobase

A multiwriter data structure for combining multiple writer cores into a view of the system. Using the event sourcing pattern, writers append blocks which are linearized into an eventually consistent order for building a view of the system, combining their inputs.

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
async function apply (nodes, view, host) {
  for (const { value } of nodes) {
    if (value.addWriter) {
      await host.addWriter(value.addWriter, { indexer: true })
      continue
    }

    await view.append(value)
  }
}
```

### Ordering

Autobase writer nodes explicitly reference previous nodes creating a causal directed acyclic graph (DAG). The nodes are linearized by analyzing the causal references so:

1. Nodes never precede nodes they reference.
2. Ordering is eventually consistent.

### Reordering

As new causal information comes in, existing nodes may be reordered when causal forks occur. Any changes to the view will be undone and reapplied on top of the new ordering.

### Signed Length

The linearizing algorithm is able to define checkpoints after which the ordering of the graph will never change. This point advances continually, so long as a majority set of indexers are writing messages. These checkpoints allow peers who are behind to catchup quickly and reduce the need to reorder nodes.

### Views

A view is one or more hypercores whose contents are created by deterministically applying the linearized nodes from writers. The view represents the combined history of all writers' inputs or the current state of the system as a whole.

Autobase accepts an `open` function for creating views and an `apply` function that can be used to update the views based on the writer nodes.

```js
function open (store) {
  return store.get('my-view')
}
```

```js
async function apply (nodes, view, host) {
  for (const n of nodes) await view.append(n)
}
```

*IMPORTANT*: Autobase messages may be reordered as new data becomes available. Updates will be undone and reapplied internally. So it is important that the `open` handler returns a data structure only derived from its `store` object argument and that while updating the view in the `apply` function, the `view` argument is the only data structure being update and that its fully deterministic. If any external data structures are used, these updates will not be correctly undone.

## API

### Autobase

#### `const base = new Autobase(store, bootstrap, opts)`

Instantiate an Autobase.

If loading an existing Autobase then set `bootstrap` to `base.key`, otherwise pass `bootstrap` as null or omit.

`opts` takes the following options:

```js
{
  open: (store, host) => { ... }, // create the view
  apply: async (nodes, view, host) => { ... }, // handle nodes to update view
  optimistic: false, // Autobase supports optimistic appends
  close: async view => { ... }, // close the view
  valueEncoding, // encoding
  ackInterval: 1000 // enable auto acking with the interval
  encryptionKey: buffer, // Key to encrypt the base
  encrypt: false, // Encrypt the base if unencrypted & no encryptionKey is set
  encrypted: false, // Expect the base to be encrypted, will throw an error otherwise, defaults to true if encryptionKey is set
  fastForward: true, // Enable fast forwarding. If passing { key: base.core.key }, they autobase will fastforward to that key first.
  wakeup: new ProtomuxWakeup(), // Set a custom wakeup protocol for hinting which writers are active, see `protomux-wakeup` for protocol details
}
```

An `ackInterval` may be set to enable automatic acknowledgements. When enabled, in cases where it would help the linearizer converge, the base shall eagerly append `null` values to merge causal forks.

Setting an autobase to be `optimistic` means that writers can append an `optimistic` block even when they are not a writer. For a block to be optimistically applied to the view, the writer must be acknowledge via `host.ackWriter(key)`.

_Note:_ Optimistic blocks should self verify in the `apply` handler to prevent unintended writers from appending blocks to exploit the system. If the `apply` handler does not have a way to verify optimistic blocks, any writer could append blocks even when not added to the system.

```js
const base = new Autobase(store, bootstrap, {
  optimistic: true,
  async apply (nodes, view, host) {
    for (const node of nodes) {
      const { value } = node
      // Verify block
      if (value.password !== 'secrets') continue

      // Acknowledge only even numbers
      if (value.num % 2 === 0) await host.ackWriter(node.from.key)

      await view.append(value.num)
    }
  }
})

// Passing the password 'secrets' represents being verifiable
await base.append({ num: 3, password: 'secrets' }, { optimistic: true }) // will not be applied because `ackWriter` isnt called
await base.append({ num: 2, password: 'secrets' }, { optimistic: true }) // will be applied
await base.append({ num: 4, password: 'incorrect' }, { optimistic: true }) // will not be applied because the block isn't verifiable
```

#### `base.key`

The primary key of the autobase.

#### `base.discoveryKey`

The discovery key associated with the autobase.

#### `base.isIndexer`

Whether the instance is an indexer.

#### `base.writable`

Whether the instance is a writer for the autobase.

#### `base.view`

The view of the autobase derived from writer inputs. The view is created in the `open` handler and can have any shape. The most common `view` is a [hyperbee](https://github.com/holepunchto/hyperbee).

#### `base.length`

The length of the system core. This is neither the length of the local writer nor the length of the view. The system core tracks the autobase as a whole.

#### `base.signedLength`

The index of the system core that has been signed by a quorum of indexers. The system up until this point will not change.

#### `base.paused`

Returns `true` if the autobase is currently paused, otherwise returns `false`.

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

#### `await base.ack(bg = false)`

Manually acknowledge the current state by appending a `null` node that references known head nodes. `null` nodes are ignored by the `apply` handler and only serve as a way to acknowledge the current linearized state. Only indexers can ack.

If `bg` is set to `true`, the ack will not be appended immediately but will set the automatic ack timer to trigger as soon as possible.

#### `const hash = await base.hash()`

Returns the hash of the system core's merkle tree roots.

#### `const stream = base.replicate(isInitiator || stream, opts)`

Creates a replication stream for replicating the autobase. Arguments are the same as [corestores's `.replicate()`](https://github.com/holepunchto/corestore?tab=readme-ov-file#const-stream--storereplicateoptsorstream).

```js
const swarm = new Hyperswarm()

// Join a topic
swarm.join(base.discoveryKey)

swarn.on('connection', (connection) => store.replicate(connection))
```

#### `const heads = base.heads()`

Gets the current writer heads. A writer head is a node which has no causal dependents, aka it is the latest write. If there is more than one head, there is a causal fork which is pretty common.

#### `await base.pause()`

Pauses the autobase prevent the next apply from running.

#### `await base.resume()`

Resumes a paused autobase and will check for an update.

#### `const core = Autobase.getLocalCore(store, handlers, encryptionKey)`

Generate a local core to be used for an Autobase.

`handlers` are any options passed to `store` to get the core.

#### `const { referrer, view } = Autobase.getUserData(core)`

Get user data associated with an autobase `core`. `referrer` is the `.key` of the autobase the `core` is from. `view` is the `name` of the view.

#### `const isBase = Autobase.isAutobase(core, opts)`

Returns whether the core is an autobase core. `opts` are the same options as [core.get(index, opts)](https://github.com/holepunchto/hypercore?tab=readme-ov-file#const-block--await-coregetindex-options).

#### `base.on('update', () => { ... })`

Triggered when the autobase view updates after `apply` has finished running.

#### `base.on('interrupt', (reason) => { ... })`

Triggered when `host.interrupt(reason)` is called in the `apply` handler. See [`host.interrupt(reason)`](#hostinterruptreason) for when interrupts are used.

#### `base.on('fast-forward', (to, from) => { ... })`

Triggered when the autobase fast forwards to a state already with a quorum. `to` and `from` are the `.signedLength` after and before the fast forward respectively.

Fast forwarding speeds up an autobase catching up to peers.

#### `base.on('is-indexer', () => { ... })`

Triggered when the autobase instance is an indexer.

#### `base.on('is-non-indexer', () => { ... })`

Triggered when the autobase instance is not an indexer.

#### `base.on('writable', () => { ... })`

Triggered when the autobase instance is now a writer.

#### `base.on('unwritable', () => { ... })`

Triggered when the autobase instance is no longer a writer.

#### `base.on('warning', (warning) => { ... })`

Triggered when a warning is triggered.

#### `base.on('error', (err) => { ... })`

Triggered when an error is triggered while updating the autobase.

### `AutoStore`

Each autobase creates a `AutoStore` which is used to create views. The store is passed to the `open` function.

#### `const core = store.get(name || { name, valueEncoding })`

Load a `Hypercore` by name (passed as `name`). `name` should be passed as a string.

### `AutobaseHostCalls`

An instance of this is passed to `apply` and can be used to invoke the following side effects on the base itself.

#### `await host.addWriter(key, { indexer = true })`

Add a writer with the given `key` to the autobase allowing their local core to append. If `indexer` is `true`, it will be added as an indexer.

#### `await host.removeWriter(key)`

Remove a writer from the autobase. This will throw if the writer cannot be removed.

#### `await host.ackWriter(key)`

Acknowledge a writer even if they haven't been added before. This is most useful for applying `optimistic` blocks from writers that are not currently a writer.

#### `host.interrupt(reason)`

Interrupt the applying of writer blocks optionally giving a `reason`. This will emit an `interrupt` event passing the `reason` to the callback and close the autobase.

Interrupts are an escape hatch to stop the apply function and resolve the issue by updating your apply function to handle it. A common scenario is adding a new block type that an older peer gets from a newer peer.

#### `host.removeable(key)`

Returns whether the writer for the given `key` can be removed. The last indexer cannot be removed.
