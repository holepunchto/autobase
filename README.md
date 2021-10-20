# Autobase

*⚠️ Alpha Warning ⚠️ - Autobase only works with the alpha release of [Hypercore 10](https://github.com/hypercore-protocol/hypercore-next)*

Automatically rebase multiple causally-linked Hypercores into a single, linearized Hypercore.

The output of an Autobase is "just a Hypercore", which means it can be used to transform higher-level data structures (like Hyperbee) into multiwriter data structures with minimal additional work.

These multiwriter data structures operate using an event-sourcing pattern, where Autobase inputs are "operation logs", and outputs are indexed views over those logs.

## How It Works

To see an example of how Autobase can be used alongside Hyperbee to build a P2P aggregation/voting tool, head over to [our multiwriter workshop](https://github.com/hypercore-protocol/hypernews).

## Installation
```
npm install autobase
```

## Usage
An Autobase is constructed from a known set of trusted input Hypercores. Authorizing these inputs is outside of the scope of Autobase -- this module is unopinionated about trust, and assumes it comes from another channel.

Here's how you would create an Autobase from 3 known inputs, and a locally-available (writable) default input:
``` js
const autobase = require('autobase')

// Assuming inputA, inputB, and inputC are Hypercore 10 instances
// inputA will be the "default input" during append operations
const base = new Autobase([inputA, inputB, inputC], { input: inputA })

// Add a few messages to the local writer.
// These messages will contain the Autobase's latest vector clock by default.
await base.append('hello')
await base.append('world')

// Create a linearized "index" Hypercore with causal ordering. `output` is a Hypercore.
// When index.update is called, the inputs will be automatically rebased into the index.
const index = base.createRebasedIndex(output)

// Use `index` as you would any other Hypercore.
await index.update()
await index.get(0)
```

Autobase lets you write concise multiwriter data structures. As an example, a multiwriter Hyperbee (with basic, last-one-wins conflict resolution) can be written with [~45 lines of code](examples/autobee-simple.js).

## API

### Autobase Creation

#### `const base = new Autobase(inputs, opts = {})`
Creates a new Autobase from a set of input Hypercores

* `inputs`: An Array of causally-linked Hypercores

Options include:

```js
{
  defaultInput: null,  // A default Hypercore to append to
  indexes: null,       // A list of rebased index Hypercores
  autocommit: true     // Automatically persist changes to rebased indexes after updates
}
```

#### `base.inputs`
The list of input Hypercores.

#### `base.defaultIndexes`
The list of default rebased indexes.

### Adding Entries

#### `await base.append(value, [clock], [input])`
Append a new value to the autobase.

* `clock`: The causal clock and defaults to base.latest

#### `const clock = await base.latest([input1, input2, ...])`

### Dynamically Changing Inputs/Indexes

#### `await base.addInput(input)`
Adds a new input Hypercore.

* `input` must either be a fresh Hypercore, or a Hypercore that has previously been used as an Autobase input.

#### `await base.removeInput(input)`
Removes an input Hypercore.

* `input` must be a Hypercore that is currently an input.

__A Note about Removal__

Removing an input, and then subsequently rebasing the Autobase into an existing index, could result in a large rebasing operation -- this is effectively "purging" that input from the index.

In the future, we're planning to add support for "soft removal", which will freeze an input at a specific length, and not process blocks past that length, while still preserving that input's history in derived indexes. For most applications, soft removal matches the intuition behind "removing a user".

#### `await base.addDefaultIndex(index)`
Adds a new default index Hypercore.

* `index` must be either a fresh Hypercore, or a Hypercore that was previously used as an Autobase index.

Default indexes are mainly useful during [remote rebasing](), when readers of an Autobase can use them as the "trunk" during rebasing, and thus can minimize the amount of local re-indexing they need to do during updates.

#### `await base.removeDefaultIndex(index)`
Removes a default index Hypercore.

* `index` must be a Hypercore that is currently a default index.

### Two Kinds of Streams

In order to generate shareable, derived indexes, Autobase must first be able to generate a deterministic, causal ordering over all the operations in its input Hypercores.

Every input node contains embedded causal information (a vector clock) linking it. By default, when a node is appended without additional options (i.e. `base.append('hello')`), Autobase will embed a clock containing the latest known lengths of all other inputs.

Using the vector clocks in the input nodes, Autobase can generate two types of streams:

### Causal Streams
Causal streams start at the heads (the last blocks) of all inputs, and walk backwards and yield nodes with a deterministic ordering (based on both the clock and the input key) such that anybody who regenerates this stream will observe the same ordering, given the same inputs.

They should fail in the presence of unavailable nodes -- the deterministic ordering ensures that any indexer will process input nodes in the same order.

The simplest kind of rebased index (`const index = base.createRebasedIndex()`), is just a Hypercore containing the results of a causal stream in reversed order (block N in the index will not be causally-dependent on block N+1).

#### `const stream = base.createCausalStream()`
Generate a Readable stream of input blocks with deterministic, causal ordering.

Any two users who create an Autobase with the same set of inputs, and the same lengths (i.e. both users have the same initial states), will produce identical causal streams.

If an input node is causally-dependent on another node that is not available, the causal stream will not proceed past that node, as this would produce inconsistent output.

### Read Streams

Similar to `Hypercore.createReadStream()`, this stream starts at the beginning of each input, and does not guarantee the same deterministic ordering as the causal stream. Unlike causal streams, which are used mainly for indexing, read streams can be used to observe updates. And since they move forward in time, they can be live.

#### `const stream = base.createReadStream(opts = {})`
Generate a Readable stream of input blocks, from earliest to latest.

Unlike `createCausalStream`, the ordering of `createReadStream` is not deterministic. The read stream only gives you the guarantee that every node it yields will __not__ be causally-dependent on any node yielded later.

`createReadStream` can be passed two custom async hooks:
* `resolve`: Called when an unsatisfied node (a node that links to an unknown input) is encountered. Can be used to dynamically add inputs to the Autobase.
  * Returning `true` indicates that you added new inputs to the Autobase, and so the read stream should begin processing those inputs.
  * Returning `false` indicates that you did not resolve the missing links, and so the node should be yielded immediately as is.
* `wait`: Called after each node is yielded. Can be used to dynamically add inputs to the Autobase.

Options include:
```js
{
  live: false, // Enable live mode (the stream will continuously yield new nodes)
  map: (node) => node // A sync map function
  resolve: async (node) => true | false, // A resolve hook (described above)
  wait: async (node) => undefined // A wait hook (described above)
}
```

### Rebased Indexes

Autobase is designed with indexing in mind. There's a one-to-many relationship between an Autobase instance, and the derived indexes it can be used to power. A single Autobase might be indexed in many different ways.

These derived indexes, called `RebasedIndexes`, in many ways look and feel like normal Hypercores. They support `get`, `update`, and `length` operations.

By default, an index is just a persisted version of an Autobase's causal stream, saved into a Hypercore. But you can do a lot more with them, by using the `apply` option to `createRebasedIndex` you can define your own indexing strategies.

Rebased Indexes are incredible powerful as they can be persisted to a Hypercore using the new truncate API added in Hypercore 10. This means that peers queries your multiwriter data structure don't need to read in all changes and apply them themself. Instead they can just start from an existing index, and if that index is missing indexing any data from inputs just apply those locally - this all happens automatically for you!

### Customizing Indexes with `apply`

The default rebased index is just a persisted causal stream -- input nodes are recorded into an index Hypercore in causal order, with no further modifications. This minimal "index" is useful on its own for applications that don't follow an event-sourcing pattern (i.e. chat), but most use-cases involve processing operations in the inputs into indexed outputs.

To support indexing, `createRebasedIndex` can be provided with an `apply` function that's passed batches of input nodes during rebasing, and can choose what to store in the index. Inside `apply`, the index can be directly mutated through the `index.append` method, and these mutations will be batched when the call exits.

The simplest `apply` function is just a mapper, a function that modifies each input node and saves it into the index in a one-to-one fashion. Here's an example that uppercases String inputs, and saves the resulting index into an `output` Hypercore:
```js
const index = base.createRebasedIndex(output, {
  async apply (batch) {
    batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
    await index.append(batch)
  }
})
```

More sophisticated indexing might require multiple appends per input node, or reading from the index during `apply` -- both are perfectly valid. The [multiwriter Hyperbee example](examples/autobee-simple.js) shows how this `apply` pattern can be used to build Hypercore-based indexing data structures using this approach.

### Index Creation

#### `const index = base.createRebasedIndex(indexes, opts)`
Creates a new auto rebasing index. The `index` instance returned, is very similar
to a Hypercore meaning it can be used where ever you would use a Hypercore.

Options include:
```js
{
  unwrap: false // Set this to auto unwrap the gets to only return .value
  apply (batch) {} // The apply function described above
}
```

### `RebasedIndex` API

#### `index.status`
The status of the last rebase operation.

Returns an object of the form `{ added: N, removed: M }` where:
* `added` indicates how many nodes were appended to the index during the rebase
* `removed` incidates how many nodes were truncated from the index during the rebase

#### `index.length`
The length of the rebased index. Similar to `hypercore.length`.

#### `await index.update()`
Make sure the index is up to date.

#### `const entry = await index.get(idx, opts)`
Get an entry from the index. If you set `unwrap` to true, it returns `entry.value`.
Otherwise it returns an entry similar to this:

```js
{
  clock, // the causal clock this entry was created at
  value // the value that is stored here
}
```

#### `await index.append([blocks])`

__Note__: This operation can only be performed inside the `apply` function.

## License

MIT
