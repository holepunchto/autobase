# Autobase

### [See the full API docs at docs.holepunch.to](https://docs.holepunch.to/building-blocks/autobase)

Automatically rebase multiple causally-linked Hypercores into a single, linearized Hypercore.

The output of an Autobase is "just a Hypercore", which means it can be used to transform higher-level data structures (like Hyperbee) into multiwriter data structures with minimal additional work.

These multiwriter data structures operate using an event-sourcing pattern, where Autobase inputs are "operation logs", and outputs are indexed views over those logs.

## How It Works

To see an example of how Autobase can be used alongside Hyperbee to build a P2P aggregation/voting tool, head over to [our multiwriter workshop](https://github.com/hypercore-protocol/p2p-multiwriter-with-autobase).

## Installation
```
npm install autobase
```

## Usage
An Autobase is constructed from a known set of trusted input Hypercores. Authorizing these inputs is outside of the scope of Autobase -- this module is unopinionated about trust, and assumes it comes from another channel.

Here's how you would create an Autobase from 3 known inputs, and a locally-available (writable) default input:
``` js
const Autobase = require('autobase')

// Assuming inputA, inputB, and inputC are Hypercore 10 instances
// inputA will be used during append operations
const base = new Autobase({
  inputs: [inputA, inputB, inputC],
  localInput: inputA,
  autostart: true
})

// Add a few messages to the local writer.
// These messages will contain the Autobase's latest vector clock by default.
await base.append('hello')
await base.append('world')

// base.view is a linearized view Hypercore with causal ordering. `output` is a Hypercore.
// When base.view.update() is called, the inputs will be automatically linearized and stored into the output.

// Use `view` as you would any other Hypercore.
await base.view.update()
await base.view.get(0)
```

Autobase lets you write concise multiwriter data structures. As an example, a multiwriter Hyperbee (with basic, last-one-wins conflict resolution) can be written with [~40 lines of code](examples/autobee-simple.js).

In addition multiwriter data structures built on Autobase inherit the same feature set as Hypercore. This means that users can securely query a multiwriter data structure built with Autobase by only downloading a fraction of the data.

## API

#### `const base = new Autobase({ inputs, outputs, ...opts } = {})`
Creates a new Autobase from a set of input/output Hypercores

Options include:

```js
{
  inputs: [],        // The list of Hypercores for Autobase to linearize
  outputs: [],       // An optional list of output Hypercores containing linearied views
  localInput: null,  // The Hypercore that will be written to in base.append operations
  localOutput: null, // A writable Hypercore that linearized views will be persisted into
  autostart: false,  // Create a linearized view (base.view) immediately
  apply: null,       // Create a linearized view (base.view) immediately using this apply function
  unwrap: false      // base.view.get calls will return node values only instead of full nodes
}
```

#### `base.inputs`
The list of input Hypercores.

#### `base.outputs`
The list of output Hypercores containing persisted linearized views.

#### `base.localInput`
If non-null, this Hypercore will be appended to in base.append operations.

#### `base.localOutput`
If non-null, `base.view` will be persisted into this Hypercore.

#### `const clock = base.clock()`
Returns a Map containing the latest lengths for all Autobase inputs.

The Map has the form: `(hex-encoded-key) -> (Hypercore length)`

#### `await Autobase.isAutobase(core)`
Returns `true` if `core` is an Autobase input or an output.

#### `await base.append(value, [clock], [input])`
Append a new value to the autobase.

* `clock`: The causal clock and defaults to base.latest.

#### `const clock = await base.latest([input1, input2, ...])`
Generate a causal clock linking the latest entries of each input.

`latest` will update the input Hypercores (`input.update()`) prior to returning the clock.

You generally will not need to use this, and can instead just use `append` with the default clock:
```js
await base.append('hello world')
```

#### `await base.addInput(input)`
Adds a new input Hypercore.

* `input` must either be a fresh Hypercore, or a Hypercore that has previously been used as an Autobase input.

#### `await base.removeInput(input)`
Removes an input Hypercore.

* `input` must be a Hypercore that is currently an input.

__A Note about Removal__

Removing an input, and then subsequently linearizing the Autobase into an existing output, could result in a large truncation operation on that output -- this is effectively "purging" that input entirely.

In the future, we're planning to add support for "soft removal", which will freeze an input at a specific length, and not process blocks past that length, while still preserving that input's history in linearized views. For most applications, soft removal matches the intuition behind "removing a user".

#### `await base.addOutput(output)`
Adds a new output Hypercore.

* `output` must be either a fresh Hypercore, or a Hypercore that was previously used as an Autobase output.

If `base.outputs` is not empty, Autobase will do "remote linearizing": `base.view.update()` will treat these outputs as the "trunk", minimizing the amount of local re-processing they need to do during updates.

#### `await base.removeOutput(output)`
Removes an output Hypercore. `output` can be either a Hypercore, or a Hypercore key.

* `output` must be a Hypercore, or a Hypercore key, that is currently an output (in `base.outputs`).

## API - Two Kinds of Streams

In order to generate shareable linearized views, Autobase must first be able to generate a deterministic, causal ordering over all the operations in its input Hypercores.

Every input node contains embedded causal information (a vector clock) linking it to previous nodes. By default, when a node is appended without additional options (i.e. `base.append('hello')`), Autobase will embed a clock containing the latest known lengths of all other inputs.

Using the vector clocks in the input nodes, Autobase can generate two types of streams:

### Causal Streams
Causal streams start at the heads (the last blocks) of all inputs, and walk backwards and yield nodes with a deterministic ordering (based on both the clock and the input key) such that anybody who regenerates this stream will observe the same ordering, given the same inputs.

They should fail in the presence of unavailable nodes -- the deterministic ordering ensures that any indexer will process input nodes in the same order.

The simplest kind of linearized view (`const view = base.linearize()`), is just a Hypercore containing the results of a causal stream in reversed order (block N in the index will not be causally-dependent on block N+1).

#### `const stream = base.createCausalStream()`
Generate a Readable stream of input blocks with deterministic, causal ordering.

Any two users who create an Autobase with the same set of inputs, and the same lengths (i.e. both users have the same initial states), will produce identical causal streams.

If an input node is causally-dependent on another node that is not available, the causal stream will not proceed past that node, as this would produce inconsistent output.

### Read Streams

Similar to `Hypercore.createReadStream()`, this stream starts at the beginning of each input, and does not guarantee the same deterministic ordering as the causal stream. Unlike causal streams, which are used mainly for indexing, read streams can be used to observe updates. And since they move forward in time, they can be live.

#### `const stream = base.createReadStream(opts = {})`
Generate a Readable stream of input blocks, from earliest to latest.

Unlike `createCausalStream`, the ordering of `createReadStream` is not deterministic. The read stream only gives you the guarantee that every node it yields will __not__ be causally-dependent on any node yielded later.

Read streams have a public property `checkpoint`, which can be used to create new read streams that resume from the checkpoint's position:
```js
const stream1 = base.createReadStream()
// Do something with stream1 here
const stream2 = base.createReadStream({ checkpoint: stream1.checkpoint }) // Resume from stream1.checkpoint

```

`createReadStream` can be passed two custom async hooks:
* `onresolve`: Called when an unsatisfied node (a node that links to an unknown input) is encountered. Can be used to dynamically add inputs to the Autobase.
  * Returning `true` indicates that you added new inputs to the Autobase, and so the read stream should begin processing those inputs.
  * Returning `false` indicates that you did not resolve the missing links, and so the node should be yielded immediately as is.
* `onwait`: Called after each node is yielded. Can be used to dynamically add inputs to the Autobase.

Options include:
```js
{
  live: false, // Enable live mode (the stream will continuously yield new nodes)
  tail: false, // When in live mode, start at the latest clock instead of the earliest
  map: (node) => node // A sync map function,
  checkpoint: null, // Resume from where a previous read stream left off (`readStream.checkpoint`)
  wait: true, // If false, the read stream will only yield previously-downloaded blocks.
  onresolve: async (node) => true | false, // A resolve hook (described above)
  onwait: async (node) => undefined // A wait hook (described above)
}
```

## API - Linearized Views

Autobase is designed for computing and sharing linearized views over many input Hypercores. A linearized view is a "merged" view over the inputs, giving you a way of interacting with the N input Hypercores as though it were a single, combined Hypercore.

These views, instances of the `LinearizedView` class, in many ways look and feel like normal Hypercores. They support `get`, `update`, and `length` operations.

By default, a view is just a persisted version of an Autobase's causal stream, saved into a Hypercore. But you can do a lot more with them: by passing a function into `linearize`'s `apply` option, you can define your own indexing strategies.

Linearized views are incredible powerful as they can be persisted to a Hypercore using the new `truncate` API added in Hypercore 10. This means that peers querying a multiwriter data structure don't need to read in all changes and apply them themself. Instead they can start from an existing view that's shared by another peer. If that view is missing indexing any data from inputs, Autobase will create a "view over the remote view", applying only the changes necessary to bring the remote view up-to-date. The best thing is that this all happens automatically for you!

### Customizing Views with `apply`

The default linearized view is just a persisted causal stream -- input nodes are recorded into an output Hypercore in causal order, with no further modifications. This minimally-processed view is useful on its own for applications that don't follow an event-sourcing pattern (i.e. chat), but most use-cases involve processing operations in the inputs into indexed representations.

To support indexing, `base.start` can be provided with an `apply` function that's passed batches of input nodes during rebasing, and can choose what to store in the output. Inside `apply`, the view can be directly mutated through the `view.append` method, and these mutations will be batched when the call exits.

The simplest `apply` function is just a mapper, a function that modifies each input node and saves it into the view in a one-to-one fashion. Here's an example that uppercases String inputs, and saves the resulting view into an `output` Hypercore:
```js
base.start({
  async apply (batch) {
    batch = batch.map(({ value }) => Buffer.from(value.toString('utf-8').toUpperCase(), 'utf-8'))
    await view.append(batch)
  }
})
// After base.start, the linearized view is available as a property on the Autobase
await base.view.update()
console.log(base.view.length)
```

More sophisticated indexing might require multiple appends per input node, or reading from the view during `apply` -- both are perfectly valid. The [multiwriter Hyperbee example](examples/autobee-simple.js) shows how this `apply` pattern can be used to build Hypercore-based indexing data structures using this approach.

### View Creation

#### `base.started`
A Boolean indicating if `base.view` has been created.

See the [linearized views section](#api---linearized-views) for details about the `apply` option.

Prior to calling `base.start()`, `base.view` will be `null`.

#### `base.start({ apply, unwrap } = {})`
Creates a new linearized view, and set it on `base.view`. The view mirrors the Hypercore API wherever possible, meaning it can be used whereever you would normally use a Hypercore.

You can either call `base.start` manually when you want to start using `base.view`, or you can pass either `apply` or `autostart` options to the Autobase constructor. If these constructor options are present, Autobase will start immediately.

If you choose to call `base.start` manually, it must only be called once.

Options include:
```js
{
  unwrap: false // Set this to auto unwrap the gets to only return .value
  apply (batch) {} // The apply function described above
}
```

#### `view.status`
The status of the last linearize operation.

Returns an object of the form `{ added: N, removed: M }` where:
* `added` indicates how many nodes were appended to the output during the linearization
* `removed` incidates how many nodes were truncated from the output during the linearization

#### `view.length`
The length of the view. Similar to `hypercore.length`.

#### `await view.update()`
Make sure the view is up to date.

#### `const entry = await view.get(idx, opts)`
Get an entry from the view. If you set `unwrap` to true, it returns `entry.value`.
Otherwise it returns an entry similar to this:

```js
{
  clock, // the causal clock this entry was created at
  value // the value that is stored here
}
```

#### `await view.append([blocks])`

__Note__: This operation can only be performed inside the `apply` function.

## License

MIT
