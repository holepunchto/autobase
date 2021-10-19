# Autobase

Automatically rebase multiple causally-linked Hypercores into a single, linearized Hypercore.

The output of an Autobase is "just a Hypercore", which means it can be used to transform higher-level data structures (like Hyperbee) into multi-writer data structures with minimal additional work.

These multi-writer data structures operate using an event-sourcing pattern, where Autobase inputs are "operation logs", and outputs are indexed views over those logs.

For a more in-depth walkthrough of the Autobase API/internals, check out [this workshop](https://github.com/hypercore-skunkworks/autobase-workshop).

## How It Works
__TODO: Should have details here, and in the walkthrough__

## Installation
```
npm install autobase
```

## Usage
An Autobase is constructed from a known set of trusted writer Hypercores. Authorizing these writers is outside of the scope of Autobase -- this module is unopinionated about trust, and assumes it comes from another channel.

Here's how you would create an Autobase from 3 known writers, and a locally-available (writable) default writer:
``` js
const autobase = require('autobase')

// Assuming writerA, writerB, and writerC are Hypercore 10 instances
const base = new Autobase([writerA, writerB, writerC], { input: writerA })

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

Autobase lets you write concise multi-writer data structures. As an example, a multi-writer Hyperbee (with basic, last-one-wins conflict resolution) can be written with [~45 lines of code](examples/autobee-simple.js).

## API

### Autobase Creation

##### `const base = new Autobase(inputs, opts = {})`
Creates a new Autobase from a set of input Hypercores

`inputs`: An Array of causally-linked Hypercores
`opts` is an Object that can contain the following options:
```js
{
  defaultInput: null,  // A default Hypercore to append to
  indexes: null,       // A list of rebased index Hypercores
  autocommit: true     // Automatically persist changes to rebased indexes after updates
}
```

##### `base.inputs`
The list of input Hypercores.

##### `base.defaultIndexes`
The list of default rebased indexes.

### Adding Log Entries

##### `await base.append(input, value, clock)`

##### `const clock = await base.latest([input1, input2, ...])`

### Dynamically Adding and Removing Inputs/Indexes

##### `await base.addInput(input)`

##### `await base.removeInput(input)`

##### `await base.addDefaultIndex(index)`

##### `await base.removeDefaultIndex(index)`

### Two Kinds of Streams

##### `const stream = base.createCausalStream()`
Generate a Readable stream of input blocks with deterministic, causal ordering.

##### `const stream = base.createReadStream(opts = {})`
Generate a Readable stream of input blocks, from earliest to latest.

Unlike `createCausalStream`, the ordering of `createReadStream` is not deterministic. The read stream only gives you the guarantee that every node it yields will __not__ be causally-dependent on any node yielded later.

`createReadStream` can be passed two custom async hooks:
1. `resolve`: Called when an unsatisfied node (a node that links to an unknown input) is encountered. Can be used to dynamically add inputs to the Autobase.
  * Returning `true` indicates that you added new inputs to the Autobase, and so the read stream should begin processing those inputs.
  * Returning `false` indicates that you did not resolve the missing links, and so the node should be yielded immediately as is.
2. `wait`: Called after each node is yielded. Can be used to dynamically add inputs to the Autobase.

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

These derived indexes, called `RebasedIndexes`, in many ways look and feel like normal Hypercores. They support `get`, `update`, and `length` operations. Under the hood, though, they're...

By default, an index is just a persisted version of an Autobase's causal stream, saved into a Hypercore. 

#### Index Creation 

#### `const index = base.createRebasedIndex(indexes, opts)`

#### `RebasedIndex` API

##### `index.status`
The status of the last rebase operation.

Returns an object of the form `{ added: N, removed: M }` where:
* `added` indicates how many nodes were appended to the index during the rebase
* `removed` incidates how many nodes were truncated from the index during the rebase

##### `index.length`
The length of the rebased index. Similar to `hypercore.length`.

##### `index.byteLength`
The length of the rebased index in bytes.

##### `await index.update()`

##### `await index.get(idx, opts)`

##### `await index.append([blocks])`

__Note__: This operation can only be performed inside the `apply` function.

##### `await index.commit()`

## License

MIT
