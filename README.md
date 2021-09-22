# Autobase

*⚠️ Alpha Warning: Autobase currently depends on an alpha version of [Hypercore 10](https://github.com/hypercore-skunkworks/hypercore-x). ⚠️*

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
const base = new Autobase([writerA, writerB, writerC], { defaultWriter: writerA })

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

#### `const base = new Autobase(inputs, opts = {})`
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

#### `const stream = base.createCausalStream()`
Generate a Readable stream of input blocks with deterministic, causal ordering.

#### `const stream = base.createReadStream(opts = {})`
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

## License

MIT
