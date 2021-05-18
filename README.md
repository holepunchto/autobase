# Autobase

*⚠️ Alpha Warning: Autobase currently depends on an alpha version of [Hypercore 10](https://github.com/hypercore-skunkworks/hypercore-x). ⚠️*

Automatically rebase multiple causally-linked Hypercores into a single, linearized Hypercore.

The output of an Autobase is "just a Hypercore", which means it can be used to transform higher-level data structures (like Hyperbee) into multi-writer data structures with minimal additional work.

For a more in-depth walkthrough of the Autobase API/internals, check out [this workshop](https://github.com/hypercore-skunkworks/autobase-workshop).

## Installation
```
npm install autobase
```

## Usage
An Autobase is constructed from a known set of trusted writer Hypercores. Authorizing these writers is outside of the scope of Autobase -- this module is unopinionated about trust, and assumes it comes from another channel.

Here's how you would create an Autobase from 3 known writers, and a locally-available (writable) "default" writer:
``` js
const autobase = require('autobase')

// Assuming writerA, writerB, and writerC are Hypercore 10 instances
const base = new Autobase([writerA, writerB, writerC], { defaultWriter: writerA })

// Add a few messages to the local writer.
// These messages will contain the Autobase's latest vector clock by default.
await base.append('hello')
await base.append('world')

// Create a linearized "index" Hypercore with causal ordering.
// When index.update is called, the inputs will be automatically rebased into the index.
const index = base.createRebasedIndex()

// Use `index` as you would any other Hypercore.
await index.update()
await index.get(0)
```

## API


## License

MIT
