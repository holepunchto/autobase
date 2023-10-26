# Autobase Test Harness

This module provides a convenient test harness for autobase.

## Usage

```js
const RAM = require('random-access-memory')
const { Base, Network, Room } = require('./')

const room = new Room(() => RAM.reusable())
await room.ready()

const root = room.root

const network = room.replicate() // set off replicators

const members = await room.createMembers(50)
const indexers = await room.addIndexers(members.slice(0, 10))

await room.spam(indexers, 100) // each indexer sends 100 messages
await room.confirm()

const [left, right] = await network.split(6) // simulate netsplit 

await left.members[0].spam(200)
await room.confirm(left.members) // left has majorit

console.log(left.members[1].view.indexedLength) // 1200
console.log(right.members[0].view.indexedLength) // 1000
```

## Base API

### `const base = new Base(storage, opts)`

Create a new base

```js
const opts = {
  root, // set the root base
  valueEncoding: 'json',
  ackInterval: 100,
  ackThreshold: 4,
  fastForward: true,
  open (store) {
    // return a view, default is hypercore
  },
  apply (batch, view, base) {
    // default is append to hypercore
  },
  close () {
    // close the view
  },
  addWriter (key, indexer) {
    // compose a message for adding a writer
  }
  message (n) {
    // compose a message, used by spam method
  }
}
```

#### `await base.ready()`

Wait for the autobase to open.

#### `base.key`

The local key of the base as a Buffer.

#### `get hex () {`

The local key of the base as a hex string.

#### `await base.join({ indexer = false, base = this.root }) {`

Get added to the root base.

#### `await base.addWriter(key, indexer)`

Add a writer to the base if we can.

#### `await base.sync(otherBases)`

Resolve when we see the same state as all `otherBases`.

#### `const unreplicate = base.replicate(remote)`

Replicate with the `remote`, calling `unreplicate()` will end the replication stream.

#### `await base.unreplicate(remote)`

Stop replication with `remote`

#### `await base.offline`

End all replication.

#### `await base.append(data)`

Write a message to the base.

#### `const { base, view, linearizer, indexers } = getState()`

Convenience method for inspecting internals.

#### `await base.spam(n)`

Send `n` messages to the base. Internally this will call the `message` function provided in the constructor, passing it a count of the total number of messages sent.

## Network API

A network is a set of bases that are all replicating with one another.

### `const network = new Network(storage, opts)`

Create a network.

#### `for (const base of network) {}`

Iterate over the bases.

#### `network.size`

The number of bases in the network.

#### `network.has(base)`

Check if a base is in this network.

#### `network.add(base)`

Add a base to the network.

#### `await network.delete(base)`

Delete a base from the network.

#### `network.clear()`

Clear all peers from the network. Will not end any ongoing replication streams.

#### `await network.sync()`

Make sure all bases in the network are synced.

#### `network.merge(otherNetwork)`

Combine another network into this one.

#### `const [left, right] await network.split(index)`

Split a network into 2 at the given `index`.

#### `netowrk.replicate(base)`

Ensure all the peers in the network are replicating with base.

#### `network.unreplicate(base)`

All peers in the network will stop replicating with this base.

If `base` is in the network, it will remain in the network and can be brought up again with `network.replicate`

#### `network.up()`

All peers in the network will stop replicating with each other.

#### `network.down()`

All peers in the network will stop replicating with each other.

#### `network.destroy()`

End all replication and clear the network.

## Room API

A room is a set of bases following the autobase.

### `const room = new Room(() => storage, opts)`

Create a new room of bases with a root.

The first parameter should be a function that returns a storage instance that can be passed to a Corestore.

```js
// ... all opts as for Base above, as well as:

const opts = {
  size, // intital number of members
  rng () {
    // return number in range [0, 1), default is Math.random
  }
}
```

#### `await room.ready()`

Wait for the room to open and members to join.

#### `room.key`

The bootstrap key for the room.

#### `for (const member of room) {}`

Iterate over the bases in the room

#### `const member = await room.createMember ()`

Create a new member.

#### `const members = await createMembers (n)`

Create `n` new members.

#### `await room.sync([bases])`

Wait until `bases` are synced. If no argument is provided, `bases` will be all the members of the room.

#### `replicate([bases])`

Ensure `bases` are replicating with eachother. If no argument is provided, `bases` will be all the members of the room.

#### `await room.addWriters(members, { indexer = false, indexers, serial = false, random = false })`

Add each base in `members` as a writer.

If `indexer` is set to true, the bases will be added as indexers.

`indexers` may be used to provide the set of existing indexers to be used to confirm the operation.

If `serial` is set to true, the writers will be added one after another.

If `random` is set to true, a random indexer will be chosen to write the message to the room.

#### `await room.addIndexers(members, opts)`

Convenience method to add a set of writers as indexers.

#### `async confirm (indexers) {}`

Bump the indexed length of the room using the given indexers.

Expects that indexers are already replicating.

#### `spam (writers, messages) {}`

Spam the room with using the given writers. Messages can be passed a number or as an array of numbers, corresponding to the number of messages each writer should send respectively.
