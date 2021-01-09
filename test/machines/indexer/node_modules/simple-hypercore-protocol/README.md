# simple-hypercore-protocol

Hypercore protocol state machine

```
npm install simple-hypercore-protocol
```

Includes a Noise handshake, and is not backwards compatible with Hypercore <= 7

## Usage

``` js
const Protocol = require('simple-hypercore-protocol')

const a = new Protocol(true, {
  send (data) { // send hook should send data
    b.recv(data)
  }
})

const b = new Protocol(false, {
  onrequest (channel, message) {
    console.log('got request message', message, 'on channel', channel)
  },
  send (data) {
    a.recv(data)
  }
})

// send a request message on channel 10
a.request(10, {
  index: 42
})
```

## API

This is still a work in progress, so that messages supported might change.
See the schema.proto file for the schema for each message.

#### `p = new Protocol(isInitator, handlers)`

Create a new protocol state machine.

* `isInitator` is a boolean indicating if you are a client or server
* `handlers` is a series of functions handling incoming messages

Everytime a binary message should be sent to another peer,
`handlers.send(data)` is invoked.

If there is a critical error, `handlers.destroy(err)` is called.

After the initial handshake transport encryption is enabled,
to ensure your stream is private.

To disable transport encryption set `handlers.encrypted = false`.

To disable the NOISE handshake set `handlers.noise = false` (works only when `encrypted` is also set to false).

#### `p.recv(data)`

Call this with incoming data.

#### `buf = p.remoteCapability(key)`

Create a remote capability for a key. Use this to verify
if a remote indeed had a key when you get an `open` message.

#### `buf = p.capability(key)`

Create a local capability.

#### `p.destroy(err)`

Destroy the protocol state machine.

#### `p.publicKey`

The local public key used for authentication.

#### `p.remotePublicKey`

The remotes public key.

#### `p.handshakeHash`

The noise handshake hash which uniquely identifies the noise session.

http://noiseprotocol.org/noise.html#channel-binding

#### `handlers.onauthenticate(remotePublicKey, done)`

Called when you should authenticate a remote public key.

#### `handlers.onhandshake()`

Called when the initial protocol handshake has finished.

#### `p.open(channel, message)`

Send an open message on a channel.

Note that if you message.key the protocol, will turn that into a capability that is sent instead of the key.

Receiving an open message triggers `handlers.onopen(channel, message)`

#### `p.options(channel, message)`

Send a options message on a channel.

Receiving a handshake message triggers `handlers.onoptions(channel, message)`

#### `p.status(channel, message)`

Send a status message on a channel.

Receiving a info message triggers `handlers.onstatus(channel, message)`

#### `p.have(channel, message)`

Send a have message on a channel.

Receiving a have message triggers `handlers.onhave(channel, message)`

#### `p.unhave(channel, message)`

Send an unhave message on a channel.

Receiving an unhave message triggers `handlers.onunhave(channel, message)`

#### `p.want(channel, message)`

Send a want message on a channel.

Receiving a want message triggers `handlers.onwant(channel, message)`

#### `p.unwant(channel, message)`

Send an unwant message on a channel.

Receiving an unwant message triggers `handlers.onunwant(channel, message)`

#### `p.request(channel, message)`

Send a request message on a channel.

Receiving a request message triggers `handlers.onrequest(channel, message)`

#### `p.cancel(channel, message)`

Send a cancel message on a channel.

Receiving a cancel message triggers `handlers.oncancel(channel, message)`

#### `p.data(channel, message)`

Send a data message on a channel.

Receiving a data message triggers `handlers.ondata(channel, message)`

#### `keyPair = Protocol.keyPair([seed])`

Static function to generate a Noise key pair, optionally from a seed.

## License

MIT
