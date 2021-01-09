# `noise-protocol`

[![Build Status](https://travis-ci.org/emilbayes/noise-protocol.svg?branch=master)](https://travis-ci.org/emilbayes/noise-protocol)

> Javascript implementation of the Noise Protocol Framework based on libsodium

:rotating_light: :warning: :rotating_light: BETA :rotating_light: :warning: :rotating_light:

Note that this implementation is low level and requires knowledge of the
[Noise Protocol Framework](http://noiseprotocol.org/noise.html), and is aimed to
be a building block for higher-level modules wishing to implement
application-specific handshakes securely.

This module only implements the `Noise_*_25519_ChaChaPoly_BLAKE2b` handshake,
meaning `Curve25519` for DH, `ChaCha20Poly1305` for AEAD and `BLAKE2b` for
hashing.

## Usage

```js
var noise = require('noise-protocol')

var sClient = noise.keygen()
var sServer = noise.keygen()

// Initialize a Noise_KK_25519_ChaChaPoly_BLAKE2b handshake
var client = noise.initialize('KK', true, Buffer.alloc(0), sClient, null, sServer.publicKey)
var server = noise.initialize('KK', false, Buffer.alloc(0), sServer, null, sClient.publicKey)

var clientTx = Buffer.alloc(128)
var serverTx = Buffer.alloc(128)

var clientRx = Buffer.alloc(128)
var serverRx = Buffer.alloc(128)

// -> e, es, ss
noise.writeMessage(client, Buffer.alloc(0), clientTx)
noise.readMessage(server, clientTx.subarray(0, noise.writeMessage.bytes), serverRx)

// <- e, ee, se
var serverSplit = noise.writeMessage(server, Buffer.alloc(0), serverTx)
var clientSplit = noise.readMessage(client, serverTx.subarray(0, noise.writeMessage.bytes), clientRx)

// Safely dispose of finished HandshakeStates
noise.destroy(client)
noise.destroy(server)

// Can now do transport encryption with splits
console.log(serverSplit)
console.log(clientSplit)
```

## API

### Constants

- `noise.PKLEN` length of a public key in bytes
- `noise.SKLEN` length of a secret key in bytes

### Supported Patterns

All one-way and fundamental handshake patterns are currently supported:

- `N`
- `K`
- `X`
- `NN`
- `KN`
- `NK`
- `KK`
- `NX`
- `KX`
- `XN`
- `IN`
- `XK`
- `IK`
- `XX`
- `IX`

### `var handshakeState = noise.initialize(handshakePattern, initiator, prologue, [staticKeys], [ephemeralKeys], [remoteStaticKey], [remoteEphemeralKey])`

Create a new Noise handshake instance with:

* `handshakePattern` must be String and one of [supported patterns](#supported-patterns)
* `initiator` must be Boolean
* `prologue` must be Buffer. This can be an empty Buffer (`Buffer.alloc(0)`) if
  not used
* `staticKeys` is local static keys as an object of `{publicKey, secretKey}`.
  This is only required if the handshake pattern mandates these as shared out of band (premessages)
* `ephemeralKeys` is local ephemeral keys as an object of `{publicKey, secretKey}`.
  This is only required if the handshake pattern mandates these as shared out of band (premessages)
* `remoteStaticKey` is a Buffer of `PKLEN` bytes. This is most likely not required
* `remoteEphemeralKey` is a Buffer of `PKLEN` bytes. This is most likely not required

:alert: Key material passed in is copied into libsodium Secure Buffers, which
can be cleared with `noise.destroy(state)`. Be aware that you manually have to
destroy this state object, unless you want to rely on GC clearing it for you.

Returns a `HandshakeState` object, which should be treated as an opaque object.
This state is passed as the first argument to subsequent `noise` functions.

### `var maybeSplit = noise.writeMessage(state, payload, messageBuffer)`

Process a new message pattern and write any output to be transmitted to the
receiving party into `messageBuffer`. Any payload data can be passed as
`payload`, or the empty Buffer in case of no payload.

* `state` must be a `HandshakeState` as returned by `noise.initialize`
* `payload` must be Buffer. Use the empty Buffer (`Buffer.alloc(0)`) in case of
  no payload. Whether it is safe to send a `payload` at a specific step of the
  handshake is at the discretion of the user. Please refer to [Noise - 7.3. Handshake pattern validity](http://noiseprotocol.org/noise.html#handshake-pattern-validity)
* `messageBuffer` must be Buffer. In the worst case it requires
  `PKLEN + PKLEN + MACLEN` (32 + 32 + 16) bytes, for a two keys and a MAC,
  plus any bytes required for `payload.byteLength + MACLEN` (`MACLEN = 16`)

If no more message patterns are left to process, a **Split** will occur. Please
see below for details. If more patterns are pending, nothing is returned.

The function may throw an error if:
* There are no more message patterns to be processed (meaning a split already
  occurred)
* The current state expects a message to be read and not written
* The `HandshakeState` is invalid for the current message pattern
* `messageBuffer` is too small to contain the required data
* An encryption error occurred

In any of these cases there was a misuse and the `HandshakeState` should be
`noise.destroy`ed and connection aborted.

### `noise.writeMessage.bytes`

This property is set after `noise.writeMessage` has been successfully executed
and signals how many bytes were written to `messageBuffer`

### `var maybeSplit = noise.readMessage(state, message, payloadBuffer)`

Process a new message pattern and read any input received from `message`.
Any remaining data in `message` is treated as payload data and will be decrypted
(depending on the `HandshakeState`) and written to `payloadBuffer`.

* `state` must be a `HandshakeState` as returned by `noise.initialize`
* `message` must be a Buffer, as produced by `noise.writeMessage`. Any framing
  or length information is left to the application as described in the Noise
  Specification.
* `payloadBuffer` must be Buffer. Use the empty Buffer (`Buffer.alloc(0)`) if no
  payload is expected, though this may throw an error if a payload is attempted
  written

If no more message patterns are left to process, a **Split** will occur. Please
see below for details. If more patterns are pending, nothing is returned.

The function may throw an error if:
* There are no more message patterns to be processed (meaning a split already
  occurred)
* The current state expects a message to be written and not read
* The `HandshakeState` is invalid for the current message pattern
* `payloadBuffer` is too small for the required data
* An decryption error occurred

In any of these cases there was a misuse and the `HandshakeState` should be
`noise.destroy`ed and connection aborted.

### `noise.readMessage.bytes`

This property is set after `noise.readMessage` has been successfully executed
and signals how many bytes were written to `payloadBuffer`

### `noise.destroy(state)`

Takes a `HandshakeState` and destroys all internal data (eg. securely zeros out
data contained in Buffer-like objects and resets state). Use this to dispose of
state objects after a split has occurred or upon error

### Split

If no more message patterns are left to process, a **Split** will occur, as
described in the Noise Specification. In this implementation an object with
`{tx: Buffer, rx: Buffer}` will be returned, each being a
[`sodium-native` Secure Buffer](https://github.com/sodium-friends/sodium-native#memory-protection)
containing a cipher state as a contiguous piece of memory. It is encoded as
`32 byte k | 8 byte n`, as describe in the Noise Specification. You can either
choose to use these Buffers with the [`cipherState`](cipher-state.js)
functions or extract values and use with another transport encryption, as long
as you are aware of the security implication of either choice. For initiator and
responder, `tx` and `rx` are opposite so a responders `rx` is equal to an
initiators `tx`.

## Install

```sh
npm install noise-protocol
```

## Deviations from the Noise specification

* Functions follow the `fn(state, output, args...)` convention
* Names the 16 bytes for an authentication tag as `MACLEN`

## Unsupported features

- Any other cryptographic primitives than the ones mentioned above
- PSK, fallback and deferred patterns. Support may be added at a later time

## License

[ISC](LICENSE)
