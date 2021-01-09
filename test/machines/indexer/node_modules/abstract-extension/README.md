# abstract-extension

Small abstraction to help build out user defined extension messages in an RPC system.

```
npm install abstract-extension
```

See [hypercore](https://github.com/mafintosh/hypercore) and [hypercore-protocol](https://github.com/mafintosh/hypercore-protocol)
for a full example on how to use this

## API

#### `const AbstractExtension = require('abstract-extension')`

Imports the AbstractExtension class. You should extend this and add the functionality you need.

#### `abstractExtension.destroy()`

Detroy an extension instance. Removes the message from the local pairing instance.

#### `abstractExtension.id`

The local id of the message. Send this over the wire instead of the message name after exchanging the initial message names.

#### `const bool = abstractExtension.remoteSupports()`

True if the remote also supports this message. Note that nothing bad will having from sending a message the remote does not support.

#### `const buffer = abstractExtension.encode(message)`

Encode a message to a buffer based on the message encoding.

#### `const local = AbstractExtension.createLocal(handlers)`

Create a local message pairing instance.

Whenever the messages are updated `local.onextensionupdate()` will be called if provided.

#### `const msg = local.add(name, handlers)`

Add a new message. `name` should be the string name of a message.

* `handlers.encoding` is an optional encoding for the message payload. Can be either `json`, `utf-8`, `binary` or any abstract encoding.
* `handlers.onmessage(message, context)` is called when a message has been received and pairing.
* `handlers.onerror(error, context)` is called when a message fails to decode.

#### `const list = local.names()`

Returns a sorted list of message names. You need to pass this to another remote pairing instance somehow.

#### `const remote = local.remote()`

Call this to setup remote pairing.

#### `remote.update(localNames)`

Pass the names of another instance to setup the pairing

#### `remote.onmessage(id, message, [context])`

Pair the remote id with the corresponding local message and call the onmessage handler.
Optionally pass a context object that is simply passed along to the `message.onmessage` function

## License

MIT
