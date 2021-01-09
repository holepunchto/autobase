# simple-message-channels

Simple streamable state machine that implements a useful channel, message-type, message pattern

```
npm install simple-message-channels
```

[![Build Status](https://travis-ci.org/mafintosh/simple-message-channels.svg?branch=master)](https://travis-ci.org/mafintosh/simple-message-channels)

## Usage

``` js
const SMC = require('simple-message-channels')

const a = new SMC({
  onmessage (channel, type, message) {
    console.log('Received a message on channel', channel) // a number
    console.log('Message type is', type) // a number
    console.log('And the message payload was', message) // a buffer
  }
})

const b = new SMC()

// produce a payload
const payload = b.send(0, 1, Buffer.from('hi'))

// somehow send it to the other instance (over a stream etc)
// can arrive chunked as long as it arrives in order
a.recv(payload)
```

(This is the basic wire protocol used by hypercore, https://github.com/mafintosh/hypercore)

## API

#### `payloadBuffer = smc.send(channel, type, message)`

Encode a channel, type, message to be sent to another person.
Channel can be any number and type can be any 4 bit number.
Message should be a buffer.

#### `success = smc.recv(payloadChunk)`

Parse a payload buffer chunk. Once a full message has been parsed
the `smc.onmessage(channel, type, message)` handler is called.

Returns true if the chunk seemed valid and false if not.
If false is returned check `smc.error` to see the error it hit.

#### `payloadBuffer = smc.sendBatch([{ channel, type, message }, ...])`

Encodes a series of messages into a single paylaod buffer.

## License

MIT
