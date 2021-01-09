# chacha20-universal

Chacha20 implemented in Javascript.

```
npm install chacha20-universal
```

## Usage

``` js
var crypto = require('crypto')
var Chacha20 = require('chacha20')

var key = crypto.randomBytes(32)
var nonce = crypto.randomBytes(24)
var out = Buffer.alloc(5)
var xor = new Chacha20(nonce, key)

xor.update(out, Buffer.from('hello'))
xor.update(out, Buffer.from('world'))

console.log(out)
// e.g. <Buffer 7c 77 23 51 f9>

xor.finalize()
```

## API

#### `var xor = chacha20(nonce, key, [counter])`

Create a new xor instance.

`nonce` should be a 12 byte buffer/uint8array and `key` should be 32 byte. An optional `counter` may be passed as a number.

#### `xor.update(output, input)`

Update the xor instance with a new `input` buffer, the result is written to `output` buffer. `output` should be the same byte length as `input`.

#### `xor.final()`

Call this method last. Clears internal state.

## License

MIT
