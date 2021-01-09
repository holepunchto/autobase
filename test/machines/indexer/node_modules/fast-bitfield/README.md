# fast-bitfield

A variable sized bitfield (based on [indexed-bitfield](https://github.com/mafintosh/indexed-bitfield)) that allows
you to quickly iterate over the bits.

```
npm install sparse-indexed-bitfield
```

Allocates a series of ~4kb bitfields (when needed) to store the underlying data efficiently.

## Usage

``` js
const bitfield = require('fast-bitfield')

const bits = bitfield()

bits.set(1000, true)
bits.set(1000000000, true)

const ite = bits.iterator()

console.log(ite.next(true)) // 1000
console.log(ite.next(true)) // 1000000000
console.log(ite.next(true)) // -1
```

## API

#### `bits = bitfield()`

Make a new fast bitfield

#### `updated = bits.set(index, bool)`

Set a bit.

Runs in `O(log32(maxBitIndex))` worst case but often `O(1)`

#### `bool = bits.get(index)`

Get a bit.

Runs in `O(log32(maxBitIndex))`

#### `bits.fill(val, [start], [end])`

Set a range of bits efficiently.

#### `iterator = bits.iterator()`

Make a new bit iterator.

#### `iterator.seek(index)`

Move the iterator to start at `index`.

Runs in `O(log32(maxBitIndex))`

#### `index = iterator.next(bit)`

Returns the index of the next `bit` and -1 if none can be found.

Runs in `O(log32(distanceToNextBit))`

## License

MIT
