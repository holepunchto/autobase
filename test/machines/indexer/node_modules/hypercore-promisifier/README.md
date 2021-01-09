# hypercore-promisifier
![Test on Node.js](https://github.com/andrewosh/hypercore-promisifier/workflows/Test%20on%20Node.js/badge.svg)

A wrapper that provides conversion to/from callback/promise interfaces in Hypercore and RemoteHypercore.

## Installation
```
npm i hypercore-promisifier
```

## Usage
```js
const hypercore = require('hypercore')
const ram = require('random-access-memory')
const { toPromises } = require('hypercore-promisifier')

const core = hypercore(ram)

// A promisified Hypercore interface
const wrapper = toPromises(core)
```

## API
The API supports two methods, each one returning a compatibilty wrapper around Hypercore.

#### `const { toCallbacks, toPromises } = require('hypercore-promisifier')`

`toCallbacks(core)` takes a Hypercore-like object with a Promises API, and returns a wrapper with a
callbacks interfaced.

`toPromises(core)` takes a Hypercore-like object with a callbacks API, and returns a wrapper with a
Promises interface.

## License
MIT

