# hypercore-cache
[![Build Status](https://travis-ci.com/andrewosh/hypercore-cache.svg?token=WgJmQm3Kc6qzq1pzYrkx&branch=master)](https://travis-ci.com/andrewosh/hypercore-cache)

A caching module for Hypercore tree nodes and blocks.

This module mplements the [hashlru](https://github.com/dominictarr/hashlru) algorithm internally for LRU caching, but it uses byte length estimates instead of the entry count for eviction.

### Installation
```
npm i hypercore-cache --save
```

### API

#### `const cache = new HypercoreCache(opts = {})`
Creates a new cache.

Options can include:
```js
{
  maxByteSize: 1e6,  // The soft maximum size of the cache (actual max size can go up to 2x this value).
  estimateSize: val => { ... },  // A function from a value to its size estimate.
  onEvict: (evicted) => { ... }  // A hook that's called when stale entries (a Map) have been evicted.
}
```

#### `cache.set(key, value)`
Sets `key` to `value`.

#### `cache.get(key)`
Gets the value for `key`.

#### `cache.del(key)`
Deletes `key` from the cache.

#### `const subCache = cache.namespace()`
Creates a namespaced sub-cache which mirrors the hypercore-cache API.

This is useful if you want to create a single cache instance that manages resources for multiple hypercores.

### License
MIT
