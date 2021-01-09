const DEFAULT_MAX_BYTE_SIZE = 1024 * 1024 * 16

class NamespacedCache {
  constructor (parent, name) {
    this.name = name
    this.parent = parent
  }

  get _info () {
    return this.parent._info
  }

  set (key, value) {
    return this.parent._set(this.name, key, value)
  }

  del (key) {
    return this.parent._del(this.name, key)
  }

  get (key) {
    return this.parent._get(this.name, key)
  }
}

module.exports = class HypercoreCache {
  constructor (opts = {}) {
    this.maxByteSize = opts.maxByteSize || DEFAULT_MAX_BYTE_SIZE
    this.onEvict = opts.onEvict
    this.estimateSize = opts.estimateSize || defaultSize

    this._nextNamespace = 0
    this.defaultCache = new NamespacedCache(this, this._nextNamespace++)

    this._stale = null
    this._fresh = new Map()
    this._freshByteSize = 0
    this._staleByteSize = 0
  }

  get _info () {
    return {
      freshByteSize: this._freshByteSize,
      staleByteSize: this._staleByteSize,
      staleEntries: this._stale ? this._stale.size : 0,
      freshEntries: this._fresh.size,
      byteSize: this.byteSize
    }
  }

  _prefix (namespace, key) {
    return namespace + ':' + key
  }

  _gc () {
    if (this.onEvict && this._staleByteSize > 0) this.onEvict(this._stale)
    this._stale = this._fresh
    this._fresh = new Map()
    this._staleByteSize = this._freshByteSize
    this._freshByteSize = 0
  }

  _get (namespace, key, prefixedKey) {
    if (!prefixedKey) prefixedKey = this._prefix(namespace, key)
    return this._fresh.get(prefixedKey) || (this._stale && this._stale.get(prefixedKey))
  }

  _set (namespace, key, value) {
    const valueSize = this.estimateSize(value)
    const prefixedKey = this._prefix(namespace, key)
    if (this._freshByteSize + valueSize > this.maxByteSize) {
      this._gc()
    }
    this._fresh.set(prefixedKey, value)
    this._freshByteSize += valueSize
  }

  _del (namespace, key) {
    const prefixedKey = this._prefix(namespace, key)
    let val = this._stale && this._stale.get(prefixedKey)
    if (val) {
      this._stale.delete(prefixedKey)
      this._staleByteSize -= this.estimateSize(val)
    }
    val = this._fresh.get(prefixedKey)
    if (val) {
      this._fresh.delete(prefixedKey)
      this._freshByteSize -= this.estimateSize(val)
    }
  }

  get byteSize () {
    return this._freshByteSize + this._staleByteSize
  }

  namespace () {
    const cache = new NamespacedCache(this, this._nextNamespace++)
    return cache
  }

  set (key, value) {
    return this.defaultCache.set(key, value)
  }

  del (key) {
    return this.defaultCache.del(key)
  }

  get (key) {
    return this.defaultCache.get(key)
  }
}

function defaultSize () {
  // Return an estimate of the object overhead, without being clever here.
  // (You should pass in a size estimator)
  return 1024
}
