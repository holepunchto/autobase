const { EventEmitter } = require('events')
const maybe = require('call-me-maybe')
const inspect = require('inspect-custom-symbol')

const SUPPORTS_PROMISES = Symbol.for('hypercore.promises')
const CORE = Symbol('hypercore-promisifier.core')
const REQUEST = Symbol('hypercore-promisifier.request')

class BaseWrapper extends EventEmitter {
  constructor (core) {
    super()
    this[CORE] = core
    this.on('newListener', (eventName, listener) => {
      core.on(eventName, listener)
    })
    this.on('removeListener', (eventName, listener) => {
      core.removeListener(eventName, listener)
    })
  }

  [inspect] (depth, opts) {
    return this[CORE][inspect](depth, opts)
  }

  get key () {
    return this[CORE].key
  }

  get discoveryKey () {
    return this[CORE].discoveryKey
  }

  get length () {
    return this[CORE].length
  }

  get byteLength () {
    return this[CORE].byteLength
  }

  get writable () {
    return this[CORE].writable
  }

  get sparse () {
    return this[CORE].sparse
  }

  get peers () {
    return this[CORE].peers
  }

  get valueEncoding () {
    return this[CORE].valueEncoding
  }

  get weak () {
    return this[CORE].weak
  }

  get lazy () {
    return this[CORE].lazy
  }
}

class CallbackToPromiseHypercore extends BaseWrapper {
  constructor (core) {
    super(core)
    this[SUPPORTS_PROMISES] = true
  }

  // Async Methods

  ready () {
    return alwaysCatch(new Promise((resolve, reject) => {
      this[CORE].ready(err => {
        if (err) return reject(err)
        return resolve(null)
      })
    }))
  }

  close () {
    return alwaysCatch(new Promise((resolve, reject) => {
      this[CORE].close(err => {
        if (err) return reject(err)
        return resolve(null)
      })
    }))
  }

  get (index, opts) {
    let req = null
    const prom = new Promise((resolve, reject) => {
      req = this[CORE].get(index, opts, (err, block) => {
        if (err) return reject(err)
        return resolve(block)
      })
    })
    prom[REQUEST] = req
    return prom
  }

  append (batch) {
    return alwaysCatch(new Promise((resolve, reject) => {
      this[CORE].append(batch, (err, seq) => {
        if (err) return reject(err)
        return resolve(seq)
      })
    }))
  }

  update (opts) {
    return alwaysCatch(new Promise((resolve, reject) => {
      this[CORE].update(opts, err => {
        if (err) return reject(err)
        return resolve(null)
      })
    }))
  }

  seek (bytes, opts) {
    return new Promise((resolve, reject) => {
      this[CORE].seek(bytes, opts, (err, index, relativeOffset) => {
        if (err) return reject(err)
        return resolve([index, relativeOffset])
      })
    })
  }

  download (range) {
    let req = null
    const prom = alwaysCatch(new Promise((resolve, reject) => {
      req = this[CORE].download(range, err => {
        if (err) return reject(err)
        return resolve(null)
      })
    }))
    prom[REQUEST] = req
    return prom
  }

  has (start, end) {
    return new Promise((resolve, reject) => {
      this[CORE].has(start, end, (err, res) => {
        if (err) return reject(err)
        return resolve(res)
      })
    })
  }

  audit () {
    return new Promise((resolve, reject) => {
      this[CORE].audit((err, report) => {
        if (err) return reject(err)
        return resolve(report)
      })
    })
  }

  destroyStorage () {
    return new Promise((resolve, reject) => {
      this[CORE].destroyStorage(err => {
        if (err) return reject(err)
        return resolve(null)
      })
    })
  }

  // Sync Methods

  createReadStream (opts) {
    return this[CORE].createReadStream(opts)
  }

  createWriteStream (opts) {
    return this[CORE].createWriteStream(opts)
  }

  undownload (range) {
    return this[CORE].undownload(range[REQUEST] || range)
  }

  cancel (range) {
    return this[CORE].cancel(range[REQUEST] || range)
  }

  replicate (initiator, opts) {
    return this[CORE].replicate(initiator, opts)
  }

  registerExtension (name, handlers) {
    return this[CORE].registerExtension(name, handlers)
  }

  setUploading (uploading) {
    return this[CORE].setUploading(uploading)
  }

  setDownloading (downloading) {
    return this[CORE].setDownloading(downloading)
  }
}

class PromiseToCallbackHypercore extends BaseWrapper {
  constructor (core) {
    super(core)
    this[SUPPORTS_PROMISES] = false
  }

  // Async Methods

  ready (cb) {
    return maybeOptional(cb, this[CORE].ready())
  }

  close (cb) {
    return maybeOptional(cb, this[CORE].close())
  }

  get (index, opts, cb) {
    const prom = this[CORE].get(index, opts)
    maybe(cb, prom)
    return prom
  }

  append (batch, cb) {
    return maybeOptional(cb, this[CORE].append(batch))
  }

  update (opts, cb) {
    return maybeOptional(cb, this[CORE].update(opts))
  }

  seek (bytes, opts, cb) {
    return maybe(cb, this[CORE].seek(bytes, opts))
  }

  download (range, cb) {
    const prom = this[CORE].download(range)
    maybeOptional(cb, prom)
    return prom
  }

  has (start, end, cb) {
    return maybe(cb, this[CORE].has(start, end))
  }

  audit (cb) {
    return maybe(cb, this[CORE].audit())
  }

  destroyStorage (cb) {
    return maybe(cb, this[CORE].destroyStorage())
  }

  // Sync Methods

  createReadStream (opts) {
    return this[CORE].createReadStream(opts)
  }

  createWriteStream (opts) {
    return this[CORE].createWriteStream(opts)
  }

  undownload (range) {
    return this[CORE].undownload(range)
  }

  cancel (range) {
    return this[CORE].cancel(range)
  }

  replicate (initiator, opts) {
    return this[CORE].replicate(initiator, opts)
  }

  registerExtension (name, handlers) {
    return this[CORE].registerExtension(name, handlers)
  }

  setUploading (uploading) {
    return this[CORE].setUploading(uploading)
  }

  setDownloading (downloading) {
    return this[CORE].setDownloading(downloading)
  }
}

module.exports = {
  toPromises,
  toCallbacks
}

function toPromises (core) {
  return core[SUPPORTS_PROMISES] ? core : new CallbackToPromiseHypercore(core)
}

function toCallbacks (core) {
  return core[SUPPORTS_PROMISES] ? new PromiseToCallbackHypercore(core) : core
}

function maybeOptional (cb, prom) {
  prom = maybe(cb, prom)
  if (prom) prom.catch(noop)
  return prom
}

function alwaysCatch (prom) {
  prom.catch(noop)
  return prom
}

function noop () {}
