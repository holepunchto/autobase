const { Writable, Readable } = require('streamx')

class WriteStream extends Writable {
  constructor (feed, opts) {
    super()

    this.feed = feed
    this.maxBlockSize = (opts && opts.maxBlockSize) || 0
  }

  _writev (batch, cb) {
    this.feed.append(this.maxBlockSize ? this._ensureMaxSize(batch) : batch, cb)
  }

  _ensureMaxSize (batch) {
    for (let i = 0; i < batch.length; i++) {
      let blk = batch[i]
      if (blk.length > this.maxBlockSize) {
        const chunked = []
        while (blk.length > this.maxBlockSize) {
          chunked.push(blk.slice(0, this.maxBlockSize))
          blk = blk.slice(this.maxBlockSize)
        }
        if (blk.length) chunked.push(blk)
        batch.splice(i, 1, ...chunked)
        i += chunked.length - 1
      }
    }
    return batch
  }
}

class ReadStream extends Readable {
  constructor (feed, opts = {}) {
    super()

    this.feed = feed
    this.start = opts.start || 0
    this.end = typeof opts.end === 'number' ? opts.end : -1
    this.live = !!opts.live
    this.snapshot = opts.snapshot !== false
    this.tail = !!opts.tail
    this.index = this.start
    this.options = { wait: opts.wait !== false, ifAvailable: !!opts.ifAvailable, valueEncoding: opts.valueEncoding }
  }

  _open (cb) {
    this.feed.ready((err) => {
      if (err) return cb(err)
      if (this.end === -1) {
        if (this.live) this.end = Infinity
        else if (this.snapshot) this.end = this.feed.length
        if (this.start > this.end) this.push(null)
      }
      if (this.tail) this.start = this.feed.length
      this.index = this.start
      cb(null)
    })
  }

  _read (cb) {
    if (this.index === this.end || (this.end === -1 && this.index >= this.feed.length)) {
      this.push(null)
      return cb(null)
    }
    this.feed.get(this.index++, this.options, (err, block) => {
      if (err) return cb(err)
      this.push(block)
      cb(null)
    })
  }
}

module.exports = { WriteStream, ReadStream }
