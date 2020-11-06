const { Readable } = require('streamx')

module.exports = class Rebaser {
  constructor (feed) {
    this.feed = feed
    this._map = new Map()
    this._needsIndex = true
  }

  _index () {
    return new Promise(resolve => {
      let seq = 0
      this.feed.createReadStream()
        .on('data', (data) => {
          this._map.set(this._id(data), seq++)
        })
        .on('end', () => {
          this._needsIndex = false
          resolve()
        })
    })
  }

  _id (node) {
    return node.clock.map(c => c.feed.toString('hex') + '@' + c.length).join(':')
  }

  _json (n) {
    return {
      clock: n.clock.map(function (c) {
        return {
          feed: c.feed.toString('hex'),
          length: c.length
        }
      }).sort((a, b) => a < b ? -1 : a > b ? 1 : 0),
      node: {
        feed: n.node.feed.toString('hex'),
        seq: n.node.seq,
        value: n.node.value
      }
    }
  }

  createReadStream () {
    const feed = this.feed
    let seq = 0

    return new Readable({
      read (cb) {
        if (seq === feed.length) {
          this.push(null)
          return cb()
        }

        feed.get(seq++, (err, data) => {
          if (err) return cb(err)
          this.push(data)
          cb(null)
        })
      }
    })
  }

  get (seq) {
    return new Promise((resolve, reject) => {
      this.feed.get(seq, function (err, val) {
        if (err) return reject(err)
        resolve(val)
      })
    })
  }

  append (data) {
    return new Promise((resolve, reject) => {
      this.feed.append(data, function (err) {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  truncate (len) {
    return new Promise((resolve, reject) => {
      this.feed.truncate(len, function (err) {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  async rebase (auto) {
    if (this._needsIndex) await this._index()

    const buffered = []
    const result = { added: 0, removed: 0 }

    for await (const data of auto.createCausalStream()) {
      const o = this._json(data)
      const id = this._id(o)

      if (this._map.has(id)) {
        if (!buffered.length) return result

        while (true) {
          const i = this._id(await this.get(this.feed.length - 1))
          if (i === id) {
            while (buffered.length) {
              const next = this._json(buffered.pop())
              this._map.set(this._id(next), this.feed.length)
              await this.append(next)
            }

            return result
          }
          await this.truncate(this.feed.length - 1)
          this._map.delete(i)
          result.removed++
        }

        return result
      }

      buffered.push(o)
      result.added++
    }

    await this.append(buffered.reverse())
    return result

  }
}
