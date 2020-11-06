const { Readable } = require('streamx')

module.exports = class AutoIndex {
  constructor (filename) {
    this.filename = filename
    this.all = require('fs').existsSync(this.filename) ? JSON.parse(require('fs').readFileSync(filename, 'utf-8')) : []
    this.map = new Map()

    for (let i = 0; i < this.all.length; i++) {
      this.map.set(this._id(this.all[i]), i)
    }
  }

  createReadStream () {
    const self = this
    let i = 0

    return new Readable({
      read (cb) {
        this.push(self.all[i++] || null)
        cb(null)
      }
    })
  }

  async rebase (auto) {
    const buffered = []
    const result = { added: 0, removed: 0 }
    for await (const data of auto.createCausalStream()) {
      const o = this._json(data)
      const id = this._id(o)

      if (this.map.has(id)) {
        if (!buffered.length) return result

        while (true) {
          const i = this._id(this.all[this.all.length - 1])
          if (i === id) {
            while (buffered.length) {
              const next = this._json(buffered.pop())
              this.map.set(this._id(next), this.all.length)
              this.all.push(next)
            }
            await require('fs').promises.writeFile(this.filename, JSON.stringify(this.all))
            return result
          }
          this.all.pop()
          this.map.delete(i)
          result.removed++
        }

        return result
      }

      buffered.push(o)
      result.added++
    }

    const data = JSON.stringify(buffered.reverse())
    await require('fs').promises.writeFile(this.filename, data)

    return result
  }

  _id (node) {
    return node.clock.map(c => c.feed + '@' + c.length).join(':')
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
}
