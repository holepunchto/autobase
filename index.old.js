const streamx = require('streamx')

module.exports = class Autobase {
  constructor (corestore, writers = [], local = null) {
    this.store = corestore
    this.writers = null
    this.local = null

    this._localKey = local
    this._writerKeys = writers
    this._writersByKey = new Map()
  }

  replicate (...opts) {
    return this.store.replicate(...opts)
  }

  ready () {
    if (this.writers) return Promise.resolve()
    return new Promise((resolve, reject) => {
      this.store.ready((err) => {
        if (err) return reject(err)
        if (!this.local) this.local = this._localKey ? this.store.get(this._localKey) : this.store.default()
        this.local.ready((err) => {
          if (err) return reject(err)
          if (this.writers) return resolve()
          this.writers = this._writerKeys.map(k => this.store.get(k))

          for (const w of this.writers) {
            this._writersByKey.set(w.key.toString('hex'), w)
          }
          if (!this._writersByKey.has(this.local.key.toString('hex'))) {
            this.writers.push(this.local)
            this._writersByKey.set(this.local.key.toString('hex'), this.local)
          }
          resolve()
        })
      })
    })
  }

  setWriters (writers) {
    this.writers = null
    this._writerKeys = writers
    this._writersByKey.clear()
  }

  createCausalStream () {
    const self = this
    let heads = null

    return new streamx.Readable({
      open (cb) {
        callbackify(self.heads(), function (err, h) {
          if (err) return cb(err)
          heads = h
          cb(null)
        })
      },
      read (cb) {
        const forks = getForks(heads)
        if (!forks.length) {
          this.push(null)
          return cb(null)
        }

        const sizes = forks.map(forkSize)
        let smallest = 0
        for (let i = 1; i < sizes.length; i++) {
          if (sizes[i] === sizes[smallest] && Buffer.compare(forks[i].feed, forks[smallest].feed) < 0) {
            smallest = i
          } else if (sizes[i] < sizes[smallest]) {
            smallest = i
          }
        }

        const next = forks[smallest]
        const i = heads.indexOf(next)
        const clock = heads.map(h => (h && { feed: h.feed, length: h.seq + 1 })).filter(h => h)

        this.push({ clock, node: next })

        nextNode(self._writersByKey.get(next.feed.toString('hex')), next.seq, function (err, node) {
          if (err) return cb(err)
          if (node) heads[i] = node
          else heads.splice(i, 1)
          cb(null)
        })
      }
    })
  }

  async append (value, links) {
    if (!links) {
      await this.heads()
      links = {}
      for (const w of this.writers) {
        if (w.key.equals(this.local.key)) continue
        links[w.key.toString('hex')] = w.length
      }
    }

    return new Promise((resolve, reject) => {
      this.local.append(JSON.stringify({ value, links }), err => {
        if (err) return reject(err)
        resolve()
      })
    })
  }

  async forks () {
    return getForks(await this.heads())
  }

  async heads () {
    await this.ready()

    let missing = this.writers.length
    if (!missing) return []

    return new Promise((resolve, reject) => {
      const heads = new Array(missing)
      let error = null

      for (let i = 0; i < this.writers.length; i++) {
        head(this.writers[i], i, onhead)
      }

      function onhead (err, node, index) {
        if (err) error = err
        else heads[index] = node
        if (--missing) return
        if (error) return reject(error)
        resolve(heads)
      }
    })
  }
}

function nextNode (w, seq, cb) {
  if (seq === 0) return cb(null, null)
  w.get(seq - 1, function (err, buf) {
    if (err) return cb(err)
    cb(null, new Node(seq - 1, w.key, buf))
  })
}

function head (feed, index, cb) {
  feed.update({ ifAvailable: true }, function () {
    const len = feed.length
    if (!len) return cb(null, null, index)
    const seq = len - 1
    feed.get(seq, function (err, block) {
      if (err) return cb(err)
      cb(null, new Node(seq, feed.key, block), index)
    })
  })
}

class Node {
  constructor (seq, feed, buf) {
    this.seq = seq
    this.feed = feed
    this.decoded = JSON.parse(buf)
    this.value = this.decoded.value
  }

  link (key) {
    if (key.equals(this.feed)) return this.seq + 1
    return this.decoded.links[key.toString('hex')] || 0
  }

  links () {
    return [[this.feed, this.seq + 1]].concat(Object.entries(this.decoded.links).map(bufEntry))
  }
}

function bufEntry ([key, val]) {
  return [Buffer.from(key, 'hex'), val]
}

function isFork (head, heads) {
  if (!head) return false
  for (const other of heads) {
    if (other && other !== head && lt(head.seq, head.feed, other)) return false
  }
  return true
}

function lt (seq, feed, node) {
  return seq < node.link(feed)
}

function getForks (heads) {
  const forks = []

  for (const head of heads) {
    if (isFork(head, heads)) forks.push(head)
  }

  return forks
}

function callbackify (p, cb) {
  p.then(cb.bind(null, null), cb)
}

function forkSize (node, i, heads) {
  const high = {}

  for (const head of heads) {
    if (head === node) continue
    for (const [feed, length] of head.links()) {
      const id = feed.toString('hex')
      if (length > (high[id] || 0)) high[id] = length
    }
  }

  let s = 0

  for (const [feed, length] of node.links()) {
    const h = high[feed.toString('hex')] || 0
    if (length > h) s += length - h
  }

  return s
}
