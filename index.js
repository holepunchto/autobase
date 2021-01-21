const streamx = require('streamx')
const lock = require('mutexify/promise')
const { toPromises } = require('hypercore-promisifier')

const { InputNode, IndexNode } = require('./lib/nodes')
const { Header } = require('./lib/messages')
const Rebaser = require('./lib/rebaser')
const MemCore = require('./lib/memory-hypercore')

const INPUT_TYPE = '@autobase/input'

module.exports = class Autobase {
  constructor (inputs = []) {
    this.inputs = inputs.map(i => toPromises(i))
    this._inputsByKey = new Map()
    this._lock = lock()
    this._readyProm = null
  }

  async _ready () {
    await Promise.all(this.inputs.map(i => i.ready()))
    this._inputsByKey = new Map(this.inputs.map(i => [i.key.toString('hex'), i]))
  }

  async ready () {
    if (this._readyProm) return this._readyProm
    this._readyProm = this._ready()
    return this._readyProm
  }

  // Private Methods

  async _getInputNode (input, seq) {
    if (seq < 1) return null
    try {
      const block = await input.get(seq)
      if (!block) return null
      const node = InputNode.decode(block)
      node.key = input.key.toString('hex')
      node.seq = seq
      return node
    } catch (_) {
      // Decoding errors should be discarded.
      return null
    }
  }

  // Public API

  async heads () {
    await this.ready()
    return Promise.all(this.inputs.map(i => this._getInputNode(i, i.length - 1)))
  }

  async latest (inputs) {
    await this.ready()
    if (!inputs) inputs = []
    else inputs = Array.isArray(inputs) ? inputs : [inputs]
    inputs = new Set(inputs.map(i => i.key.toString('hex')))

    const heads = await this.heads()
    const links = new Map()

    for (const head of heads) {
      if (!head) continue
      if (inputs.size && !inputs.has(head.key)) continue
      links.set(head.key, this._inputsByKey.get(head.key).length - 1)
    }
    return links
  }

  createCausalStream (opts = {}) {
    const self = this
    let heads = null

    const nextNode = async (input, seq) => {
      if (seq === 0) return null
      return this._getInputNode(input, seq - 1)
    }

    return new streamx.Readable({
      open (cb) {
        self.heads()
          .then(h => { heads = h })
          .then(() => cb(null), err => cb(err))
      },
      read (cb) {
        const { forks, clock, smallest } = forkInfo(heads)
        if (!forks.length) {
          this.push(null)
          return cb(null)
        }

        const node = forks[smallest]
        const forkIndex = heads.indexOf(node)
        this.push(new IndexNode({ node, clock }))

        nextNode(self._inputsByKey.get(node.key), node.seq).then(next => {
          if (next) heads[forkIndex] = next
          else heads.splice(forkIndex, 1)
          return cb(null)
        }, err => cb(err))
      }
    })
  }

  async addInput (input) {
    input = toPromises(input)
    const release = await this._lock()
    await this.ready()
    try {
      await input.ready()
      this.inputs.push(input)
      this._inputsByKey.set(input.key.toString('hex'), input)
    } finally {
      release()
    }
  }

  async append (input, value, links) {
    const release = await this._lock()
    await this.ready()
    links = linksToMap(links)
    try {
      if (!input.length) {
        await input.append(Header.encode({
          protocol: INPUT_TYPE
        }))
      }
      return input.append(InputNode.encode({ value, links }))
    } finally {
      release()
    }
  }

  async remoteRebase (indexes, opts = {}) {
    await Promise.all([this.ready(), ...indexes.map(i => i.ready())])

    // If opts is an Array, then the index-specific options will be passed to each rebaser.
    // TODO: Better way to handle this?
    const rebasers = indexes.map((i, idx) => {
      const opt = Array.isArray(opts) ? opts[idx] : opts
      return new Rebaser(opt.wrap !== false ? new MemCore(i) : i, opt)
    })

    let best = null
    for await (const inputNode of this.createCausalStream(opts)) {
      for (const rebaser of rebasers) {
        if (!(await rebaser.update(inputNode))) continue
        best = rebaser
        break
      }
      if (best) break
    }

    if (!best) best = rebasers[0]
    await best.commit()

    return {
      index: best.index,
      added: best.added,
      removed: best.removed
    }
  }

  async localRebase (index, opts = {}) {
    await Promise.all([this.ready(), index.ready()])

    const rebaser = new Rebaser(index, opts)

    for await (const inputNode of this.createCausalStream(opts)) {
      if (await rebaser.update(inputNode)) break
    }
    await rebaser.commit()

    return {
      added: rebaser.added,
      removed: rebaser.removed
    }
  }

  // TODO: Better place to put this?
  static unwrap (output) {
    return new Proxy(output, {
      get (target, prop) {
        if (prop !== 'get') return target[prop]
        return async (idx, opts) => {
          const block = await target.get(idx, {
            ...opts,
            valueEncoding: null
          })
          let val = IndexNode.decode(block).value
          if (opts && opts.valueEncoding) val = opts.valueEncoding.decode(val)
          return val
        }
      }
    })
  }
}

function isFork (head, heads) {
  if (!head) return false
  for (const other of heads) {
    if (!other) continue
    if (head.lte(other)) return false
  }
  return true
}

function forkSize (node, i, heads) {
  const high = {}

  for (const head of heads) {
    if (head === node) continue
    for (const [key, length] of head.links) {
      if (length > (high[key] || 0)) high[key] = length
    }
  }

  let s = 0

  for (const [key, length] of node.links) {
    const h = high[key] || 0
    if (length > h) s += length - h
  }

  return s
}

function forkInfo (heads) {
  const forks = []
  const clock = []

  for (const head of heads) {
    if (!head) continue
    if (isFork(head, heads)) forks.push(head)
    clock.push([head.key, head.seq])
  }

  const sizes = forks.map(forkSize)
  let smallest = 0
  for (let i = 1; i < sizes.length; i++) {
    if (sizes[i] === sizes[smallest] && forks[i].key < forks[smallest].feed) {
      smallest = i
    } else if (sizes[i] < sizes[smallest]) {
      smallest = i
    }
  }

  return {
    forks,
    clock,
    smallest
  }
}

function linksToMap (links) {
  if (!links) return new Map()
  if (links instanceof Map) return links
  if (Array.isArray(links)) return new Map(links)
  return new Map(Object.entries(links))
}
