const { EventEmitter } = require('events')
const streamx = require('streamx')
const lock = require('mutexify/promise')
const codecs = require('codecs')
const { toPromises } = require('hypercore-promisifier')

const { InputNode, IndexNode } = require('./lib/nodes')
const { Header } = require('./lib/messages')
const Rebaser = require('./lib/rebaser')
const MemoryView = require('./lib/memory-view')

const INPUT_TYPE = '@autobase/input'

module.exports = class Autobase extends EventEmitter {
  constructor (inputs = []) {
    super()
    this.inputs = inputs.map(i => toPromises(i))
    this._inputsByKey = new Map()
    this._lock = lock()
    this._readyProm = null

    for (const core of this.inputs) {
      core.on('append', (...args) => this.emit('input-append', ...args))
    }
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

  async _getInputNode (input, seq, opts = {}) {
    if (seq < 1) return null
    try {
      const block = await input.get(seq)
      if (!block) return null
      const node = InputNode.decode(block)
      node.key = input.key.toString('hex')
      node.seq = seq
      if (node.partial && !opts.allowPartial) {
        while (++seq < input.length) {
          const next = await this._getInputNode(input, seq, { allowPartial: true })
          if (next.partial) continue
          node.clock = next.clock
          break
        }
      }
      return node
    } catch (_) {
      // Decoding errors should be discarded.
      return null
    }
  }

  // Public API

  async heads () {
    await this.ready()
    await Promise.all(this.inputs.map(i => i.update()))
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
      if (!Array.isArray(value)) return input.append(InputNode.encode({ value, links }))
      const nodes = []
      for (let i = 0; i < value.length; i++) {
        const node = { value: value[i] }
        if (i !== value.length - 1) node.partial = true
        else node.links = links
        nodes.push(InputNode.encode(node))
      }
      return input.append(nodes)
    } finally {
      release()
    }
  }

  async remoteRebase (indexes, opts = {}) {
    await Promise.all([this.ready(), ...indexes.map(i => i.ready())])
    await Promise.all([this.ready(), ...indexes.map(i => i.update())])

    const rebasers = indexes.map(i => new Rebaser(opts.wrap !== false ? MemoryView.from(i) : i, opts))

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
      index,
      added: rebaser.added,
      removed: rebaser.removed
    }
  }

  // TODO: Better way to do put this?
  decodeIndex (output, decodeOpts = {}) {
    const get = async (idx, opts) => {
      const block = await output.get(idx, {
        ...opts,
        valueEncoding: null
      })
      const decoded = IndexNode.decode(block)
      if (decodeOpts.includeInputNodes && this._inputsByKey.has(decoded.node.key)) {
        const input = this._inputsByKey.get(decoded.node.key)
        const inputNode = await this._getInputNode(input, decoded.node.seq)
        inputNode.key = decoded.node.key
        inputNode.seq = decoded.node.seq
        decoded.node = inputNode
      }
      if (!decodeOpts.unwrap) return decoded
      let val = decoded.value || decoded.node.value
      if (opts && opts.valueEncoding) {
        if (opts.valueEncoding.decode) val = opts.valueEncoding.decode(val)
        else val = codecs(opts.valueEncoding).decode(val)
      }
      return val
    }
    return new Proxy(output, {
      get (target, prop) {
        if (prop === 'get') return get
        return target[prop]
      }
    })
  }

  decodeInput (input, decodeOpts = {}) {
    const get = async (idx, opts) => {
      const decoded = await this._getInputNode(input, idx)
      if (!decodeOpts.unwrap) return decoded
      return decoded.value
    }
    return new Proxy(input, {
      get (target, prop) {
        if (prop === 'get') return get
        return target[prop]
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
