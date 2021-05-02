const streamx = require('streamx')
const lock = require('mutexify/promise')
const cenc = require('compact-encoding')

const Rebaser = require('./lib/rebaser')
const MemoryView = require('./lib/views/memory')
const { InputNode, IndexNode } = require('./lib/nodes')
const { Header } = require('./lib/messages')

const INPUT_TYPE = '@autobase/input'
const INDEX_TYPE = '@autobase/index'

module.exports = class AutobaseCore {
  constructor (inputs) {
    this.inputs = inputs

    this._lock = lock()
    this._inputsByKey = null

    this._opening = this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  async _open () {
    await Promise.all(this.inputs.map(i => i.ready()))
    this._inputsByKey = new Map(this.inputs.map(i => [i.key.toString('hex'), i]))
    this._opening = null
  }

  // Private Methods

  async _getInputNode (input, seq, opts = {}) {
    if (seq < 1) return null
    if (typeof input === 'string') {
      if (!this._inputsByKey.has(input)) return null
      input = this._inputsByKey.get(input)
    }

    const block = await input.get(seq)
    if (!block) return null

    let node = null
    try {
      node = InputNode.decode(block, { key: input.key, seq })
    } catch (_) {
      // Decoding errors should be discarded.
      return null
    }
    if (!node.batch || opts.allowPartial) return node

    let batchEnd = node
    while (++seq < input.length) {
      const next = await this._getInputNode(input, seq, { allowPartial: true })
      if (next.batch === node.batch) {
        batchEnd = next
        continue
      }
      break
    }
    node.links = batchEnd.links
    if (node.seq > 1) node.links.set(node.id, node.seq - 1)
    else node.links.delete(node.id)

    return node
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
      if (inputs.size && !inputs.has(head.id)) continue
      links.set(head.id, this._inputsByKey.get(head.id).length - 1)
    }
    return links
  }

  createCausalStream (opts = {}) {
    const self = this
    let heads = null

    const open = function (cb) {
      self.heads()
        .then(h => { heads = h })
        .then(() => cb(null), err => cb(err))
    }

    const read = function (cb) {
      const { forks, clock, smallest } = forkInfo(heads)

      if (!forks.length) {
        this.push(null)
        return cb(null)
      }

      const node = forks[smallest]
      const forkIndex = heads.indexOf(node)
      this.push(new IndexNode({ node, clock }))

      if (node.seq <= 0) {
        heads.splice(forkIndex, 1)
        return cb(null)
      }

      self._getInputNode(node.id, node.seq - 1).then(next => {
        heads[forkIndex] = next
        return cb(null)
      }, err => cb(err))
    }

    return new streamx.Readable({ open, read })
  }

  async _append (input, value, links) {
    const head = await this._getInputNode(input, input.length - 1)

    // Make sure that causal information propagates.
    // TODO: This should use an embedded index in the future.
    if (head && head.links) {
      const inputId = input.key.toString('hex')
      for (const [id, length] of head.links) {
        if (id === inputId || links.has(id)) continue
        links.set(id, length)
      }
    }

    if (!Array.isArray(value)) return input.append(InputNode.encode({ key: input.key, value, links }))

    const nodes = []
    const batchId = input.length
    for (let i = 0; i < value.length; i++) {
      const node = { key: input.key, value: value[i], batch: batchId }
      if (i === value.length - 1) node.links = links
      nodes.push(InputNode.encode(node))
    }

    return input.append(nodes)
  }

  async append (input, value, links) {
    const release = await this._lock()
    await this.ready()
    links = linksToMap(links)
    try {
      if (!input.length) {
        await input.append(cenc.encode(Header, {
          protocol: INPUT_TYPE
        }))
      }
      return this._append(input, value, links)
    } finally {
      release()
    }
  }

  async rebasedView (indexes, opts = {}) {
    if (!Array.isArray(indexes)) indexes = [indexes]

    await Promise.all([this.ready(), ...indexes.map(i => i.update())])

    const rebasers = []
    if (!opts.view) {
      for (const index of indexes) {
        const view = (index instanceof MemoryView)
          ? index
          : new MemoryView(this, index)
        rebasers.push(new Rebaser(view, opts))
      }
    } else {
      rebasers.push(new Rebaser(opts.view, opts))
    }

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
    await best.commit({ flush: false })

    return {
      index: best.index,
      added: best.added,
      removed: best.removed
    }
  }

  async rebaseInto (index, opts = {}) {
    if (!index && opts.view) index = opts.view
    await Promise.all([this.ready(), index.ready()])

    if (!(index instanceof MemoryView)) {
      if (!index.length) {
        await index.append(cenc.encode(Header, {
          protocol: INDEX_TYPE
        }))
      }
      index = new MemoryView(this, index)
    }

    const rebaser = new Rebaser(index, opts)

    for await (const inputNode of this.createCausalStream(opts)) {
      if (await rebaser.update(inputNode)) break
    }
    await rebaser.commit()

    return {
      index: new MemoryView(this, index, {
        ...opts,
        readonly: true,
        unwrap: !!opts.unwrap,
        includeInputNodes: opts.includeInputNodes !== false
      }),
      added: rebaser.added,
      removed: rebaser.removed
    }
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
  const clock = new Map()

  for (const head of heads) {
    if (!head) continue
    if (isFork(head, heads)) forks.push(head)
    clock.set(head.id, head.seq)
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

function noop () {}
