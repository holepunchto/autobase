const streamx = require('streamx')
const lock = require('mutexify/promise')
const { toPromises } = require('hypercore-promisifier')

const { InputNode, OutputNode } = require('./lib/nodes')

module.exports = class Autobase {
  constructor(inputs = []) {
    this.inputs = inputs.map(i => toPromises(i))
    this._inputsByKey = new Map()
    this._lock = lock()
  }

  async ready() {
    await Promise.all(this.inputs.map(i => i.ready()))
    this._inputsByKey = new Map(this.inputs.map(i => [i.key.toString('hex'), i]))
  }

  // Private Methods

  async _getInputNode(input, seq) {
    if (seq < 0) return null
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

  heads() {
    return Promise.all(this.inputs.map(i => this._getInputNode(i, i.length - 1)))
  }

  async latest (inputs) {
    inputs = Array.isArray(inputs) ? inputs: [inputs]
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

  createCausalStream(opts = {}) {
    const self = this
    let heads = null

    const nextNode = async (input, seq) => {
      if (seq === 0) return null
      return this._getInputNode(input, seq - 1)
    }

    return new streamx.Readable({
      open(cb) {
        self.heads()
          .then(h => { heads = h })
          .then(() => cb(null), err => cb(err))
      },
      read(cb) {
        const { forks, clock, smallest } = forkInfo(heads)
        if (!forks.length) {
          this.push(null)
          return cb(null)
        }

        const node = forks[smallest]
        const forkIndex = heads.indexOf(node)
        this.push(new OutputNode({ node, clock }))

        nextNode(self._inputsByKey.get(node.key), node.seq).then(next => {
          if (next) heads[forkIndex] = next
          else heads.splice(forkIndex, 1)
          return cb(null)
        }, err => cb(err))
      }
    })
  }

  async addInput(input) {
    input = toPromises(input)
    const release = await this._lock()
    try {
      await input.ready()
      this.inputs.push(toPromises(input))
      this._inputsByKey.set(input.key.toString('hex'), input)
    } finally {
      release()
    }
  }

  async append(input, value, links) {
    const release = await this._lock()
    links = linksToMap(links)
    try {
      return input.append(InputNode.encode({ value, links }))
    } finally {
      release()
    }
  }

  async rebase (index, opts = {}) {
    await index.ready()

    // TODO: This should store buffered nodes in a temp file.
    const buf = []
    const result = { added: 0, removed: 0 }

    const getIndexHead = async () => {
      if (index.length <= 0) return null
      return OutputNode.decode(await index.get(index.length - 1))
    }

    for await (const inputNode of this.createCausalStream(opts)) {
      if (!index.length) {
        result.added++
        buf.push(inputNode)
        continue
      }

      let indexNode = await getIndexHead()

      if (inputNode.lte(indexNode) && inputNode.clock.size === indexNode.clock.size) {
        break
      }

      let popped = false
      while (indexNode && indexNode.contains(inputNode) && !popped) {
        popped = indexNode.equals(inputNode)
        result.removed++
        await index.truncate(index.length - 1)
        indexNode = await getIndexHead()
      }

      result.added++
      buf.push(inputNode)
    }

    while (buf.length) {
      await index.append(OutputNode.encode(buf.pop()))
    }

    return result
  }
}

function isFork(head, heads) {
  if (!head) return false
  for (const other of heads) {
    if (!other) continue
    if (head.lte(other)) return false
  }
  return true
}

function forkSize(node, i, heads) {
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

function forkInfo(heads) {
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

function debugIndexNode(outputNode) {
  if (!outputNode) return null
  return {
    value: outputNode.node.value.toString('utf8'),
    key: outputNode.node.key,
    seq: outputNode.node.seq,
    links: outputNode.node.links,
    clock: outputNode.clock
  }
}

function linksToMap (links) {
  if (!links) return new Map()
  if (links instanceof Map) return links
  if (Array.isArray(links)) return new Map(links)
  return new Map(Object.entries(links))
}
