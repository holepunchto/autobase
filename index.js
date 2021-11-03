const streamx = require('streamx')
const lock = require('mutexify/promise')
const cenc = require('compact-encoding')
const codecs = require('codecs')

const RebasedHypercore = require('./lib/rebase')
const { InputNode, IndexNode } = require('./lib/nodes')
const { Header } = require('./lib/messages')

const INPUT_TYPE = '@autobase/input'

module.exports = class Autobase {
  constructor (inputs, opts = {}) {
    this.inputs = null
    this.defaultIndexes = null
    this.defaultInput = null

    this._inputs = inputs || []
    this._defaultIndexes = opts.indexes
    this._defaultInput = opts.input
    this._autocommit = opts.autocommit
    this._lock = lock()
    this._inputsByKey = null
    this._defaultIndexesByKey = null
    this._rebasersWithDefaultIndexes = []
    this._readStreams = []

    this._onappend = this._bumpReadStreams.bind(this)

    this.opened = false
    this._opening = this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  async _open () {
    this._validateInputs()

    this.defaultInput = await this._defaultInput
    const inputs = (await this._inputs) || []
    let defaultIndexes = (await this._defaultIndexes) || []

    if (defaultIndexes && !Array.isArray(defaultIndexes)) defaultIndexes = [defaultIndexes]

    await Promise.all(inputs.map(i => i.ready()))
    await Promise.all(defaultIndexes.map(i => i.ready()))

    this._inputsByKey = intoByKeyMap(inputs)
    this._defaultIndexesByKey = intoByKeyMap(defaultIndexes)

    this.inputs = [...this._inputsByKey.values()]
    this.defaultIndexes = [...this._defaultIndexesByKey.values()]

    if (!this.defaultInput) {
      for (const input of this.inputs) {
        if (input.writable) {
          this.defaultInput = input
          break
        }
      }
    }

    for (const input of this.inputs) {
      input.on('append', this._onappend)
    }

    this.opened = true
  }

  // Private Methods

  _validateInputs () {
    for (const input of this._inputs) {
      this._validateInput(input)
    }
  }

  _validateInput (input) {
    if (input.valueEncoding && input.valueEncoding !== codecs.binary) {
      throw new Error('Hypercore inputs must be binary ones')
    }
  }

  async _getInputNode (input, seq, opts = {}) {
    if (seq < 1) return null
    if (Buffer.isBuffer(input)) input = input.toString('hex')
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

    if (node.batch[1] === 0) return node

    const batchEnd = await this._getInputNode(input, seq + node.batch[1])

    node.clock = batchEnd.clock
    if (node.seq > 1) node.clock.set(node.id, node.seq - 1)
    else node.clock.delete(node.id)

    return node
  }

  _bumpReadStreams () {
    for (const stream of this._readStreams) {
      stream.bump()
    }
  }

  // Public API

  async heads () {
    if (!this.opened) await this.ready()
    await Promise.all(this.inputs.map(i => i.update()))
    return Promise.all(this.inputs.map(i => this._getInputNode(i, i.length - 1)))
  }

  async latest (inputs) {
    if (!this.opened) await this.ready()
    if (!inputs) inputs = []
    else inputs = Array.isArray(inputs) ? inputs : [inputs]
    inputs = new Set(inputs.map(i => i.key.toString('hex')))

    const heads = await this.heads()
    const clock = new Map()

    for (const head of heads) {
      if (!head) continue
      if (inputs.size && !inputs.has(head.id)) continue
      clock.set(head.id, this._inputsByKey.get(head.id).length - 1)
    }
    return clock
  }

  async addInput (input) {
    this._validateInput(input)

    if (!this.opened) await this.ready()
    await input.ready()
    const id = input.key.toString('hex')
    if (!this._inputsByKey.has(id)) {
      this.inputs.push(input)
      this._inputsByKey.set(id, input)
      input.on('append', this._onappend)
      this._bumpReadStreams()
    }
  }

  async removeInput (input) {
    if (!this.opened) await this.ready()
    if (typeof input.ready === 'function') await input.ready()
    const id = Buffer.isBuffer(input) ? input.toString('hex') : input.key.toString('hex')
    if (!this._inputsByKey.has(id)) return

    input = this._inputsByKey.get(id)
    const idx = this.inputs.indexOf(input)

    this.inputs.splice(idx, 1)
    this._inputsByKey.delete(id)
    input.removeListener('append', this._onappend)
    this._bumpReadStreams()

    return input
  }

  async addDefaultIndex (index) {
    if (!this.opened) await this.ready()
    await index.ready()

    if (this._defaultIndexesByKey.has(index.key.toString('hex'))) return

    this.defaultIndexes.push(index)
  }

  async removeDefaultIndex (index) {
    if (!this.opened) await this.ready()
    if (typeof index.ready === 'function') await index.ready()
    const id = Buffer.isBuffer(index) ? index.toString('hex') : index.key.toString('hex')
    if (!this._defaultIndexesByKey.has(id)) return

    index = this._defaultIndexesByKey.get(id)
    const idx = this.defaultIndexes.indexOf(index)

    this.defaultIndexes.splice(idx, 1)
    this._defaultIndexesByKey.delete(id)

    return index
  }

  createCausalStream (opts = {}) {
    const self = this
    let heads = null

    return new streamx.Readable({ open, read })

    function open (cb) {
      self.heads()
        .then(h => { heads = h })
        .then(() => cb(null), err => cb(err))
    }

    function read (cb) {
      const { forks, clock, smallest } = forkInfo(heads)

      if (!forks.length) {
        this.push(null)
        return cb(null)
      }

      const node = forks[smallest]
      const forkIndex = heads.indexOf(node)
      this.push(new IndexNode({ ...node, change: node.key, clock }))

      // TODO: When reading a batch node, parallel download them all (use batch[0])
      // TODO: Make a causal stream extension for faster reads

      if (node.seq <= 0) {
        heads.splice(forkIndex, 1)
        return cb(null)
      }

      self._getInputNode(node.id, node.seq - 1).then(next => {
        heads[forkIndex] = next
        return cb(null)
      }, err => cb(err))
    }
  }

  createReadStream (opts = {}) {
    const self = this

    const positionsByKey = new Map()
    const nodesByKey = new Map()
    const snapshotLengthsByKey = new Map()

    let running = false
    let bumped = false

    const stream = new streamx.Readable({
      open: cb => _open().then(cb, cb),
      read: cb => _read().then(cb, cb)
    })
    stream.bump = () => {
      bumped = true
      _read().catch(err => stream.destroy(err))
    }

    this._readStreams.push(stream)
    stream.once('close', () => {
      const idx = this._readStreams.indexOf(stream)
      if (idx === -1) return
      this._readStreams.splice(idx, 1)
    })

    return stream

    async function _open (cb) {
      if (!self.opened) await self.ready()
      await maybeSnapshot()
      await updateAll()
    }

    async function _read () {
      if (running) return
      running = true
      try {
        while (!streamx.Readable.isBackpressured(stream)) {
          await updateAll()
          while (bumped) {
            bumped = false
            await updateAll()
          }

          const oldest = findOldestNode(nodesByKey)

          if (!oldest) {
            if (opts.live) return
            stream.push(null)
            return
          }

          const unsatisfied = hasUnsatisfiedInputs(oldest, self._inputsByKey)
          if (unsatisfied && opts.resolve) {
            const resolved = await opts.resolve(oldest)
            if (resolved !== false) continue
            // If resolved is false, yield the unresolved node as usual
          }

          const pos = positionsByKey.get(oldest.id)
          nodesByKey.delete(oldest.id)
          positionsByKey.set(oldest.id, pos + 1)

          const mapped = opts.map ? opts.map(oldest) : oldest
          stream.push(mapped)

          if (opts.wait) await opts.wait(mapped)
        }
      } finally {
        running = false
      }
    }

    async function maybeSnapshot () {
      for (const [key, input] of self._inputsByKey) {
        await input.update()
        if (!opts.live) snapshotLengthsByKey.set(key, input.length)
      }
    }

    async function updateAll () {
      const allowUpdates = opts.live || opts.resolve || opts.wait // TODO: Make this behavior more customizable
      const inputKeys = allowUpdates ? self._inputsByKey.keys() : snapshotLengthsByKey.keys()
      for (const key of inputKeys) {
        const input = self._inputsByKey.get(key)

        let pos = positionsByKey.get(key)
        if (pos === undefined) {
          pos = 1
          positionsByKey.set(key, pos)
        }

        const snapshotLength = snapshotLengthsByKey.get(key)
        if (snapshotLength) {
          if (pos >= snapshotLength) continue
        } else {
          if (pos >= input.length) await input.update()
          if (pos >= input.length) continue
        }

        nodesByKey.set(key, await self._getInputNode(input, pos))
      }
    }

    function findOldestNode (nodesByKey) {
      for (const node of nodesByKey.values()) {
        if (!node) continue
        let oldest = true
        for (const n2 of nodesByKey.values()) {
          if (!n2 || n2 === node) continue
          if (n2.lte(node)) oldest = false
        }
        if (oldest) return node
      }
      return null
    }

    function hasUnsatisfiedInputs (node, inputsByKey) {
      for (const key of node.clock.keys()) {
        if (!inputsByKey.has(key)) return true
      }
      return false
    }
  }

  async _append (input, value, clock) {
    const head = await this._getInputNode(input, input.length - 1)

    // Make sure that causal information propagates.
    // TODO: This should use an embedded index in the future.
    if (head && head.clock) {
      const inputId = input.key.toString('hex')
      for (const [id, length] of head.clock) {
        if (id === inputId || clock.has(id)) continue
        clock.set(id, length)
      }
    }

    if (!Array.isArray(value)) return input.append(InputNode.encode({ key: input.key, value, clock }))

    const nodes = []
    for (let i = 0; i < value.length; i++) {
      const batch = [i, value.length - 1 - i]
      const node = { key: input.key, value: value[i], batch }
      if (i === value.length - 1) node.clock = clock
      nodes.push(InputNode.encode(node))
    }

    return input.append(nodes)
  }

  async append (value, clock, input) {
    if (clock !== null && !Array.isArray(clock) && !(clock instanceof Map)) {
      input = clock
      clock = null
    }
    if (!this.opened) await this.ready()
    const release = await this._lock()
    input = input || this.defaultInput
    clock = clockToMap(clock || await this.latest())
    try {
      if (!input.length) {
        await input.append(cenc.encode(Header, {
          protocol: INPUT_TYPE
        }))
      }
      return this._append(input, value, clock)
    } finally {
      release()
    }
  }

  createRebasedIndex (indexes, opts = {}) {
    if (isOptions(indexes)) return this.createRebasedIndex(null, indexes)
    indexes = indexes || this.defaultIndexes || this._defaultIndexes || []
    if (opts.autocommit === undefined) {
      opts.autocommit = this._autocommit
    }
    return new RebasedHypercore(this, indexes, opts)
  }

  close () {
    for (const input of this.inputs) {
      input.removeListener('append', this._onappend)
    }
  }

  // For testing
  _decodeInput (input, decodeOpts = {}) {
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
    for (const [key, length] of head.clock) {
      if (length > (high[key] || 0)) high[key] = length
    }
  }

  let s = 0

  for (const [key, length] of node.clock) {
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

function clockToMap (clock) {
  if (!clock) return new Map()
  if (clock instanceof Map) return clock
  if (Array.isArray(clock)) return new Map(clock)
  return new Map(Object.entries(clock))
}

function isOptions (o) {
  return (Object.prototype.toString.call(o) === '[object Object]') && !isHypercore(o)
}

function isHypercore (o) {
  return o.get && o.replicate && o.append
}

function intoByKeyMap (cores) {
  const m = new Map()
  if (!cores) return m
  if (!Array.isArray(cores)) cores = [cores]
  for (const core of cores) {
    const id = core.key.toString('hex')
    if (m.has(id)) continue
    m.set(id, core)
  }
  return m
}

function noop () {}
