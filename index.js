const streamx = require('streamx')
const codecs = require('codecs')
const debounce = require('debounceify')
const c = require('compact-encoding')

const LinearizedView = require('./lib/linearize')
const { InputNode, OutputNode } = require('./lib/nodes')
const { NodeHeader } = require('./lib/nodes/messages')

const INPUT_PROTOCOL = '@autobase/input/v1'
const OUTPUT_PROTOCOL = '@autobase/output/v1'

module.exports = class Autobase {
  constructor (inputs, opts = {}) {
    this.inputs = null
    this.defaultOutputs = null
    this.defaultInput = null
    this._clock = null

    this._autocommit = opts.autocommit

    this._inputs = inputs || []
    this._defaultInput = opts.input
    this._inputsByKey = null
    this._onappend = debounce(this._onInputAppended.bind(this))

    this._defaultOutputs = opts.outputs
    this._defaultOutputsByKey = null
    this._rebasersWithDefaultOutputs = []

    this._readStreams = []

    this.opened = false
    this._closing = null
    this._opening = this._open()

    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  async _open () {
    this.defaultInput = await this._defaultInput
    const inputs = (await this._inputs) || []
    let defaultOutputs = (await this._defaultOutputs) || []

    if (defaultOutputs && !Array.isArray(defaultOutputs)) defaultOutputs = [defaultOutputs]

    await Promise.all(inputs.map(i => i.ready()))
    await Promise.all(defaultOutputs.map(o => o.ready()))
    this._validateInputs()

    if (!this.defaultInput) {
      for (const input of inputs) {
        if (input.writable) {
          this.defaultInput = input
          break
        }
      }
    }

    this._inputsByKey = intoByKeyMap(inputs)
    this._defaultOutputsByKey = intoByKeyMap(defaultOutputs)

    this.inputs = [...this._inputsByKey.values()]
    this.defaultOutputs = [...this._defaultOutputsByKey.values()]

    for (const input of this.inputs) {
      input.on('append', this._onappend)
    }

    this.opened = true
  }

  // Private Methods

  _createInputBatch (input, batch, clock) {
    const nodes = []
    for (let i = 0; i < batch.length; i++) {
      const batchOffset = (batch.length !== 1) ? [i, batch.length - 1 - i] : null
      const node = { key: input.key, value: batch[i], batch: batchOffset }
      if (i === batch.length - 1) node.clock = clock
      nodes.push(node)
    }
    if (input.length === 0) {
      nodes[0].header = {
        protocol: INPUT_PROTOCOL
      }
    }
    return nodes.map(InputNode.encode)
  }

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

  async _getInputNode (input, seq, opts) {
    if (seq < 0) return null
    if (Buffer.isBuffer(input)) input = input.toString('hex')
    if (typeof input === 'string') {
      if (!this._inputsByKey.has(input)) return null
      input = this._inputsByKey.get(input)
    }

    const block = await input.get(seq, opts)
    if (!block) return null

    let node = null
    try {
      node = InputNode.decode(block, { key: input.key, seq })
    } catch {
      // Decoding errors should be discarded.
      return null
    }

    if (node.batch[1] === 0) return node

    const batchEnd = await this._getInputNode(input, seq + node.batch[1])

    node.clock = batchEnd.clock
    if (node.seq > 0) node.clock.set(node.id, node.seq - 1)
    else node.clock.delete(node.id)

    return node
  }

  async _onInputAppended () {
    this._bumpReadStreams()
    this._getLatestClock() // Updates this._clock
  }

  _bumpReadStreams () {
    for (const stream of this._readStreams) {
      stream.bump()
    }
  }

  // Public API

  clock () {
    return this._clock
  }

  async heads () {
    if (!this.opened) await this.ready()
    await Promise.all(this.inputs.map(i => i.update()))
    return Promise.all(this.inputs.map(i => this._getInputNode(i, i.length - 1)))
  }

  _getLatestClock (inputs) {
    if (!inputs) inputs = this.inputs
    if (!Array.isArray(inputs)) inputs = [inputs]

    const clock = new Map()
    for (const input of inputs) {
      const id = input.key.toString('hex')
      if (!this._inputsByKey.has(id)) throw new Error('Hypercore is not an input of the Autobase')
      if (input.length === 0) continue
      clock.set(id, input.length - 1)
    }

    this._clock = clock
    return clock
  }

  async latest (inputs) {
    if (!this.opened) await this.ready()
    await Promise.all(this.inputs.map(i => i.update()))
    return this._getLatestClock(inputs)
  }

  async addInput (input) {
    if (!this.opened) await this.ready()
    await input.ready()
    this._validateInput(input)

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

  async addDefaultOutput (output) {
    if (!this.opened) await this.ready()
    await output.ready()

    if (this._defaultOutputsByKey.has(output.key.toString('hex'))) return

    this.defaultOutputs.push(output)
  }

  async removeDefaultOutput (output) {
    if (!this.opened) await this.ready()
    if (typeof output.ready === 'function') await output.ready()
    const id = Buffer.isBuffer(output) ? output.toString('hex') : output.key.toString('hex')
    if (!this._defaultOutputsByKey.has(id)) return

    output = this._defaultOutputsByKey.get(id)
    const idx = this.defaultOutputs.indexOf(output)

    this.defaultOutputs.splice(idx, 1)
    this._defaultOutputsByKey.delete(id)

    return output
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
      this.push(new OutputNode({ ...node, change: node.key, clock }))

      // TODO: When reading a batch node, parallel download them all (use batch[0])
      // TODO: Make a causal stream extension for faster reads

      if (node.seq === 0) {
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

    const positionsByKey = opts.checkpoint || new Map()
    const nodesByKey = new Map()
    const snapshotLengthsByKey = new Map()

    const wait = opts.wait !== false
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
    stream.checkpoint = positionsByKey

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
          if (unsatisfied && opts.onresolve) {
            const resolved = await opts.onresolve(oldest)
            if (resolved !== false) continue
            // If resolved is false, yield the unresolved node as usual
          }

          const pos = positionsByKey.get(oldest.id)
          nodesByKey.delete(oldest.id)
          positionsByKey.set(oldest.id, pos + 1)

          const mapped = opts.map ? opts.map(oldest) : oldest
          stream.push(mapped)

          if (opts.onwait) await opts.onwait(mapped)
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
      const allowUpdates = opts.live || opts.onresolve || opts.onwait // TODO: Make this behavior more customizable
      const inputKeys = allowUpdates ? self._inputsByKey.keys() : snapshotLengthsByKey.keys()

      const loadPromises = []
      for (const key of inputKeys) {
        const input = self._inputsByKey.get(key)

        let pos = positionsByKey.get(key)
        if (pos === undefined) {
          pos = 0
          positionsByKey.set(key, pos)
        }

        const snapshotLength = snapshotLengthsByKey.get(key)
        if (snapshotLength) {
          if (pos >= snapshotLength) continue
        } else {
          if (pos >= input.length && wait) await input.update()
          if (pos >= input.length) continue
        }

        loadPromises.push(self._getInputNode(input, pos, { wait }).then(node => [key, node]))
      }

      for (const [key, node] of await Promise.all(loadPromises)) {
        nodesByKey.set(key, node)
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

  async append (value, clock, input) {
    if (clock !== null && !Array.isArray(clock) && !(clock instanceof Map)) {
      input = clock
      clock = null
    }
    if (!Array.isArray(value)) value = [value]
    if (!this.opened) await this.ready()

    input = input || this.defaultInput
    clock = clockToMap(clock || this._getLatestClock())

    // Make sure that causal information propagates.
    // This is tracking inputs that were present in the previous append, but have since been removed.
    // TODO: This should use an embedded index in the future.
    const head = await this._getInputNode(input, input.length - 1)
    if (head && head.clock) {
      const inputId = input.key.toString('hex')
      for (const [id, seq] of head.clock) {
        if (id === inputId || clock.has(id)) continue
        clock.set(id, seq)
      }
    }

    const batch = this._createInputBatch(input, value, clock)
    return input.append(batch)
  }

  linearize (outputs, opts = {}) {
    if (isOptions(outputs)) return this.linearize(null, outputs)
    outputs = outputs || this.defaultOutputs || this._defaultOutputs || []
    if (opts.autocommit === undefined) {
      opts.autocommit = this._autocommit
    }
    return new LinearizedView(this, outputs, {
      ...opts,
      header: {
        protocol: OUTPUT_PROTOCOL
      }
    })
  }

  close () {
    for (const input of this.inputs) {
      input.removeListener('append', this._onappend)
    }
  }

  static async isAutobase (core) {
    if (core.length === 0) return false
    try {
      const block = await core.get(0, { valueEncoding: 'binary' })
      const decoded = c.decode(NodeHeader, block)
      const protocol = decoded && decoded.protocol
      return !!protocol && (protocol === INPUT_PROTOCOL || protocol === OUTPUT_PROTOCOL)
    } catch {
      return false
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
    for (const [key, seq] of head.clock) {
      const length = seq + 1
      if (length > (high[key] || 0)) high[key] = length
    }
  }

  let s = 0

  for (const [key, seq] of node.clock) {
    const length = seq + 1
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
