const { EventEmitter } = require('events')

const safetyCatch = require('safety-catch')
const streamx = require('streamx')
const mutexify = require('mutexify/promise')
const c = require('compact-encoding')
const b = require('b4a')

const LinearizedCore = require('./lib/linearize')
const MemberBatch = require('./lib/batch')
const KeyCompressor = require('./lib/compression')
const Output = require('./lib/output')
const { length } = require('./lib/clock')
const { InputNode, OutputNode } = require('./lib/nodes')
const { Node: NodeSchema, decodeHeader } = require('./lib/nodes/messages')

const INPUT_PROTOCOL = '@autobase/input/v1'
const OUTPUT_PROTOCOL = '@autobase/output/v1'

const FORCE_NON_SPARSE = +process.env['NON_SPARSE'] // eslint-disable-line

module.exports = class Autobase extends EventEmitter {
  constructor ({ inputs, outputs, localInput, localOutput, apply, unwrap, view, autostart, eagerUpdate, sparse } = {}) {
    super()
    this.localInput = localInput
    this.localOutput = localOutput
    this.opened = false
    this.closed = false

    this._closing = null
    this._inputs = inputs || []
    this._outputs = outputs || []
    this._inputsByKey = new Map()
    this._outputsByKey = new Map()
    this._keyCompressors = new Map()
    this._viewSessions = new Set()
    this._readStreams = []
    this._batchId = 0
    this._lock = mutexify()
    this._eagerUpdate = eagerUpdate === undefined ? !!localOutput : eagerUpdate
    this._sparse = (sparse !== false) && (FORCE_NON_SPARSE !== 1)
    this._loadingInputsCount = 0
    this._pendingUpdates = []

    this.view = null
    if (apply || autostart) this.start({ apply, view, unwrap })

    this._onappend = () => {
      this.emit('append')
      this._onInputsChanged()
    }

    this._opening = this._open()
    this._opening.catch(safetyCatch)
    this.ready = () => this._opening
  }

  // Private Methods

  async _open () {
    await Promise.all([
      ...this._inputs.map(i => i.ready()),
      ...this._outputs.map(o => o.ready())
    ])
    for (const input of this._inputs) {
      this._addInput(input)
    }
    for (const output of this._outputs) {
      this._addOutput(output)
    }
    if (this.localOutput) {
      await this.localOutput.ready()
      this._addOutput(this.localOutput, { local: true })
    }

    this.opened = true
  }

  // Called by MemberBatch
  _addInput (input) {
    const id = b.toString(input.key, 'hex')
    if (this._inputsByKey.has(id)) return

    const session = input.session({ sparse: this._sparse })
    session.on('append', this._onappend)
    this._inputsByKey.set(id, session)

    this._onInputsChanged()
  }

  // Called by MemberBatch
  _addOutput (core, opts) {
    const id = b.toString(core.key, 'hex')
    if (this._outputsByKey.has(id)) return

    const output = new Output(core)
    this._outputsByKey.set(id, output)

    if (opts && opts.local) this.localOutput = output
  }

  // Called by MemberBatch
  _removeInput (input, opts) {
    const id = b.isBuffer(input) ? b.toString(input, 'hex') : b.toString(input.key, 'hex')
    if (!this._inputsByKey.has(id)) return

    input = this._inputsByKey.get(id)
    input.removeListener('append', this._onappend)
    this._inputsByKey.delete(id)

    this._onInputsChanged()
  }

  // Called by MemberBatch
  _removeOutput (output, opts) {
    const id = b.isBuffer(output) ? b.toString(output, 'hex') : b.toString(output.key, 'hex')
    if (!this._outputsByKey.has(id)) return

    output = this._outputsByKey.get(id)
    this._outputsByKey.delete(id)

    if (opts && opts.local) this.localOutput = null
  }

  _onInputsChanged () {
    this._bumpReadStreams()
    if (this._eagerUpdate && this.view) {
      // Eagerly update the primary view if there's a local output
      // This will be debounced internally
      this.view.update().catch(safetyCatch)
    }
  }

  _bumpReadStreams () {
    for (const stream of this._readStreams) {
      stream.bump()
    }
  }

  _getKeyCompressor (input) {
    let keyCompressor = this._keyCompressors.get(input)
    if (!keyCompressor) {
      keyCompressor = new KeyCompressor(input)
      this._keyCompressors.set(input, keyCompressor)
    }
    return keyCompressor
  }

  async _decompressClock (input, seq, clock) {
    const keyCompressor = this._getKeyCompressor(input)
    return keyCompressor.decompress(clock, seq)
  }

  async _compressClock (input, seq, clock) {
    const keyCompressor = this._getKeyCompressor(input)
    return keyCompressor.compress(clock, seq)
  }

  async _getInputNode (input, seq, opts) {
    if (seq < 0) return null
    if (b.isBuffer(input)) input = b.toString(input, 'hex')
    if (typeof input === 'string') {
      if (!this._inputsByKey.has(input)) return null
      input = this._inputsByKey.get(input)
    }

    const block = await input.get(seq, opts)
    if (!block && opts.wait === false) return null

    let decoded = null
    try {
      decoded = NodeSchema.decode({ start: 0, end: block.length, buffer: block })
    } catch (err) {
      return safetyCatch(err)
    }

    const node = new InputNode({ ...decoded, key: input.key, seq })
    let loaded = false

    if (opts && opts.loadBatchClock !== false) {
      if (node.batch[1] !== 0) {
        const batchEnd = await this._getInputNode(input, seq + node.batch[1], opts)
        node.clock = batchEnd.clock
        loaded = true
      }
    }

    if (node.clock) {
      if (!loaded) node.clock = await this._decompressClock(input, seq, decoded.clock)
      if (node.seq > 0) node.clock.set(node.id, node.seq - 1)
      else node.clock.delete(node.id)
    }

    return node
  }

  _waitForInputs () {
    return new Promise(resolve => {
      this._pendingUpdates.push(resolve)
    })
  }

  // Public API

  get inputs () {
    return [...this._inputsByKey.values()]
  }

  get outputs () {
    return [...this._outputsByKey.values()]
  }

  get started () {
    return !!this.view
  }

  loadingInputs () {
    this._loadingInputsCount++
    return () => {
      if (--this._loadingInputsCount === 0) {
        for (const resolve of this._pendingUpdates) {
          resolve()
        }
      }
    }
  }

  start ({ view, apply, unwrap } = {}) {
    if (this.view) throw new Error('Start must only be called once')
    const core = new LinearizedCore(this, {
      header: { protocol: OUTPUT_PROTOCOL },
      view,
      apply,
      unwrap
    })
    const session = core.session()
    this._viewSessions.add(session)
    this.view = view ? view(session) : session
  }

  async heads (clock) {
    if (!this.opened) await this._opening
    clock = clock || await this.latest()

    const headPromises = []
    for (const key of clock.keys()) {
      const input = this._inputsByKey.get(key)
      if (!input) return []
      headPromises.push(this._getInputNode(input, clock ? clock.get(key) : input.length - 1))
    }

    return Promise.all(headPromises)
  }

  _loadClockNodes (clock) {
    return Promise.all([...clock].map(([id, seq]) => {
      return this._getInputNode(id, seq, { loadBatchClock: false }).then(node => [id, node])
    }))
  }

  _getLatestSparseClock (inputs) {
    if (!inputs) inputs = this.inputs
    if (!Array.isArray(inputs)) inputs = [inputs]

    const clock = new Map()
    for (const input of inputs) {
      const id = b.toString(input.key, 'hex')
      if (!this._inputsByKey.has(id)) throw new Error('Hypercore is not an input of the Autobase')
      if (input.length === 0) continue
      clock.set(id, input.length - 1)
    }

    return clock
  }

  async _getLatestNonSparseClock (clock) {
    const allHeads = await this._loadClockNodes(clock)
    const availableClock = new Map()

    // If the latest node in an input is in the middle of a batch, shift the clock back to before that batch.
    let wasAdjusted = false
    for (const [id, node] of allHeads) {
      if (node.batch[1] === 0) {
        availableClock.set(id, node.seq)
      } else {
        wasAdjusted = true
        const adjusted = node.seq - node.batch[0] - 1
        if (adjusted < 0) continue
        availableClock.set(id, adjusted)
      }
    }

    // If any head links to a node that's after a current head, then it can't be satisfied.
    // If not satisfiable, find the latest satisfiable head for that input.
    const heads = wasAdjusted ? await this._loadClockNodes(availableClock) : allHeads
    while (heads.length) {
      const [id, node] = heads[heads.length - 1]
      let satisfied = true
      let available = true
      for (const [clockId, clockSeq] of node.clock) {
        if (availableClock.has(clockId) && clockSeq <= availableClock.get(clockId)) continue
        satisfied = false
        const next = node.seq - node.batch[0] - 1
        if (next < 0) {
          available = false
          break
        }
        heads[heads.length - 1][1] = await this._getInputNode(id, next, { loadBatchClock: false })
        break
      }
      if (satisfied) {
        availableClock.set(id, node.seq)
        heads.pop()
      } else if (!available) {
        availableClock.delete(id)
        heads.pop()
      }
    }

    return availableClock
  }

  async latest (inputs) {
    if (!this.opened) await this._opening
    await Promise.all(this.inputs.map(i => i.update()))
    const sparseClock = this._getLatestSparseClock(inputs)
    if (this._sparse) return sparseClock
    return this._getLatestNonSparseClock(sparseClock)
  }

  memberBatch () {
    return new MemberBatch(this)
  }

  async addInput (input, opts) {
    const batch = new MemberBatch(this)
    batch.addInput(input, opts)
    return batch.commit()
  }

  async removeInput (input, opts) {
    const batch = new MemberBatch(this)
    batch.removeInput(input, opts)
    return batch.commit()
  }

  async addOutput (output, opts) {
    const batch = new MemberBatch(this)
    batch.addOutput(output, opts)
    return batch.commit()
  }

  async removeOutput (output, opts) {
    const batch = new MemberBatch(this)
    batch.removeOutput(output, opts)
    return batch.commit()
  }

  createCausalStream (opts = {}) {
    const self = this
    let heads = null
    let batchIndex = null
    let batchClock = null

    return new streamx.Readable({ open, read })

    function open (cb) {
      self.heads(opts.clock)
        .then(h => { heads = h })
        .then(() => cb(null), err => cb(err))
    }

    function read (cb) {
      let node = null
      let headIndex = null
      let clock = null

      if (batchIndex !== null) {
        node = heads[batchIndex]
        headIndex = batchIndex
        clock = batchClock
        if (node.batch[0] === 0) {
          batchIndex = null
          batchClock = null
        }
      } else {
        const info = forkInfo(heads)
        if (!info.forks.length) {
          this.push(null)
          return cb(null)
        }
        node = info.forks[info.smallest]
        headIndex = heads.indexOf(node)
        clock = info.clock
      }

      if (node.batch[1] === 0 && node.batch[0] > 0) {
        batchIndex = headIndex
        batchClock = clock
      }

      this.push(new OutputNode({ ...node, change: node.key, clock, operations: length(clock) }))

      // TODO: When reading a batch node, parallel download them all (use batch[0])
      // TODO: Make a causal stream extension for faster reads

      if (node.seq === 0) {
        heads.splice(headIndex, 1)
        return cb(null)
      }

      self._getInputNode(node.id, node.seq - 1).then(next => {
        heads[headIndex] = next
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
      if (!self.opened) await self._opening
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
            // If resolved is false, yield he unresolved node as usual
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
          pos = opts.tail === true ? input.length : 0
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

  async _createInputBatch (input, batch, clock) {
    const nodes = []

    // Only the first block in the batch stores the keys, and only the last stores the clock
    const compressed = await this._compressClock(input, input.length, clock)

    for (let i = 0; i < batch.length; i++) {
      const batchOffset = (batch.length !== 1) ? [i, batch.length - 1 - i] : null
      const node = {
        value: !b.isBuffer(batch[i]) ? b.from(batch[i]) : batch[i],
        key: input.key,
        batch: batchOffset,
        clock: null,
        keys: null
      }
      if (i === 0) {
        node.keys = compressed.keys
      }
      if (i === batch.length - 1) {
        node.clock = compressed.clock
      }
      nodes.push(node)
    }

    if (input.length === 0) {
      nodes[0].header = {
        protocol: INPUT_PROTOCOL
      }
    }

    return nodes.map(node => c.encode(NodeSchema, node))
  }

  async _append (value, clock, input) {
    if (clock !== null && !Array.isArray(clock) && !(clock instanceof Map)) {
      input = clock
      clock = null
    }
    if (!Array.isArray(value)) value = [value]

    clock = clockToMap(clock || await this.latest())
    input = input || this.localInput

    // Make sure that causal information propagates.
    // This is tracking inputs that were present in the previous append, but have since been removed.
    const head = await this._getInputNode(input, input.length - 1)
    const inputId = b.toString(input.key, 'hex')

    if (head && head.clock) {
      for (const [id, seq] of head.clock) {
        if (id === inputId || clock.has(id)) continue
        clock.set(id, seq)
      }
    }

    const batch = await this._createInputBatch(input, value, clock)
    return input.append(batch)
  }

  async append (value, clock, input) {
    if (!this.opened) await this._opening
    const release = await this._lock()
    try {
      return await this._append(value, clock, input)
    } finally {
      release()
    }
  }

  async _close () {
    if (this.closed) return
    await this._opening
    for (const input of this.inputs) {
      input.removeListener('append', this._onappend)
    }
    const views = [...this._viewSessions]
    await Promise.all([
      ...views.map(s => s.close()),
      ...this._inputs.map(i => i.close()),
      ...this.inputs.map(i => i.close()),
      ...this.outputs.map(o => o.close())
    ])
    this.closed = true
  }

  close () {
    if (this.closed) return Promise.resolve()
    if (this._closing) return this._closing
    this._closing = this._close()
    this._closing.catch(safetyCatch)
    return this._closing
  }

  static async isAutobase (core) {
    if (core.length === 0) return false
    try {
      const block = await core.get(0, { valueEncoding: 'binary' })
      const header = decodeHeader(block)
      const protocol = header && header.protocol
      return !!protocol && (protocol === INPUT_PROTOCOL || protocol === OUTPUT_PROTOCOL)
    } catch {
      return false
    }
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
    if (sizes[i] === sizes[smallest] && forks[i].key < forks[smallest].key) {
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
