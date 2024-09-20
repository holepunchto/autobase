const Linearizer = require('./linearizer')
const NodeBuffer = require('./node-buffer')
const c = require('compact-encoding')
const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')
const assert = require('nanoassert')
const SignalPromise = require('signal-promise')

const MAX_PRELOAD = 4

module.exports = class Writer extends ReadyResource {
  constructor (base, core, length, isRemoved) {
    super()

    this.base = base
    this.core = core
    this.isRemoved = isRemoved
    this.updated = false
    this.range = null
    this.nodes = new NodeBuffer(length)
    this.node = null
    this.isBootstrap = false // maintained by updateBootstrapWriters
    this.isActiveIndexer = false
    this.available = length
    this.length = length
    this.seenLength = 0
    this.recover = false

    this.system = null
    this.digestIndex = 0
    this.digestLength = 0

    this.pendingCheckpoints = []
    this.syncSignal = null
  }

  pause () {
    if (this.range) this.range.destroy()
    this.range = null
  }

  setBootstrap (bool) {
    this.isBootstrap = bool
    this.core.setActive(bool)
  }

  async isInSystem () {
    const bootstrapping = this.base.system.core.length === 0 && b4a.equals(this.core.key, this.base.key)

    if (!bootstrapping) {
      const record = await this.base.system.get(this.core.key, { onlyActive: true })
      if (record === null) return false
    }

    return true
  }

  resume () {
    if (this.range) return
    this.range = this.core.download({ start: this.nodes.length, end: -1, linear: true })
  }

  seen (length) {
    if (length > this.seenLength) this.seenLength = length
  }

  waitForSynced () {
    if (this.core.length === this.length) return Promise.resolve()
    if (this.syncSignal === null) this.syncSignal = new SignalPromise()
    return this.syncSignal.wait()
  }

  async _open () {
    await this.core.ready()
    await this.core.setUserData('referrer', this.base.key)

    // remove later
    this.recover = autoRecover(this.core)

    // add it again incase it wasn't readied before, only needed if this is the first time we set the referrer...
    this.base._wakeup.add(this.core)
  }

  // in case we are in the middle of a migration we might need to old sigs to reach threshold on the new cores
  inflateBackground () {
    this.inflateExistingCheckpoints().catch(safetyCatch)
  }

  async inflateExistingCheckpoints () {
    await this.ready()

    if (this.core.length === 0 || !this.isActiveIndexer || this.nodes.length === 0) {
      return
    }

    const seq = this.nodes.length - 1
    const { checkpoint } = await this.core.get(seq)
    if (!checkpoint) return

    for (let i = 0; i < checkpoint.length; i++) {
      const c = checkpoint[i]
      if (c.checkpointer === 0) {
        this._addCheckpoint(i, c.checkpoint)
        continue
      }
      const prev = await this.core.get(seq - c.checkpointer)
      this._addCheckpoint(i, prev.checkpoint[i].checkpoint)
    }
  }

  _close () {
    if (this.syncSignal !== null) this.syncSignal.notify()
    return this.core.close()
  }

  reset (length) {
    assert(length <= this.core.length || length <= this.length)

    this.pause()

    this.updated = false
    this.node = null
    this.nodes = new NodeBuffer(length)

    this.length = length
    this.available = length
    this.seenLength = length
  }

  get indexed () {
    return this.nodes.offset
  }

  idle () {
    return this.length === this.available && this.length === this.core.length && this.core.opened
  }

  flushed () { // TODO: prop a cleaner way to express this...
    return this.seenLength <= this.length && this.length === this.available && this.length === this.core.length &&
        this.shiftable() === false && !this.core.core.upgrading && this.core.opened
  }

  compare (writer) {
    return b4a.compare(this.core.key, writer.core.key)
  }

  head () {
    return this.nodes.get(this.length - 1)
  }

  advance () {
    if (this.syncSignal !== null && this.length + 1 === this.core.length) this.syncSignal.notify()
    return this.length < this.available ? this.nodes.get(this.length++) : null
  }

  shiftable () {
    return this.length > this.nodes.offset
  }

  shift () {
    if (this.shiftable() === false) return false

    let node = this._shiftAndClear()
    while (node.batch > 1) node = this._shiftAndClear()

    return true
  }

  deriveNamespace (name) {
    const { namespace } = this.core.manifest.signers[0]
    return this.base._viewStore.deriveNamespace(name, namespace)
  }

  get (seq) {
    return seq < this.length ? this.nodes.get(seq) : null
  }

  append (value, heads, batch, dependencies, version) {
    const node = Linearizer.createNode(this, this.nodes.length + 1, value, heads, batch, dependencies, version)

    node.actualHeads = node.heads.slice(0)

    this.nodes.push(node)
    this.available++
    this.length++

    return node
  }

  async update (force) {
    if (this.opened === false) await this.ready()

    while (this.available - this.length < MAX_PRELOAD) {
      // quick sanity check
      if (this.nodes.length === this.core.length || this.core.length === 0) break

      // load next node
      if (this.node === null && !(await this._loadNextNode())) break
      if (!(await this._ensureNodeDependencies())) break

      // TODO: need a proper solution for knowing if we should load node
      if (!force && this.length === 0 && !(await this.isInSystem()) && !this.recover) break
      if (this.recover) this.node.value = null

      this.nodes.push(this.node)
      if (this.node.batch === 1) this.available = this.nodes.length
      this.node = null
    }

    if (this.digestLength < this.core.length && this.isActiveIndexer) await this._checkDigest()

    return this.length < this.available
  }

  async _checkDigest () {
    const index = this.core.length - 1
    const node = await this.core.get(index, { wait: false })

    if (!node) {
      this.core.get(index).catch(safetyCatch)
      return
    }

    if (!node.digest) {
      this.digestLength = index + 1
      return
    }

    const seq = index - node.digest.pointer
    if (seq < 0 || seq === this.digestIndex) {
      this.digestLength = index + 1
      return
    }

    const digestNode = seq === index ? node : await this.core.get(seq, { wait: false })

    if (!digestNode) {
      this.core.get(seq).catch(safetyCatch)
      return
    }

    this.digestLength = index + 1
    this.digestIndex = seq
    this.system = digestNode.digest.key

    // signal that things have changed
    this.base._maybeStaticFastForward = true
  }

  async getVersion (length = this.core.length) {
    if (this.opened === false) await this.ready()

    if (length === 0) return -1

    const node = await this.core.get(length - 1)

    return node.maxSupportedVersion
  }

  async getDigest (length = this.core.length) {
    if (this.opened === false) await this.ready()

    if (length === 0) return null

    let node = await this.core.get(length - 1)

    if (node.digest === null) return null

    if (node.digest.pointer) {
      length -= node.digest.pointer
      node = await this.core.get(length - 1)
    }

    node.digest.pointer = this.core.length - (length - 1)

    return node.digest
  }

  async getCheckpoint (index, length = this.core.length) {
    if (this.opened === false) await this.ready()

    if (length === 0) return null

    let node = await this.core.get(length - 1)

    let target = node.checkpoint[index]
    if (!target) return null

    if (!target.checkpoint) {
      length -= target.checkpointer
      node = await this.core.get(length - 1)
      target = node.checkpoint[index]
    }

    return target.checkpoint
  }

  _shiftAndClear () {
    const node = this.nodes.shift()
    node.clear()
    return node
  }

  flushCheckpoints (systemIndex) {
    const index = systemIndex + 1 // view index is offset by 1
    if (index >= this.pendingCheckpoints.length) return []

    const pending = this.pendingCheckpoints[index]
    this.pendingCheckpoints[index] = null

    while (this.pendingCheckpoints.length > 0 && this.pendingCheckpoints[this.pendingCheckpoints.length - 1] === null) {
      this.pendingCheckpoints.pop()
    }

    return pending !== null ? pending : []
  }

  _addCheckpoints (checkpoints) {
    for (let i = 0; i < checkpoints.length; i++) {
      const { checkpoint, checkpointer } = checkpoints[i]
      if (checkpointer === 0) this._addCheckpoint(i, checkpoint)
    }
  }

  _upsertPendingCheckpoints (index) {
    while (index >= this.pendingCheckpoints.length) this.pendingCheckpoints.push(null)
    if (this.pendingCheckpoints[index]) return this.pendingCheckpoints[index]
    const p = this.pendingCheckpoints[index] = []
    return p
  }

  _addCheckpoint (index, checkpoint) {
    const core = this.base._viewStore.getByIndex(index - 1) // view index is offset by 1

    if (core) {
      core.signer.addCheckpoint(this.core.key, checkpoint)
      return
    }

    const p = this._upsertPendingCheckpoints(index)
    if (p.length > 0 && p[p.length - 1].length >= checkpoint.length) return
    p.push(checkpoint)
  }

  async _loadNextNode () {
    const seq = this.nodes.length
    if (!(await this.core.has(seq))) return false
    const { node, checkpoint, maxSupportedVersion } = await this.core.get(seq, { wait: false })

    if (this.isActiveIndexer && checkpoint) {
      this._addCheckpoints(checkpoint)
    }

    const value = node.value == null ? null : c.decode(this.base.valueEncoding, node.value)
    this.node = Linearizer.createNode(this, seq + 1, value, node.heads, node.batch, new Set(), maxSupportedVersion)
    return true
  }

  async _ensureNodeDependencies () {
    while (this.node.dependencies.size < this.node.heads.length) {
      const rawHead = this.node.heads[this.node.dependencies.size]

      const headWriter = await this.base._getWriterByKey(rawHead.key, -1, rawHead.length, true, false, null)

      if (headWriter !== this && (headWriter === null || headWriter.length < rawHead.length)) {
        this.base._ensureWakeup(headWriter)
        return false
      }

      let headNode = headWriter.nodes.get(rawHead.length - 1)

      // could be a stub node
      if (!headNode) {
        for (const node of this.base.linearizer.heads) {
          if (!compareHead(node, rawHead)) continue
          headNode = node
          break
        }
      }

      // TODO: generalise DAG validation and freeze the writer
      assert(!this.node.dependencies.has(headNode), 'Corrupted DAG')

      // TODO: better way to solve the stub check is to never mutate heads below
      if (headNode === null) { // already yielded
        popAndSwap(this.node.heads, this.node.dependencies.size)
        continue
      }

      this.node.dependencies.add(headNode)
    }

    // always link previous node if it's not indexed
    const offset = this.node.length - 1
    if (offset > this.indexed) {
      this.node.dependencies.add(this.nodes.get(offset - 1))
    }

    return true
  }
}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}

function compareHead (node, head) {
  if (node.length !== head.length) return false
  return b4a.equals(node.writer.core.key, head.key)
}

// this is a list of peers we bugged in the btc and planb room.
// adding them here so migration can run, can be removed in a month or so from time of commit
// note, no security implications of this, we just null them out.

function autoRecover (core) {
  assert(core.opened)

  switch (core.id) {
    case 'ghrpexaboutdm46ombqho7mroxknassnntrxx3cubfux4qi6w6hy':
    case 'qoaanao71s4he1rcd197d336qepykk4467geo1uq8cwnzmpb786o':
    case 'fomhdxgn4j4tzjqy6y7iskhffimzokt7kraddyd8orcht3r8q61o':
    case 'd8f5taxxrit51apftoi38e5b86hb98cgfd7dfp3uo1uoh95qt49o':
    case 'objyf75uggsqpjcut69xdgj46ks8r71jjrq7oxdfsz95sstchkno':
      return true
  }

  return false
}
