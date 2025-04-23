const c = require('compact-encoding')
const b4a = require('b4a')
const ReadyResource = require('ready-resource')
const assert = require('nanoassert')
const SignalPromise = require('signal-promise')

const Linearizer = require('./linearizer')
const NodeBuffer = require('./node-buffer')
const { WriterEncryption } = require('./encryption')

const MAX_PRELOAD = 4
const MAX_PREFETCH = 256

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
    this.isActive = false
    this.isCurrentlyCoupled = false
    this.isCoupled = false // maintained by updateBootstrapWriters
    this.isBootstrap = false // maintained by updateBootstrapWriters
    this.isActiveIndexer = false
    this.available = length
    this.length = length
    this.seenLength = 0
    this.recover = false
    this.frozen = false

    this.syncSignal = null
  }

  static createEncryption (base) {
    return new WriterEncryption(base)
  }

  _pause () {
    if (!this.range) return
    this.range.destroy()
    this.range = null
  }

  _resume () {
    if (this.range) {
      // if we didnt fill up the preload buffer yet, do not commit to more work
      if (this.range.range.end === this.nodes.length + MAX_PREFETCH) return
      this.range.destroy()
    }
    this.range = this.core.download({ start: this.nodes.length, end: this.nodes.length + MAX_PREFETCH, linear: true })
  }

  _updateCoupling () {
    if (!this.base._coupler || this.isCoupled === this.isCurrentlyCoupled) return

    if (this.isCurrentlyCoupled) this.base._coupler.remove(this.core)
    else this.base._coupler.add(this.core)

    this.isCurrentlyCoupled = this.isCoupled
  }

  _removeCouple () {
    if (this.base._coupler && !this.isCoupled && this.isCurrentlyCoupled) {
      this.isCurrentlyCoupled = false
      this.base._coupler.remove(this.core)
    }
  }

  updateActivity () {
    if (!this.core.opened) return

    if (this.seenLength > this.core.length || this.length < this.core.length || this.isBootstrap) {
      // if we have seen a later core, or if we are behind, or if bootstrap
      this.isActive = true
      this.core.setActive(true)
    } else if (this.length === this.core.length) {
      // things look steady
      this.isActive = false
      this.core.setActive(false)
    }

    this._updateCoupling()

    if (this.core.writable) return

    if (this.base.isFastForwarding() || !this.isActive) {
      this._pause()
    } else {
      this._resume()
    }
  }

  setBootstrap (bool) {
    this.isBootstrap = bool
    this.updateActivity()
  }

  seen (length) {
    if (length > this.seenLength) this.seenLength = length
    this.updateActivity()
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
    await this.base._wakeup.add(this.core.core)

    this.updateActivity()

    if (this.core.length > this.length) this.base._queueBump()
  }

  _close () {
    if (this.syncSignal !== null) this.syncSignal.notify()
    return this.core.close()
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

  get (seq) {
    return seq < this.length ? this.nodes.get(seq) : null
  }

  append (value, heads, batch, dependencies, version, optimistic) {
    const node = Linearizer.createNode(this, this.nodes.length + 1, value, heads, batch, dependencies, version, optimistic)

    node.actualHeads = node.heads.slice(0)

    this.nodes.push(node)
    this.available++
    this.length++

    return node
  }

  async update (boot) {
    if (this.opened === false) await this.ready()
    if (this.frozen) return false

    // if this is a boot node, DO NOT, preload as that will make it load
    // and reject nodes based on the tmp state of the system.
    // prop needs a better solution but this works for now
    const preload = boot ? 1 : MAX_PRELOAD

    while (this.available - this.length < preload) {
      // quick sanity check
      if (this.nodes.length === this.core.length || this.core.length === 0) break

      // load next node
      if (this.node === null && !(await this._loadNextNode())) break
      if (!(await this._ensureNodeDependencies(boot))) break

      if (this.recover) this.node.value = null

      this.nodes.push(this.node)
      if (this.node.batch === 1) this.available = this.nodes.length
      this.node = null
    }

    this.updateActivity()

    return this.length < this.available
  }

  _shiftAndClear () {
    const node = this.nodes.shift()
    node.clear()
    return node
  }

  async _loadNextNode () {
    const seq = this.nodes.length

    if (!(await this.core.has(seq))) return false

    try {
      const { node, optimistic, maxSupportedVersion } = await this.core.get(seq, { wait: false })

      const value = node.value == null ? null : c.decode(this.base.valueEncoding, node.value)
      this.node = Linearizer.createNode(this, seq + 1, value, node.heads, node.batch, new Set(), maxSupportedVersion, optimistic)
      return true
    } catch (err) {
      this.frozen = true
      throw err
    }
  }

  async _ensureNodeDependencies (boot) {
    while (this.node.dependencies.size < this.node.heads.length) {
      const rawHead = this.node.heads[this.node.dependencies.size]

      const headWriter = await this.base._getWriterByKey(rawHead.key, -1, rawHead.length, true, false, boot)

      if (headWriter !== this && (headWriter === null || headWriter.length < rawHead.length)) {
        if (!boot) this.base._ensureWakeup(headWriter)
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
        this.node.heads.splice(this.node.dependencies.size, 1)
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
