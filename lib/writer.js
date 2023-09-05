const Linearizer = require('./linearizer')
const NodeBuffer = require('./node-buffer')
const c = require('compact-encoding')
const b4a = require('b4a')
const ReadyResource = require('ready-resource')

const MAX_PRELOAD = 4

module.exports = class Writer extends ReadyResource {
  constructor (base, core, length) {
    super()

    this.base = base
    this.core = core
    this.range = this.core.download({ start: length, end: -1 })
    this.nodes = new NodeBuffer(length)
    this.node = null
    this.isIndexer = false
    this.available = length
    this.length = length
  }

  async _open () {
    await this.core.ready()
    await this.core.setUserData('referrer', this.base.key)

    // add it again incase it wasn't readied before, only needed if this is the first time we set the referrer...
    this.base._wakeup.add(this.core)
  }

  _close () {
    return this.core.close()
  }

  get indexed () {
    return this.nodes.offset
  }

  flushed () { // TODO: prop a cleaner way to express this...
    return this.length === this.available && this.length === this.core.length && this.shiftable() === false && !this.core.core.upgrading && this.core.opened
  }

  compare (writer) {
    return b4a.compare(this.core.key, writer.core.key)
  }

  head () {
    return this.nodes.get(this.length - 1)
  }

  advance () {
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

  append (value, heads, batch, dependencies) {
    const node = Linearizer.createNode(this, this.nodes.length + 1, value, heads, batch, dependencies)

    for (const dep of dependencies) {
      node.clock.add(dep.clock)
    }

    node.clock.set(node.writer.core.key, node.length)
    node.actualHeads = node.heads.slice(0)

    this.nodes.push(node)
    this.available++
    this.length++

    return node
  }

  async update () {
    if (this.opened === false) await this.ready()

    while (this.available - this.length < MAX_PRELOAD) {
      // quick sanity check
      if (this.nodes.length === this.core.length || this.core.length === 0) break

      // load next node
      if (this.node === null && !(await this._loadNextNode())) break
      if (!(await this._ensureNodeDependencies())) break

      this.nodes.push(this.node)
      if (this.node.batch === 1) this.available = this.nodes.length
      this.node = null
    }

    return this.length < this.available
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

  async _loadNextNode () {
    const seq = this.nodes.length
    if (!(await this.core.has(seq))) return false
    const { node } = await this.core.get(seq, { wait: false })
    const value = node.value == null ? null : c.decode(this.base.valueEncoding, node.value)
    this.node = Linearizer.createNode(this, seq + 1, value, node.heads, node.batch, new Set())
    return true
  }

  async _ensureNodeDependencies () {
    while (this.node.dependencies.size < this.node.heads.length) {
      const rawHead = this.node.heads[this.node.dependencies.size]

      const headWriter = await this.base._getWriterByKey(rawHead.key, -1, true)

      if (headWriter !== this && (headWriter === null || headWriter.length < rawHead.length)) {
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

      // TODO: better way to solve the stub check is to never mutate heads below
      if (headNode === null) { // already yielded
        popAndSwap(this.node.heads, this.node.dependencies.size)
        continue
      }

      this.node.dependencies.add(headNode)

      if (!headNode.yielded) this.node.clock.add(headNode.clock)
    }

    this.node.clock.set(this.node.writer.core.key, this.node.length)
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
