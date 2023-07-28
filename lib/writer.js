const Linearizer = require('./linearizer')
const NodeBuffer = require('./node-buffer')
const c = require('compact-encoding')
const b4a = require('b4a')

const MAX_PRELOAD = 4

module.exports = class Writer {
  constructor (base, core, length) {
    this.base = base
    this.core = core
    this.range = this.core.download({ start: length, end: -1 })
    this.nodes = new NodeBuffer(length)
    this.node = null
    this.isIndexer = false
    this.indexed = length
    this.available = length
    this.length = length
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
    return this.shiftable() ? this.nodes.shift() : null
  }

  get (seq) {
    return seq < this.length ? this.nodes.get(seq) : null
  }

  append (value, dependencies, batch) {
    const node = Linearizer.createNode(this, this.nodes.length + 1, value, [], batch, dependencies)

    for (const dep of dependencies) {
      if (!dep.yielded) {
        node.clock.add(dep.clock)
      }

      node.heads.push({
        key: dep.writer.core.key,
        length: dep.length
      })
    }

    node.clock.set(node.writer.core.key, node.length)

    this.nodes.push(node)
    this.available++

    return node
  }

  async update () {
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

  async getCheckpoint (index) {
    await this.core.update()

    let length = this.core.length
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

  async _loadNextNode () {
    const seq = this.nodes.length
    if (!(await this.core.has(seq))) return false
    const { node } = await this.core.get(seq, { wait: false })
    const value = node.value == null ? null : c.decode(this.base.valueEncoding, node.value)
    this.node = Linearizer.createNode(this, seq + 1, value, node.heads, node.batch, [])
    return true
  }

  async _ensureNodeDependencies () {
    while (this.node.dependencies.size < this.node.heads.length) {
      const rawHead = this.node.heads[this.node.dependencies.size]

      const headWriter = await this.base._getWriterByKey(rawHead.key)

      if (headWriter !== this && (headWriter === null || headWriter.length < rawHead.length)) {
        return false
      }

      const headNode = headWriter.nodes.get(rawHead.length - 1)

      if (headNode === null) { // already yielded
        popAndSwap(this.node.heads, this.node.dependencies.size)
        continue
      }

      this.node.dependencies.add(headNode)

      await this._addClock(this.node.clock, headNode)
    }

    this.node.clock.set(this.node.writer.core.key, this.node.length)
    return true
  }

  async _addClock (clock, node) {
    if (node.yielded) return // gc'ed
    for (const [key, length] of node.clock) {
      if (clock.get(key) < length && !(await this.base.system.isIndexed(key, length))) {
        clock.set(key, length)
      }
    }
  }
}

function popAndSwap (list, i) {
  const pop = list.pop()
  if (i >= list.length) return false
  list[i] = pop
  return true
}
