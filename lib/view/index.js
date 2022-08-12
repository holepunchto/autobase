const safetyCatch = require('safety-catch')
const debounceify = require('debounceify')

const { eq } = require('../clock')

const LinearizedCore = require('./core')
const AppliedBranch = require('./branches/applied')
const OutputsBranch = require('./branches/outputs')

class Update {
  constructor (autobase, clock, cores, opts = {}) {
    this.autobase = autobase
    this.clock = clock
    this.cores = cores
    this.executed = !!opts.executed

    this._executing = null
    this._applying = null
  }

  async _findRemoteAncestors () {
    // P1: Found intersection between the new branch and the old branch
    // P2: Found intersection between the new branch and the outputs
    const pending = []
    let p1 = null
    let p2 = null

    // TODO: Causal stream should treat this.clock as the "first" node so as to avoid over-reading blocks.
    // (this change would also provide fast-forward implicitly)
    for await (const node of this.autobase.createCausalStream({ clock: this.clock })) {
      let a = null
      let b = null
      if (!p1) a = this.applied.update(node)
      if (!p2) b = this.latestOutputs.update(node)
      const result = await Promise.all([a, b])
      if (a) p1 = result[0]
      if (b) p2 = result[1]
      if (p2 || p1) break
      pending.push(node)
    }

    // P2 > P1 -> random-access back to fork point using outputs
    // just a faster way to find P1 vs continuing to unroll the causal stream
    if (p2 && !p1) {
      if (!this.applied.opened) await this.applied.open()
      while (this.applied.head) {
        p1 = await this.latestOutputs.intersect(this.applied.head)
        if (p1) break
        await this.applied.pop()
      }
    }

    if (p1 && !p2) {
      // We found an intersection with the old branch before we found one with the latest outputs -- use the old outputs
      // TODO: Potentially roll the stream ~5 back because the output is probably there
      const latestOutputs = this.latestOutputs
      this.latestOutputs = this.lastOutputs
      p2 = {
        length: this.latestOutputs.length,
        operations: this.latestOutputs.operations
      }
      await latestOutputs.close()
    } else {
      if (this.lastOutputs) {
        const lastOutputs = this.lastOutputs
        this.lastOutputs = null
        await lastOutputs.close()
      }
    }

    if (!p1) p1 = { length: 0, intersection: 0 }
    if (!p2) p2 = { length: 0, intersection: 0 }

    return { pending, p1, p2 }
  }

  async _findLocalAncestors () {
    const pending = []
    let p2 = null
    for await (const node of this.autobase.createCausalStream({ clock: this.clock })) {
      p2 = await this.latestOutputs.update(node)
      if (p2) break
      pending.push(node)
    }
    if (!p2) p2 = { length: 0, intersection: 0 }
    if (this.lastOutputs) {
      const lastOutputs = this.lastOutputs
      this.lastOutputs = null
      await lastOutputs.close()
    }
    return { pending, p2 }
  }

  async _executeRemote () {
    const oldLength = this.applied.length
    const { pending, p1, p2 } = await this._findRemoteAncestors()

    const nodes = (this.applied && (p1.length > p2.length)) ? this.applied.slice() : []
    const length = this.latestOutputs.length + nodes.length
    const appended = length - Math.max(p1.length, p2.length)
    const truncated = oldLength - p1.length

    return { pending, nodes, length, appended, truncated, p1, p2 }
  }

  async _executeLocal () {
    const oldLength = this.localOutput.length
    const { pending, p2 } = await this._findLocalAncestors()
    return {
      length: p2.length,
      truncated: oldLength - p2.length,
      appended: 0,
      nodes: [],
      pending,
      p1: p2,
      p2
    }
  }

  _execute () {
    return this.applied ? this._executeRemote() : this._executeLocal()
  }

  execute () {
    if (this._executing) return this._executing
    this._executing = this._execute()
    this._executing.catch(safetyCatch)
    return this._executing
  }

  clone () {
    const latestOutputs = this.latestOutputs && this.latestOutputs.clone()
    const lastOutputs = this.lastOutputs ? this.lastOutputs.clone() : latestOutputs
    return new Update(this.autobase, this.clock, lastOutputs, latestOutputs, this.applied, this.localOutput, {
      executed: this.executed
    })
  }

  async close () {
    if (this.lastOutputs) await this.lastOutputs.close()
    await this.latestOutputs.close()
  }
}

class LinearizedView {
  constructor (autobase, cores, opts = {}) {
    this.autobase = autobase
    this.cores = cores

    this.applyFunction = opts.apply || defaultApply
    this.openFunction = opts.open
    this.userView = null
    this.clock = null

    this._writable = opts.writable !== false
    this._applying = null

    this.update = debounceify(this._update.bind(this))
  }

  async _applyPending (pending) {
    let batch = []
    while (pending.length) {
      const node = pending.pop()
      batch.push(node)
      if (node.batch[1] > 0) continue
      this._applying = node

      if (!this.userView) {
        const { view } = this._open({ pin: true })
        this.userView = view
      }

      const inputNode = await this.autobase._getInputNode(node.change, node.seq)
      const clocks = {
        local: inputNode.clock,
        global: this._applying.clock
      }
      const start = this.nodes.length

      try {
        await this.applyFunction(this.userView, batch, clocks, node.change)
      } catch (err) {
        while (this.nodes.length > start) {
          this.nodes.pop()
        }
        throw err
      }

      if (this.nodes.length === start) {
        throw new Error('For now, every apply call must append at least one value')
      }

      for (let j = start; j < this.nodes.length; j++) {
        const change = this.nodes[j]
        change.batch[0] = j - start
        change.batch[1] = this.nodes.length - j - 1
      }

      this._applying = null
      batch = []
    }
    if (batch.length) throw new Error('Cannot rebase a partial batch')
  }

  async _rebuild (clock) {
    if (!this.autobase.opened) await this.autobase.ready()
    const lastOutputs = this.lastUpdate ? this.lastUpdate.latestOutputs : null
    const localOutput = this.autobase.localOutput

    let applied = null
    if (!localOutput || !this._writable) {
      if (this.lastUpdate) {
        applied = new AppliedBranch(this.autobase, this.nodes, this.length, this.lastUpdate.clock)
      } else {
        applied = new AppliedBranch(this.autobase, [], 0, new Map()) // If we ever change the clock format, update this
      }
    }

    const outputs = new OutputsBranch(localOutput ? [localOutput] : this.autobase.outputs)
    this.lastUpdate = new Update(this.autobase, clock, lastOutputs, outputs, applied, localOutput)

    try {
      this.status = await this.lastUpdate.execute()
      await this._commitUpdate(localOutput)
      return this.appended > 0
    } catch (err) {
      safetyCatch(err)
      return false
    }
  }

  async _commitUpdate () {
    try {
      await this._applyPending(this.status.pending)
    } catch (err) {
      safetyCatch(err)
      throw err
    } finally {
      if (this.nodes.length) {
        // If we have nodes, then the last applied clock is the clock (useful in the case of partial applies)
        // If apply was ever called, then this.nodes will be populated
        this.clock = this.nodes[this.nodes.length - 1].clock
      }
    }

    if (this._writable && localOutput) {
      if (this.status.truncated) {
        await localOutput.truncate(localOutput.length - this.status.truncated)
      }
      if (localOutput.length === 0 && this.nodes.length) {
        this.nodes[0].header = this.header
      }
      await localOutput.append(this.nodes)
    }

    for (const session of this._sessions) {
      if (this.status.truncated) session.emit('truncate', this.lastUpdate.length - this.status.truncated)
      if (this.status.appended) session.emit('append')
    }
  }

  async _update () {
    // First check if any work needs to be done
    // If we're building a local index, and the clock is the same, no work is needed.
    // (If we're not building a local index, the state of the remote outputs might have changed, so must update)
    const clock = await this.autobase.latest()

    if (this.autobase.localOutput && this.lastUpdate && eq(clock, this.lastUpdate.clock)) {
      this.status = { appended: 0, truncated: 0 }
      return
    }

    // Next check if any snapshot sessions need to be migrated to a root clone before the update
    if (this.isRoot) {
      const snapshots = this._sessions.filter(s => s._snapshotted)
      for (const snapshot of snapshots) {
        migrateSession(this, this.clone(), snapshot)
      }
    }

    // Next perform the update
    await this._rebuild(clock)
  }
}

module.exports = LinearizedView

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}
