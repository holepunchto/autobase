const ReadyResource = require('ready-resource')
const safetyCatch = require('safety-catch')

const HypercoreBisector = require('../bisect')
const { eq, maybeLte } = require('../../clock')
const { decodeHeader } = require('../../nodes/messages')

const IntersectionStatus = {
  Found: 0,
  NotFound: 1,
  TooNew: 2
}

class MultiOutputSnapshot {
  constructor (key, cores) {
    this.key = key
    this.cores = cores
    this.lengths = (new Array(cores.length)).fill(0)
    this.clocks = (new Array(cores.length)).fill(null)
  }

  _createBisector (core, clock) {
    return new HypercoreBisector(core, {
      skip (node) {
        if (!node.batch) return 0
        return node.batch[0]
      },
      validate (node) {
        if (!node || maybeLte(clock, node.clock)) return true
        return false
      },
      cmp (node) {
        if (node && eq(node.clock, clock)) return 0
        if (!node || maybeLte(clock, node.clock)) return -1
        return 1
      }
    })
  }

  async _findCheckout (primaryIdx, idx, core, clocks) {
    if (primaryIdx === idx) return null
    const target = clocks[idx]
    if (!target) return null

    const bisector = this._createBisector(core, target.clock)
    await bisector.search()
    if (!bisector.value) return null

    return {
      length: bisector.seq + bisector.value.batch[0] + 1,
      clocks: bisector.value.clocks,
      core
    }
  }

  async intersect (node, opts = {}) {
    const bisectors = this.cores.map(c => this._createBisector(c, node.clock))
    const intersectionLengths = opts.update === false ? (new Array(this.cores.length)).fill(0) : this.lengths
    const intersectionClocks = opts.update === false ? (new Array(this.cores.length)).fill(null) : this.clocks

    // Bisect each of the outputs in parallel without doing unnecessary work
    const stepAll = async () => {
      const res = await Promise.all(bisectors.map(b => b.step()))
      return res.includes(true)
    }

    let idx = -1
    while (idx === -1) {
      const shouldContinue = await stepAll()
      // Once the first node is found, stop bisecting
      idx = bisectors.findIndex(b => b.value)
      if (!shouldContinue) break
    }
    if (idx === -1) {
      if (bisectors.findIndex(b => b.invalid) !== -1) return { status: IntersectionStatus.TooNew, lengths: intersectionLengths }
      return { status: IntersectionStatus.NotFound, lengths: intersectionLengths }
    }

    const success = bisectors[idx]
    const length = success.seq + success.value.batch[1] + 1
    const clocks = success.value.clocks
    intersectionLengths[idx] = length
    intersectionClocks[idx] = clocks

    // If this is the only output, no more work to be done
    if (bisectors.length === 1) return { status: IntersectionStatus.Found, lengths: intersectionLengths }

    // If there are multiple outputs linked by intersection point, find the other output checkouts
    const remaining = await Promise.all(this.cores.map((c, i) => this._findCheckout(idx, i, c, success.value.clocks)))
    for (let i = 0; i < remaining.length; i++) {
      const checkout = remaining[i]
      if (!checkout) continue
      intersectionLengths[i] = checkout.length
      intersectionClocks[i] = checkout.clocks
    }

    return { status: IntersectionStatus.Found, lengths: intersectionLengths }
  }
}

class OutputsTracker extends ReadyResource {
  constructor (autobase, opts = {}) {
    super()
    this.autobase = autobase
    this.invalid = false
    this.wait = opts.wait !== false

    this._coreSnapshots = opts.coreSnapshots
    this._multiSnapshotsByKey = new Map()
    this._invalidSnapshotKeys = new Set()
    this._activeSnapshot = null
    this._header = opts.header
  }

  async _open () {
    const updates = []
    if (!this._coreSnapshots) {
      this._coreSnapshots = new Map()
      for (const cores of this.autobase._outputsByKey.values()) {
        updates.push(...cores.map(c => c.update()))
      }
      await Promise.all(updates)
      for (const [key, cores] of this.autobase._outputsByKey) {
        this._coreSnapshots.set(key, cores.map(c => c.snapshot()))
      }
    }
    // Check to ensure that the view versions match, else the output is invalid
    await Promise.all([...this._coreSnapshots.keys()].map(key => this._validateHeaders(key)))
    for (const [key, cores] of this._coreSnapshots) {
      if (this._invalidSnapshotKeys.has(key)) continue
      this._multiSnapshotsByKey.set(key, new MultiOutputSnapshot(key, cores))
    }
  }

  async _close () {
    for (const cores of this._coreSnapshots.values()) {
      await Promise.all(cores.map(c => c.close()))
    }
  }

  async _validateHeaders (key) {
    const blockPromises = []
    const snapshots = this._coreSnapshots.get(key)
    for (const output of snapshots) {
      if (output.length === 0) {
        this._invalidSnapshotKeys.add(key)
        return
      }
      blockPromises.push(output.core.get(0, { wait: this.wait }))
    }
    const blocks = await Promise.all(blockPromises)
    const headers = await blocks.map(decodeHeader)
    for (const header of headers) {
      if (!header || (header.version !== this._header.version)) {
        this._invalidSnapshotKeys.add(key)
        return
      }
    }
  }

  get lengths () {
    if (!this._activeSnapshot) return null
    return this._activeSnapshot.lengths
  }

  async _intersectMultiSnapshot (key, multiSnapshot, node, opts = {}) {
    try {
      const { status, lengths } = await multiSnapshot.intersect(node, opts)
      return { key, status, lengths }
    } catch (err) {
      this._multiSnapshotsByKey.delete(key)
      safetyCatch(err)
      return { key, status: IntersectionStatus.NotFound }
    }
  }

  async _intersect (node, opts = {}) {
    const proms = []
    for (const [key, multiSnapshot] of this._multiSnapshotsByKey) {
      if (opts.allSnapshots !== true && this._invalidSnapshotKeys.has(key)) continue
      proms.push(this._intersectMultiSnapshot(key, multiSnapshot, node, opts))
    }
    const intersectionsByKey = new Map()
    for (const { key, status, lengths } of await Promise.all(proms)) {
      intersectionsByKey.set(key, { status, lengths })
    }
    return intersectionsByKey
  }

  async intersect (node, opts = {}) {
    if (!this.opened) await this.ready()
    const statusByKey = await this._intersect(node, opts)
    for (const [key, { status, lengths }] of statusByKey) {
      switch (status) {
        case IntersectionStatus.Found:
          if (opts.update !== false) {
            this._activeSnapshot = this._multiSnapshotsByKey.get(key)
          }
          return lengths
        case IntersectionStatus.NotFound:
          if (opts.update !== false) {
            this._invalidSnapshotKeys.add(key)
          }
          continue
        case IntersectionStatus.TooNew:
          continue
      }
    }
    if (opts.update !== false && this._invalidSnapshotKeys.size === this._multiSnapshotsByKey.size) {
      // There are no remaining good snapshots, so the outputs are all invalid
      this.invalid = true
    }
    return null
  }

  async get (id, seq, opts) {
    if (!this._activeSnapshot) throw new Error('Outputs tracker does not have an active snapshot')
    const core = this._activeSnapshot.cores[id]
    // TODO: Error-handle the get and select a different remote
    try {
      const block = await core.get(seq, opts)
      return block
    } catch (err) {
      this._invalidSnapshotKeys.add(this._activeSnapshot.key)
      throw err
    }
  }

  length (id) {
    if (!this._activeSnapshot) throw new Error('Outputs tracker does not have an active snapshot')
    return this._activeSnapshot.lengths[id]
  }

  clocks (id) {
    if (!this._activeSnapshot) throw new Error('Outputs tracker does not have an active snapshot')
    return this._activeSnapshot.clocks[id]
  }
}

module.exports = {
  IntersectionStatus,
  OutputsTracker
}
