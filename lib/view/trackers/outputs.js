const safetyCatch = require('safety-catch')
const b = require('b4a')

const HypercoreBisector = require('../bisect')
const { eq } = require('../clock')

const UpdateStatus = {
  Found: 0,
  NotFound: 1,
  TooNew: 2
}

class MultiOutputSnapshot {
  constructor (cores) {
    this.cores = cores
  }

  _createBisector (snapshot, clock, operations) {
    return new HypercoreBisector(snapshot, {
      skip (node) {
        if (!node.batch) return 0
        return node.batch[0]
      },
      validate (node) {
        if (operations > node.operations) return false
        if (operations === node.operations && !eq(node.clock, clock)) return false
        return true
      },
      cmp (node) {
        return node.operations - operations
      }
    })
  }

  async _findCheckout (primaryKey, snapshot, clocks) {
    if (snapshot.key.equals(primaryKey)) return null
    const target = clocks.get(b.toString(snapshot.key, 'hex'))
    if (!target) return null

    const bisector = this._createBisector(snapshot, target.clock, target.operations)
    await bisector.search()
    if (!bisector.value) return null

    return {
      clock: target,
      operations: bisector.value.operations,
      length: bisector.seq + 1
    }
  }

  async update (node) {
    const bisectors = this.cores.map(c => this._createBisector(c, node.clock, node.operations))

    // Bisect each of the outputs in parallel without doing unnecessary work
    const stepAll = async () => {
      const res = await Promise.all(bisectors.map(b => b.step()))
      return res.includes(true)
    }

    let idx = -1
    while (idx === -1 && await stepAll()) {
      // Once the first node is found, stop bisecting
      idx = bisectors.findIndex(b => b.value)
    }
    if (idx === -1) {
      if (bisectors.findIndex(b => b.invalid) !== -1) return { status: UpdateStatus.TooNew }
      return { status: UpdateStatus.NotFound }
    }

    const success = bisectors[idx]
    const clock = success.value.clock
    const length = success.seq + 1
    const core = this.cores[idx]
    const intersections = new Map()

    // If this is the only output, no more work to be done
    if (bisectors.length === 1) {
      intersections.set(idx, length)
      return { status: UpdateStatus.Found, intersections }
    }

    // If there are multiple outputs linked by intersection point, find the other output checkouts
    const remaining = await Promise.all(this.cores.map(c => this._findCheckout(core.key, c, success.value.clocks)))
    for (let i = 0; i < remaining.length; i++) {
      const checkout = remaining[i]
      if (!checkout) continue
      intersections.set(i, checkout.length)
    }

    return { status: UpdateStatus.Found, intersections }
  }
}

class OutputTracker {
  constructor (coreSnapshots) {
    this.opened = false
    this.closed = false
    this.invalid = false

    this._coreSnapshots = coreSnapshots
    this._multiSnapshotsByKey = new Map()
    this._invalidSnapshotKeys = new Set()
    for (const [key, cores] of this._coreSnapshots) {
      this._multiSnapshotsByKey.set(key, new MultiOutputSnapshot(cores))
    }
  }

  async _intersectMultiSnapshot (key, multiSnapshot, node) {
    try {
      const intersection = await multiSnapshot.update(node)
      return { key, intersection }
    } catch (err) {
      this._multiSnapshotsByKey.delete(key)
      safetyCatch(err)
      return { key, intersection: null }
    }
  }

  async intersect (node, opts = {}) {
    const proms = []
    for (const [key, multiSnapshot] of this._multiSnapshotsByKey) {
      if (opts.allSnapshots !== true && this._invalidSnapshotsKeys.has(key)) continue
      proms.push(this._intersectMultiSnapshot(key, multiSnapshot, node))
    }
    const intersectionsByKey = new Map()
    for (const { key, intersections } of await Promise.all(proms)) {
      intersectionsByKey.set(key, intersections)
    }
    return intersectionsByKey
  }

  async update (node) {
    const intersectionsByKey = await this.intersect(node)
    for (const [key, intersections] of intersectionsByKey) {
      switch (intersections.status) {
        case UpdateStatus.Found:
          return intersections
        case UpdateStatus.NotFound:
          this._invalidSnapshotKeys.add(key)
          continue
        case UpdateStatus.TooNew:
          continue
      }
    }
    if (this._invalidSnapshotKeys.size === this._multiSnapshotsByKey.size) {
      // There are no remaining good snapshots, so the outputs are all invalid
      this.invalid = true
    }
    return null
  }
}

module.exports = OutputTracker
