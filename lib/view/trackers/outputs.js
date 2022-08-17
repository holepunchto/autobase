const safetyCatch = require('safety-catch')
const b = require('b4a')

const HypercoreBisector = require('../bisect')
const { eq } = require('../../clock')

const UpdateStatus = {
  Found: 0,
  NotFound: 1,
  TooNew: 2
}

class MultiOutputSnapshot {
  constructor (cores) {
    this.cores = cores
  }

  _createBisector (core, clock, operations) {
    return new HypercoreBisector(core, {
      skip (node) {
        if (!node.batch) return 0
        return node.batch[0]
      },
      validate (node) {
        if (operations > node.operations) return false
        if (operations === node.operations && !anyEqual(node.clocks, clock)) return false
        return true
      },
      cmp (node) {
        return node.operations - operations
      }
    })
  }

  async _findCheckout (primaryIdx, idx, core, clocks) {
    if (primaryIdx === idx) return null
    const target = clocks[idx]
    if (!target) return null

    const bisector = this._createBisector(core, target.clock, target.operations)
    await bisector.search()
    if (!bisector.value) return null

    return {
      length: bisector.seq + 1,
      clocks: bisector.value.clocks,
      core
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
    const length = success.seq + 1
    const core = this.cores[idx]
    const intersections = new Array(this.cores.length)

    // If this is the only output, no more work to be done
    if (bisectors.length === 1) {
      intersections[idx] = { length, core, clocks: success.value.clocks }
      return { status: UpdateStatus.Found, intersections }
    }

    // If there are multiple outputs linked by intersection point, find the other output checkouts
    const remaining = await Promise.all(this.cores.map(c => this._findCheckout(core.key, c, success.value.clocks)))
    for (let i = 0; i < remaining.length; i++) {
      const checkout = remaining[i]
      if (!checkout) continue
      intersections[i] = checkout
    }

    return { status: UpdateStatus.Found, intersections }
  }
}

class OutputsTracker {
  constructor (autobase) {
    this.autobase = autobase
    this.opened = false
    this.invalid = false

    this._multiSnapshotsByKey = new Map()
    this._invalidSnapshotKeys = new Set()

    this._opening = null
  }

  async _open () {
    const updates = []
    const coreSnapshots = new Map()
    for (const cores of this.autobase._outputsByKey.values()) {
      updates.push(...cores.map(c => c.update()))
    }
    await Promise.all(updates)
    for (const [key, cores] of this.autobase._outputsByKey) {
      coreSnapshots.set(key, cores.map(c => c.snapshot()))
    }
    for (const [key, cores] of coreSnapshots) {
      this._multiSnapshotsByKey.set(key, new MultiOutputSnapshot(cores))
    }
    this.opened = true
  }

  async open () {
    if (this._opening) return this._opening
    this._opening = this._open()
    this._opening.catch(safetyCatch)
    return this._opening
  }

  async _intersectMultiSnapshot (key, multiSnapshot, node) {
    try {
      const intersections = await multiSnapshot.update(node)
      return { key, intersections }
    } catch (err) {
      this._multiSnapshotsByKey.delete(key)
      safetyCatch(err)
      return { key, intersections: null }
    }
  }

  async intersect (node, opts = {}) {
    const proms = []
    for (const [key, multiSnapshot] of this._multiSnapshotsByKey) {
      if (opts.allSnapshots !== true && this._invalidSnapshotKeys.has(key)) continue
      proms.push(this._intersectMultiSnapshot(key, multiSnapshot, node))
    }
    const intersectionsByKey = new Map()
    for (const { key, intersections } of await Promise.all(proms)) {
      console.log('key:', key, 'intersections:', intersections)
      intersectionsByKey.set(key, intersections)
    }
    return intersectionsByKey
  }

  async update (node) {
    if (!this.opened) await this.open()
    const intersectionsByKey = await this.intersect(node)
    console.log('intersections by key:', intersectionsByKey)
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

module.exports = OutputsTracker

function anyEqual (clocks, clock) {
  for (const { clock: nodeClock } of clocks) {
    if (eq(nodeClock, clock)) return true
  }
  return false
}
