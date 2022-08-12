const safetyCatch = require('safety-catch')
const b = require('b4a')

const HypercoreBisector = require('../bisect')
const { eq } = require('../clock')

const UpdateStatus = {
  Found: 0,
  NotFound: 1,
  TooNew: 2
}

class OutputSnapshot {
  constructor (core, clock, length) {
    this.core = core
    this.clock = clock
    this.length = length
  }

  get (seq, opts) {
    if (!this.core || seq < 0 || seq >= this.length) return null
    return this.core.get(seq, opts)
  }
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
    const snapshots = new Map([
      [b.toString(core.key, 'hex'), new OutputSnapshot(core, clock, length)]
    ])

    // If this is the only output, no more work to be done
    if (bisectors.length === 1) {
      return { status: UpdateStatus.Found, intersection: { clock, length, snapshots } }
    }

    // If there are multiple outputs linked by intersection point, find the other output checkouts
    const remaining = await Promise.all(this.cores.map(c => this._findCheckout(core.key, c, success.value.clocks)))
    for (let i = 0; i < remaining.length; i++) {
      const checkout = remaining[i]
      if (!checkout) continue
      snapshots.set(b.toString(this.cores[i].key, 'hex'), new OutputSnapshot(this.cores[i], checkout.clock, checkout.length))
    }

    return { status: UpdateStatus.Found, intersection: { clock, length, snapshots } }
  }
}

class OutputTracker {
  constructor (autobase) {
    this.autobase = autobase
    this.opened = false
    this.closed = false
    this.invalid = false

    this._coreSnapshots = new Map()
    this._multiSnapshotsByKey = new Map()
    this._invalidSnapshotIds = new Set()

    this._closing = null
    this._opening = this._open()
    this._opening.catch(safetyCatch)
  }

  async _open () {
    const updates = []
    for (const cores of this._autobase._outputsByKey.values()) {
      updates.push(...cores.map(c => c.update()))
    }
    await Promise.all(updates)
    for (const [key, cores] of this._autobase._outputsByKey) {
      this._coreSnapshots.set(key, cores.map(c => c.snapshot()))
    }
    for (const [key, cores] of this._coreSnapshots) {
      this._multiSnapshotsByKey.set(key, new MultiOutputSnapshot(cores))
    }
    this.opened = true
  }

  async _close () {
    if (!this._opened) await this._opening
    await [...this._coreSnapshots.values()].map(s => s.close())
    this.closed = true
  }

  _closeSnapshot (id) {
    const coreSnapshot = this._coreSnapshots.get(id)
    this._multiSnapshotsByKey.delete(id)
    this._coreSnapshots.delete(id)
    return coreSnapshot.close()
  }

  close () {
    if (this._closing) return this._closing
    this._closing = this._close()
    this._closing.catch(safetyCatch)
    return this._closing
  }

  async _intersectMultiSnapshot (id, multiSnapshot, node) {
    try {
      const intersection = await multiSnapshot.update(node)
      return { id, intersection }
    } catch (err) {
      await this._closeSnapshot(id)
      safetyCatch(err)
      return { id, intersection: null }
    }
  }

  async intersect (node, opts = {}) {
    const proms = []
    for (const [id, multiSnapshot] of this._multiSnapshotsByKey) {
      if (opts.allSnapshots !== true && this._invalidSnapshotsIds.has(id)) continue
      proms.push(this._intersectMultiSnapshot(id, multiSnapshot, node))
    }
    const intersectionsById = new Map()
    for (const { id, intersection } of await Promise.all(proms)) {
      intersectionsById.set(id, intersection)
    }
    return intersectionsById
  }

  async update (node) {
    if (!this.opened) await this._opening

    // TODO: This wastes work by throwing away valid intersections -- save them and use them for redundancy
    const intersectionsById = await this.intersect(node)
    for (const [id, intersection] of intersectionsById) {
      switch (intersection.status) {
        case UpdateStatus.Found:
          return intersection
        case UpdateStatus.NotFound:
          this._invalidSnapshotIds.add(id)
          continue
        case UpdateStatus.TooNew:
          continue
      }
    }
    if (this._invalidSnapshotIds.size === this._multiSnapshotsByKey.size) {
      // There are no remaining good snapshots, so the outputs are all invalid
      this.invalid = true
    }
    return null
  }
}

module.exports = OutputTracker
