const { EventEmitter } = require('events')
const safetyCatch = require('safety-catch')

const { OutputNode } = require('./nodes')
const { eq, lte, greatestCommonAncestor } = require('./clock')
const promises = Symbol.for('hypercore.promises')

class MemoryBranch {
  constructor (clock, applied) {
    this.clock = clock
    this.appled = applied || []
    this.pending = []
    this.length = 0
  }

  async locate (clock) {

  }

  apply (func, view) {

  }

  shrinkApplied (size) {
  }
}

class PersistentBranches {
  constructor (cores) {
    this.cores = cores
    this.snapshots = null
    this.heads = []
  }

  async _open () {
    await Promise.all(this.cores.map(c => c.update()))
    this.snapshots = this.cores.map(c => c.snapshot())
    for (const snapshot of this.snapshots) {
      if (!snapshot.length) this.heads.push(null)
      try {
        const head = await snapshot.get(snapshot.length - 1)
        this.heads.push(head)
      } catch {
        // If the head can't be loaded, skip over this output
        this.heads.push(null)
      }
    }
  }

  async _bisect (idx, clock) {
    const snapshot = this.snapshots[idx]
    const head = this.heads[idx]

    if (eq(head.clock, clock)) return snapshot.length
    const target = greatestCommonAncestor(head.clock, clock)

    let found = null
    let upper = null
    let lower = null
    for (let i = 1; i < snapshot.length - 1; i <<= 1) {
      const idx = Math.max(snapshot.length - 1 - i, 0)
      const h = await snapshot.get(idx)
      if (eq(h.clock, target)) {
        found = { node: h, pos: idx }
        break
      }
      if (!lte(h.clock, target)) continue
      lower = idx
      upper = snapshot.length - 1 - i >> 1
      break
    }

    while (!found && lower <= upper) {
      const mid = Math.floor((upper + lower) / 2)
      const node = await snapshot.get(mid)
      if (eq(node.clock, clock)) {
        found = { node, pos: mid }
        break
      }
      if (!lte(node.clock, target)) {
        upper = mid - 1
      } else {
        lower = mid + 1
      }
    }

    if (!found) return null

    const batchOffset = found.node.batch[1]
    return found.pos + batchOffset
  }

  async locate (clock, { update } = {}) {
    if (!this.snapshots) await this._open()

    const bisects = []
    for (let i = 0; i < this.heads.length; i++) {
      const head = this.heads[i]
      if (!head) continue
      if (eq(head.clock, clock)) return { snapshot: this.snapshots[i], length: this.snapshots[i].length } // Quick fast-forward check
      if (!lte(clock, head.clock)) continue // clock is not contained in the output
      bisects.push(i) // clock needs to be located with a bisect
    }

    if (!bisects.length) return null
    const lengths = await Promise.allSettled(bisects.map(i => this._bisect(i, clock)))

    let bestLength = null
    let bestSnapshot = null
    for (let i = 0; i < lengths.length; i++) {
      const { value } = lengths[i]
      if (value === undefined) {
        if (update === true) {
          // If we are updating, and a bisect failed to find the clock, this snapshot can be discarded.
          const idx = bisects[i]
          const snapshot = this.snapshots[idx]
          await snapshot.close()
          this.heads[idx] = null
          this.snapshots[idx] = null
          continue
      }
      if (bestLength === null || value > bestLength) {
        bestLength = value
        bestSnapshot = this.snapshots[bisects[i]]
      }
    }

    if (bestLength === null) return null
    return { snapshot: bestSnapshot, length: bestLength }
  }

  close () {
    if (!this.snapshots) return Promise.resolve()
    return Promise.all(this.snapshots.map(s => s.close()))
  }
}

class Linearization {
  constructor (autobase, clock, last) {
    this.autobase = autobase
    this.clock = clock
    this.last = last

    this.persistentBranches = new PersistentBranches(clock, autobase.outputs)
    this.applied = last ? last.applied : []
    this.pending = []

    this.length = 0
    this.snapshot = null
    this.status = null
  }

  _shrinkApplied (size) {
    // The applied buffer is modified copy-on-write
    const applied = []
    for (let i = size; i < this.applied.length; i++) {
      applied.push(this.applied[i])
    }
    this.applied = applied
  }

  _applyPending (view, apply) {
    // TODO: This is easy
  }

  async update ({ view, apply } = {}) {
    let snapshot = null
    let truncated = 0
    let appended = 0

    // First find the persistent truncation point, if this is not the first update.
    if (this.last && this.last.snapshot) {
      const truncationSnapshot = await this.persistentBranches.locate(this.last.clock)
      if (!truncationSnapshot) {
        // If the old clock is nowhere to be found, then there was a full reorg
        truncated += this.last.snapshot.length
      } else {
        if (truncationSnapshot.length < this.last.snapshot.length) {
          truncated += this.last.snapshot.length - truncationSnapshot.length
        }
        appended += snapshot.length - truncationSnapshot.length
      }
    }

    // If the persistent branch truncated, bust the in-memory view and rebuild it
    // If the persistent branch grew, shrink the in-memory view
    // If the persistent branch stayed the same, don't alter the memory view
    if (truncated > 0) {
      this._shrinkApplied(this.applied.length)
    } else if (appended > 0) {
      this._shrinkApplied(appended)
    }

    // Feed the applied nodes into the persistent branches until one can't be located
    // TODO: This should update the branches too...
    let appliedOffset = 0
    while (appliedOffset < this.applied.length) {
      const node = this.applied[appliedOffset]
      if (!(await this.persistentBranches.locate(node.clock))) break
      appliedOffset += node.batch[1] > 0 ? node.batch[1] + 1 : 1
    }

    const pending = []
    // First find the best snapshot and buffer nodes along the way
    for await (const node of this.autobase.createCausalStream({ clock: this.clock })) {
      pending.push(node)
      snapshot = await this.persistentBranches.locate(node.clock, { update: true })
      if (snapshot) break
    }

    this.status = {
      appended,
      truncated
    }
    this.snapshot = snapshot
    this.length = snapshot.length + this.applied.length
  }

  get (seq) {
    if (!this.snapshot) throw new Error('Cannot call get before the linearizer has been updated')
    if (seq < 0 || seq > this.length - 1) return null
    if (seq > this.snapshot.length) return this.applied[seq]
    return this.snapshot.get(seq)
  }
}

module.exports = class LinearizedCore extends EventEmitter {
  constructor (autobase, opts = {}) {
    super()
    this[promises] = true
    this.autobase = autobase
    this.view = opts.view ? opts.view(this) : this
    this.byteLength = 0

    this._header = opts.header
    this._view = opts.view
    this._snapshot = opts.snapshot
    this._apply = opts.apply || defaultApply

    this._lastLinearization = new Linearization({
      header: this.header,
      apply: this.apply
    })

    this._nodes = []
    this._pending = []
    this._applying = null
  }

  get length () {
    return this._lastLinearization ? this._lastLinearization.length : 0
  }

  update () {

  }

  get (seq, opts) {

  }

  append (blocks) {

  }

  unwrap () {

  }

  wrap () {

  }

  snapshot () {

  }

  close () {

  }
}

function defaultApply (core, batch, clock, change) {
  return core.append(batch.map(b => b.value))
}
