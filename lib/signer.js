const b4a = require('b4a')
const { partialSignature } = require('hypercore/lib/multisig.js')

module.exports = class Signer {
  constructor (base, core) {
    this.base = base
    this.core = core

    this.opened = false
    this.checkpoints = new Map()

    this.open()
  }

  async sign (indexers, length) {
    const signatures = await this.getSignatures(indexers, length)
    const tree = this.core.core.createTreeBatch()
    const p = await Promise.all(signatures.map(s => partialSignature(tree, s.signer, length, s.length, s.signature)))
    return this.core.core.session.core.verifier.assemble(p)
  }

  async _verify (length, signature, key) {
    if (!this.core.core || length > this.core.core.length) return false
    if (length < this.core.core.indexedLength) return true
    const batch = await this.core.core.restoreBatch(length)
    const { publicKey } = this.base.getNamespace(key, this.core)
    return batch.tree.crypto.verify(batch.signable(this.core.key), signature, publicKey)
  }

  open () {
    if (this.opened) return true
    if (!this.core.pendingIndexedLength) return false

    for (const idx of this.base.linearizer.indexers) {
      for (const checkpoint of idx.flushCheckpoints(this.core.systemIndex)) {
        this.addCheckpoint(idx.core.key, checkpoint)
      }
    }

    this.opened = true
    return true
  }

  addCheckpoint (key, checkpoint) {
    const hex = b4a.toString(key, 'hex')

    let checkpoints = this.checkpoints.get(hex)
    if (!checkpoints) {
      checkpoints = []
      this.checkpoints.set(hex, checkpoints)
    }

    if (checkpoints.length > 0 && checkpoint.length <= checkpoints[checkpoints.length - 1].length) return

    checkpoints.push(checkpoint)
  }

  bestCheckpoint (idx, gc = false) {
    const hex = b4a.toString(idx.core.key, 'hex')

    const checkpoints = this.checkpoints.get(hex)
    if (!checkpoints) return null

    const i = findBestCheckpoint(checkpoints, this.core.pendingIndexedLength)
    if (i === -1) return null

    const checkpoint = checkpoints[i]

    if (gc) {
      this.checkpoints.set(hex, checkpoints.slice(i))
    }

    return checkpoint
  }

  async getSignableLength (indexers) {
    if (!this.open()) return 0

    const signed = []
    const thres = (indexers.length >> 1) + 1

    for (const idx of indexers) {
      const checkpoint = this.bestCheckpoint(idx)
      if (!checkpoint) continue

      // signature is invalid
      if (!(await this._verify(checkpoint.length, checkpoint.signature, idx.core.key))) {
        continue
      }

      signed.push(checkpoint.length)
    }

    return signed.length < thres ? 0 : signed.sort(descendingOrder)[thres - 1]
  }

  async getSignatures (indexers, length) {
    const signatures = []
    const thres = (indexers.length >> 1) + 1

    for (let signer = 0; signer < indexers.length; signer++) {
      const idx = indexers[signer]
      const checkpoint = this.bestCheckpoint(idx, true)
      if (!checkpoint) continue

      // signature is invalid
      if (!(await this._verify(checkpoint.length, checkpoint.signature, idx.core.key))) {
        continue
      }

      const signature = {
        signature: checkpoint.signature,
        length: checkpoint.length,
        signer
      }

      if (checkpoint.length === length) {
        if (signatures.length >= thres) signatures.pop()
        signatures.push(signature)
      } else if (checkpoint.length > length && checkpoint.length <= this.core.core.length) {
        signatures.push(signature)
      }

      if (signatures.length >= thres) return signatures
    }

    return null
  }
}

function findBestCheckpoint (checkpoints, len) {
  if (!checkpoints.length || checkpoints[0].length > len) return -1

  let btm = 0
  let top = checkpoints.length

  while (true) {
    const mid = (btm + top) >> 1

    const c = checkpoints[mid]
    if (c.length === len) return mid

    if (c.length > len) top = mid
    else btm = mid + 1

    if (btm === top) {
      return c.length < len ? mid : mid - 1
    }
  }
}

function descendingOrder (a, b) {
  return a > b ? -1 : a < b ? 1 : 0
}
