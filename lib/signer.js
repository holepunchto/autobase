const { assemble, partialSignature } = require('hypercore/lib/multisig.js')

module.exports = class Signer {
  constructor (base, core) {
    this.base = base
    this.core = core
  }

  async sign (indexers, length) {
    const signatures = this.getSignatures(indexers, length)

    const nodes = this.core.indexBatch(0, this.core.nodes.length - this.core._shifted)
    const tree = this.core.core.createTreeBatch(this.core.pendingIndexedLength, nodes)
    const p = await Promise.all(signatures.map(s => partialSignature(tree, s.signer, length, s.length, s.signature)))
    return assemble(p)
  }

  refresh () {
  }

  _verify (length, signature, key) {
    if (!this.core.core || length > this.core.core.length) return false
    if (length < this.core.core.length) return true
    const batch = this.core.core.createTreeBatch(length)
    const { publicKey, namespace } = this.base.getNamespace(key, this.core)
    return batch.tree.crypto.verify(batch.signable(namespace), signature, publicKey)
  }

  getSignableLength (indexers = this.core.indexers) {
    const index = this.core.likelyIndex
    if (index === -1) return 0

    let length = 0
    const signed = []
    const thres = (indexers.length >> 1) + 1

    for (const idx of indexers) {
      const checkpoints = idx.getLatestCheckpoint()
      if (!checkpoints || checkpoints.length <= index) continue

      const checkpoint = checkpoints[index]
      if (checkpoint.length > this.core.pendingIndexedLength) continue

      // signature is invalid
      if (!this._verify(checkpoint.length, checkpoint.signature, idx.core.key)) {
        continue
      }

      if (signed.length < thres) {
        signed.push(checkpoint.length)
        if (!length || checkpoint.length < length) length = checkpoint.length
        continue
      }

      if (checkpoint.length <= length) continue

      length = 0
      let sub = checkpoint.length

      for (let i = 0; i < signed.length; i++) {
        if (sub.length >= signed[i]) {
          const swap = signed[i]
          signed[i] = sub
          sub = swap[i]
        }

        if (!length || signed[i] < length) {
          length = signed[i]
        }
      }
    }

    return signed.length >= thres ? length : 0
  }

  getSignatures (indexers, length) {
    const index = this.core.likelyIndex
    const signatures = []
    const thres = (indexers.length >> 1) + 1

    for (let signer = 0; signer < indexers.length; signer++) {
      const checkpoints = indexers[signer].getLatestCheckpoint()
      if (!checkpoints || checkpoints.length <= index) continue

      const checkpoint = checkpoints[index]

      const signature = {
        signature: checkpoint.signature,
        length: checkpoint.length,
        signer
      }

      if (checkpoint.length === length) {
        if (signatures.length >= thres) signatures.pop()
        signatures.push(signature)
      } else if (checkpoint.length > length) {
        signatures.push(signature)
      }

      if (signatures.length >= thres) return signatures
    }

    return null
  }
}
