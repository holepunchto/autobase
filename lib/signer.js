const { assemble, partialSignature } = require('hypercore/lib/multisig.js')
const BufferMap = require('tiny-buffer-map')

module.exports = class Signer {
  constructor (base, core) {
    this.base = base
    this.core = core

    this.signatures = new BufferMap()
    this._pendingSignatures = new BufferMap()
  }

  async sign (indexers, length) {
    const signatures = this.getSignatures(indexers, length)

    const nodes = this.core.indexBatch(0, this.core.nodes.length - this.core._shifted)
    const tree = this.core.core.createTreeBatch(this.core.pendingIndexedLength, nodes)
    const p = await Promise.all(signatures.map(s => partialSignature(tree, s.signer, length, s.length, s.signature)))
    return assemble(p)
  }

  // triggered by base
  _oncheckpoint ({ indexer, checkpoint }) {
    const { length } = checkpoint

    // signature is behind
    if (length < this.core.signedLength) return false

    // we have a newer signature for this indexer
    if (this.signatures.has(indexer)) {
      if (this.signatures.get(indexer).length >= length) return false
    }

    // TODO: surely we can sign up to pendingIndexedLength?
    if (length > this.core.pendingIndexedLength) {
      let pending = this._pendingSignatures.get(indexer)
      if (!pending) {
        pending = []
        this._pendingSignatures.set(indexer, pending)
      }
      pending.push(checkpoint)
    } else {
      this.signatures.set(indexer, checkpoint)
    }

    this.refresh()

    if (this.core.indexers && this.getSignableLength() > this.core.signedLength) {
      return true
    }

    return false
  }

  refresh () {
    for (const [writer, pending] of this._pendingSignatures) {
      let replace = false
      let ck = this.signatures.get(writer)

      while (pending.length) {
        if (pending[0].length > this.core.pendingIndexedLength) break
        replace = true
        ck = pending.shift()
      }

      if (!replace) continue
      this.signatures.set(writer, ck)
    }
  }

  _verify (length, signature, key) {
    if (!this.core.core || length > this.core.core.length) return false
    if (length < this.core.core.length) return true
    const batch = this.core.core.createTreeBatch(length)
    const { publicKey, namespace } = this.base.getNamespace(key, this.core)
    return batch.tree.crypto.verify(batch.signable(namespace), signature, publicKey)
  }

  getSignableLength (indexers = this.core.indexers) {
    let length = 0
    const signed = []
    const thres = (indexers.length >> 1) + 1

    for (const idx of indexers) {
      const checkpoint = this.signatures.get(idx.core.key)
      if (!checkpoint) continue

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
    const signatures = []
    const thres = (indexers.length >> 1) + 1

    for (let signer = 0; signer < indexers.length; signer++) {
      const checkpoint = this.signatures.get(indexers[signer].core.key)
      if (!checkpoint) continue

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
