const BufferMap = require('tiny-buffer-map')

const Clock = require('./clock')

const UNSEEN = 0
const NEWER = 1
const ACKED = 2

// Consensus machine for Autobase. Sort DAG nodes using
// vector clocks to determine a globally consistent view

module.exports = class Consensus {
  constructor (indexers) {
    this.merges = new Set()
    this.majority = (indexers.length >>> 1) + 1
    this.indexers = indexers
    this.removed = new Clock()
    this.updated = false

    this.writers = new BufferMap()
    for (const idx of this.indexers) {
      this.writers.set(idx.core.key, idx)
    }
  }

  addHead (node) {
    if (!node.writer.isActiveIndexer) return
    if (this._isMerge(node)) this.merges.add(node)
    this.updated = true
    return node
  }

  /* Indexer Only DAG methods */

  _tails (node, tails) {
    const tailSet = new Set()
    for (const t of tails) {
      if (node.clock.includes(t.writer.core.key, t.length)) tailSet.add(t)
    }

    return tailSet
  }

  _tailsAndMerges (node, tails) {
    const all = this._tails(node, tails)
    for (const m of this.merges) {
      if (m !== node && node.clock.includes(m.writer.core.key, m.length)) {
        all.add(m)
      }
    }
    return all
  }

  _isMerge (node) {
    if (!node.writer.isActiveIndexer) return false

    const deps = []

    for (const idx of this.indexers) {
      let seq = node.clock.get(idx.core.key) - 1

      if (idx === node.writer) seq--

      const head = idx.get(seq)
      if (!head || this.removed.includes(head.writer.core.key, head.length)) continue

      let isDep = true
      for (let i = 0; i < deps.length; i++) {
        const d = deps[i]
        if (d === head) continue

        if (d.clock.includes(head.writer.core.key, head.length)) {
          isDep = false
          break
        }

        if (head.clock.includes(d.writer.core.key, d.length)) {
          const popped = deps.pop()
          if (d === popped) continue
          deps[i--] = popped
        }
      }

      if (isDep) deps.push(head)
    }

    return deps.length > 1
  }

  _indexerTails () {
    const tails = new Set()
    for (const idx of this.indexers) {
      const length = this.removed.has(idx.core.key) ? this.removed.get(idx.core.key) : idx.indexed

      const head = idx.get(length)
      if (!head || this.removed.includes(head.writer.core.key, head.length)) continue

      let isTail = true
      for (const t of tails) {
        if (head.clock.includes(t.writer.core.key, t.length)) {
          isTail = false
          break
        }

        if (t.clock.includes(head.writer.core.key, head.length)) {
          tails.delete(t)
        }
      }

      if (isTail) tails.add(head)
    }

    return tails
  }

  // parent is newer if for any node in parent's view,
  // either node can see object or object can see node
  _strictlyNewer (object, parent) {
    for (const [key, latest] of parent.clock) {
      const oldest = this.removed.get(key)
      if (latest <= oldest) continue // check quickly if we removed it

      // get the NEXT mode from the writer from the objects pov, adjust if its removed
      let length = object.clock.get(key)
      if (length <= oldest) length = oldest

      // sanity check, likely not needed as someone has checked this before, but whatevs, free
      if (latest < length) return false

      // if the same, they both seen it, continue
      if (latest === length) continue

      const writer = this.writers.get(key)

      // might not be in the removed set but the writer can tell us if it was indexed...
      const next = writer && writer.get(length >= writer.indexed ? length : writer.indexed)

      // no next, its been indexed, both seen it
      if (!next) continue

      // if the NEXT node has seen the object its fine - newer
      if (next.clock.includes(object.writer.core.key, object.length)) continue

      // otherwise the parent must also NOT has seen the next node
      if (!parent.clock.includes(next.writer.core.key, next.length)) continue

      return false
    }

    return parent.clock.get(object.writer.core.key) >= object.length
  }

  _acks (target) {
    const acks = target.writer.isActiveIndexer ? [target] : [] // TODO: can be cached on the target node in future (ie if we add one we dont have to check it again)

    for (const idx of this.indexers) {
      if (idx === target.writer) continue

      let next = target.clock.get(idx.core.key)
      if (next < idx.nodes.offset) next = idx.nodes.offset

      const nextIndexNode = idx.get(next >= idx.indexed ? next : idx.indexed)

      // no node - no ack
      if (!nextIndexNode) continue

      // if the next index node does not see the target, no ack
      if (!nextIndexNode.clock.includes(target.writer.core.key, target.length)) continue

      // if the next index node is not strictly newer, skip to avoid ambig...
      if (!this._strictlyNewer(target, nextIndexNode)) continue

      acks.push(nextIndexNode)
    }

    return acks
  }

  acksFromNode (target, view) {
    const acks = new Set()

    if (!view || !view.clock.includes(target.writer.core.key, target.length)) return acks

    acks.add(view.writer)

    for (const idx of this.indexers) {
      if (idx === view.writer) continue

      const length = view.clock.get(idx.core.key)
      if (!length) continue

      if (target.clock.includes(idx.core.key, length)) continue

      const head = idx.get(length - 1)
      if (!head) continue

      if (head.clock.includes(target.writer.core.key, target.length)) {
        acks.add(idx)
      }
    }

    return acks
  }

  _ackedAt (acks, parent) {
    let seen = 0
    let missing = acks.length

    for (const node of acks) {
      missing--

      if (!parent.clock.includes(node.writer.core.key, node.length)) {
        if (seen + missing < this.majority) return false
        continue
      }

      if (++seen >= this.majority) return true
    }

    return false
  }

  confirms (indexer, target, acks, length) {
    if (!length || this.removed.get(indexer.core.key) >= length) return UNSEEN
    // def feels like there is a smarter way of doing this part
    // ie we just wanna find a node from the indexer that is strictly newer than target
    // and seens a maj of the acks - thats it

    let jump = true
    let newer = true

    for (let i = length - 1; i >= 0; i--) {
      const head = indexer.get(i)
      if (head === null) return UNSEEN

      let seen = 0

      for (const node of acks) {
        // if (node.writer === indexer) continue
        if (!head.clock.includes(node.writer.core.key, node.length)) continue
        if (++seen >= this.majority) break
      }

      if (!newer && seen < this.majority) {
        break
      }

      if (!this._strictlyNewer(target, head)) {
        // all strictly newer nodes are clustered together so bisect until we find the cluster
        if (jump) {
          jump = false

          let t = length - 1
          let b = 0

          while (t > b) {
            const mid = (t + b) >>> 1
            const node = indexer.get(mid)

            if (node === null || !node.clock.includes(target.writer.core.key, target.length) || this._strictlyNewer(target, node)) {
              b = mid + 1
            } else {
              t = mid - 1
            }
          }

          // + 2 in case we are off by one and the i--. its fine, just an optimisation
          if (b + 1 < i) i = b + 2
        }

        newer = false
        continue
      } else if (seen < this.majority) {
        return NEWER
      }

      return ACKED
    }

    return UNSEEN
  }

  _isConfirmed (target, parent = null) {
    const acks = this._acks(target)
    const confs = new Set()

    if (acks.length < this.majority) return false
    let allNewer = true

    for (const indexer of this.indexers) {
      const length = parent
        ? (parent.writer === indexer) ? parent.length - 1 : parent.clock.get(indexer.core.key)
        : indexer.length

      const result = this.confirms(indexer, target, acks, length)

      if (result === ACKED) {
        confs.add(indexer)
        if (confs.size >= this.majority) {
          return true
        }
      }

      if (result === UNSEEN) allNewer = false
    }

    if (parent) return this._isConfirmableAt(target, parent, acks, confs)

    return allNewer
  }

  _isConfirmableAt (target, parent, acks, confs) {
    if (!this._ackedAt(acks, parent)) return false

    let potential = confs.size

    for (const indexer of this.indexers) {
      if (confs.has(indexer)) continue

      const length = parent.clock.get(indexer.core.key)
      const isSeen = target.clock.includes(indexer.core.key, length)

      // if the target has seen the latest node, it can freely be used to confirm the target later
      // otherwise, check if a newer node is strictly newer...
      if (!isSeen) {
        const head = indexer.get(length - 1)

        // the next indexer head HAS to be strictly newer - meaning the current one has to be also.
        if (head && !this.removed.includes(head.writer.core.key, head.length) && !this._strictlyNewer(target, head)) {
          continue
        }
      }

      if (++potential >= this.majority) return true
    }

    return false
  }

  // this can get called multiple times for same node
  remove (node) {
    this.merges.delete(node)
    this.removed.set(node.writer.core.key, node.length)
    return node
  }

  shift () {
    if (!this.updated) return []

    const tails = this._indexerTails()

    for (const tail of tails) {
      if (this._isConfirmed(tail)) {
        return [this.remove(tail)]
      }
    }

    for (const merge of this.merges) {
      if (this._isConfirmed(merge)) {
        return this._yieldNext(merge, tails)
      }
    }

    this.updated = false
    return []
  }

  // yields next indexer node
  _yieldNext (node, tails) {
    // only stop when we find a tail
    while (!tails.has(node)) {
      let next = null

      // for merges check if one fork is confirmed
      for (const t of this._tailsAndMerges(node, tails)) {
        if (this._isConfirmed(t, node)) {
          next = t
          break
        }
      }

      if (next) {
        node = next
        continue
      }

      // otherwise yield all tails
      const tailSet = []
      for (const t of this._tails(node, tails)) {
        tailSet.push(this.remove(t))
      }

      return tailSet
    }

    return [this.remove(node)]
  }

  shouldAck (writer) {
    for (const t of this._indexerTails()) {
      if (t.writer === writer) continue
      if (this._shouldAckNode(t, writer)) return true
    }

    return false
  }

  _shouldAckNode (target, writer) {
    const head = writer.head()
    const next = target.clock.get(writer.core.key)
    const nextIndexNode = writer.get(next >= writer.indexed ? next : writer.indexed)

    // if we have no next node and we didn't write target then ack
    if (!nextIndexNode && writer !== target.writer) return true

    // shortcuts if we have next node
    if (nextIndexNode) {
      // if the next node does not see the target, should ack
      if (!nextIndexNode.clock.includes(target.writer.core.key, target.length)) {
        return !head.clock.includes(target.writer.core.key, target.length)
      }

      // if the next node is not strictly newer, no point acking
      if (!this._strictlyNewer(target, nextIndexNode)) return false
    }

    // now check if we can double confirm
    const acks = this._acks(target)

    // need enough to double confirm
    if (acks.length >= this.majority) {
      return this.confirms(writer, target, acks, writer.length) === UNSEEN
    }

    return false
  }
}
