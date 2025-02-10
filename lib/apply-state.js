const SystemView = require('./system')
const ReadyResource = require('ready-resource')
const assert = require('nanoassert')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const messages = require('./messages')
const { partialSignature } = require('hypercore/lib/multisig.js')
const safetyCatch = require('safety-catch')

class CheckpointCore {
  constructor (view, core, signer) {
    this.view = view
    this.core = core
    this.signer = signer
    this.length = 0
    this.digest = null
    this.checkpoints = []
    this.closed = false
  }

  async update () {
    if (await this.updateCheckpoints()) await this.view.maybeSigned()
  }

  async updateCheckpoints () {
    if (this.core.length <= this.length || this.closed) return false

    const length = this.core.length
    const value = await this.core.get(length - 1)

    if (length <= this.length || this.closed || !value.digest) return false

    const [
      digest,
      checkpoints
    ] = await Promise.all([
      this._inflateDigest(length, value.digest),
      this._inflateCheckpoint(length, value.checkpoint)
    ])

    if (length <= this.length || this.closed || !digest || !checkpoints) return false

    this.length = length
    this.digest = digest
    this.checkpoints = checkpoints

    return true
  }

  signatures () { // if .checkpoints is mutated, copy here
    return this.checkpoints
  }

  updateBackground () {
    return this.update().catch(safetyCatch)
  }

  makeDigest (key, length) {
    if (this.digest && (this.digest.key === key || b4a.equals(this.digest.key, key))) {
      return { key: null, pointer: length - this.digest.at }
    }
    this.digest = { key, at: length }
    return { key, pointer: 0 }
  }

  async ready () {
    await this.core.ready()
    this.core.on('append', this.updateBackground.bind(this))
    if (this.core.writable) await this.updateCheckpoints()
    this.updateBackground()
  }

  signedLength () {
    return this.checkpoints.length ? this.checkpoints[0].length : 0
  }

  close () {
    this.closed = true
    return this.core.close()
  }

  async _inflateDigest (length, digest) {
    if (!digest) return null

    if (digest.pointer === 0) {
      return { key: digest.key, at: length }
    }

    const len = length - digest.pointer
    if (len === 0) return null

    const value = await this.core.get(len - 1)
    if (!value.digest || !value.digest.key) return null

    return { key: value.digest.key, at: len }
  }

  async _inflateCheckpoint (length, checkpoint) {
    if (!checkpoint) return []

    const resolved = new Array(checkpoint.length)
    for (let i = 0; i < resolved.length; i++) {
      // TODO: inflate if needed
      resolved[i] = checkpoint[i].checkpoint
    }

    return resolved
  }
}

module.exports = class ApplyState extends ReadyResource {
  constructor (base) {
    super()

    this.base = base
    this.encryption = this.base.encryption
    this.valueEncoding = base.valueEncoding
    this.store = base._viewStore.atomize()
    this.system = null
    this.view = null
    this.views = []
    this.systemRef = null

    this.updates = []

    this.indexersUpdated = false
    this.indexedLength = 0
    this.quorum = 0
    this.interrupted = false
    this.applying = false

    this.localCheckpoint = null
    this.checkpoints = []
    this.pendingViews = null
    this.dirty = false
  }

  _createCheckpointCore (key, active, signer) {
    const encryption = this.encryption
    const session = this.base.store.get({
      key,
      valueEncoding: messages.OplogMessage,
      encryption,
      active
    })

    return new CheckpointCore(this, session, signer)
  }

  async _open () {
    const boot = (await this.base._getBootRecord()) || { key: null, indexedLength: 0, indexersUpdated: false, heads: null }

    const sysCore = this.store.get({ name: '_system' })
    await sysCore.ready()
    await sysCore.setUserData('referrer', this.base.key)

    this.systemRef = this.store.getViewByName('_system')

    // reset so we dont track the _system
    this.store.opened = []

    const system = new SystemView(sysCore)
    await system.ready()

    const view = this.base._hasOpen ? this.base._handlers.open(this.store, this.base) : null

    this.system = system

    this.view = view
    this.indexedLength = boot.indexedLength
    this.indexersUpdated = boot.indexersUpdated
    this.quorum = sysCore.manifest ? sysCore.manifest.quorum : 0

    const added = new Set()

    for (let i = 0; i < system.views.length; i++) {
      const { key, length } = system.views[i]
      const v = await this.store.findViewByKey(key)

      if (v === null) {
        this.views.push({ name: null, key, length, core: null, ref: null })
        continue
      }

      await v.core.setUserData('referrer', this.base.key)

      this.views.push({ name: v.name, key, length, core: v.atomicBatch, ref: v })
      added.add(v)
    }

    for (const v of this.store.opened) {
      if (added.has(v)) continue
      this.views.push({ name: v.name, key: v.core.key, length: v.atomicBatch.length, core: v.atomicBatch, ref: v })
    }

    for (let i = 0; i < system.indexers.length; i++) {
      const idx = system.indexers[i]
      const chk = this._createCheckpointCore(idx.key, true, i)
      await chk.ready()
      this.checkpoints.push(chk)
    }

    const shouldSign = this.quorum > 0 // TODO: more conditions would be good

    if (shouldSign) await this._startLocalCheckpoint()

    await this._refreshWriters()

    // deferred
    this.maybeSigned().catch(noop)
  }

  async _startLocalCheckpoint () {
    for (const chk of this.checkpoints) {
      if (chk.core.id === this.base.local.id) {
        this.localCheckpoint = chk
        return
      }
    }

    this.localCheckpoint = this._createCheckpointCore(this.base.local.key, false, 0)
    await this.localCheckpoint.ready()
  }

  interrupt () {
    this.interrupted = true
  }

  async maybeSigned () {
    if (this.opened === false) await this.ready()
    if (this.interrupted) return

    const thres = this.checkpoints.length - this.quorum
    if (thres < 0 || this.checkpoints.length <= thres) return

    this.checkpoints.sort(cmpCheckpoints)

    const signableLength = this.checkpoints[thres].signedLength()
    if (signableLength <= this.system.core.signedLength) return
    if (signableLength > this.indexedLength) return

    const expected = this.system.core.key
    for (let i = thres; i < this.checkpoints.length; i++) {
      const chk = this.checkpoints[i]
      // we must agree on what the system is obvs
      if (!b4a.equals(expected, chk.digest.key)) return
      // if we dont have the indexed state ourself, then no way for us to verify / patch
      if (chk.checkpoints.length === 0 || chk.checkpoints[0].length > this.indexedLength) return
    }

    const chk = []
    for (let i = thres; i < this.checkpoints.length; i++) {
      chk.push({ signer: this.checkpoints[i].signer, signatures: this.checkpoints[i].signatures() })
    }

    await this._signAll(signableLength, chk)
  }

  async _signAll (signableLength, checkpoints) {
    const views = new Array(this.views.length + 1) // +1 is sys
    const info = await this.system.getIndexedInfo(signableLength)
    if (this.interrupted) return

    if (this.pendingViews && this.pendingViews[0].length >= signableLength) {
      return
    }

    for (let i = 0; i < views.length; i++) {
      const length = i === 0 ? signableLength : info.views[i - 1].length
      const core = i === 0 ? this.system.core : this.views[i - 1].core
      const ref = i === 0 ? this.systemRef : this.views[i - 1].ref

      if (!core || length <= core.signedLength) continue

      const sign = {
        length,
        core,
        ref,
        signatures: new Array(checkpoints.length),
        partials: new Array(checkpoints.length)
      }

      for (let j = 0; j < checkpoints.length; j++) {
        const { signer, signatures } = checkpoints[j]
        const { length, signature } = signatures[i]

        sign.signatures[j] = {
          signature,
          length,
          signer
        }
      }

      views[i] = sign
    }

    const promises = []
    for (const v of views) {
      if (!v) continue

      for (let i = 0; i < v.signatures.length; i++) {
        promises.push(setPartialSignature(v, i))
      }
    }

    await Promise.all(promises)
    if (this.interrupted) return

    if (this.pendingViews && this.pendingViews[0].length >= signableLength) {
      return
    }

    this.pendingViews = views
    this.dirty = true

    // TODO: only needed if not updating, wont crash so keeping for now, just less efficient
    this.base._queueBump()
  }

  async _close () {
    this.interrupted = true
    for (const chk of this.checkpoints) await chk.close()
    if (this.base._hasClose) await this.base._handlers.close(this.view)

    const promises = []
    for (const v of this.views) promises.push(v.ref.release())
    promises.push(this.systemRef.release())

    await Promise.all(promises)
  }

  async catchup (linearizer) {
    if (!this.opened) await this.ready()
    if (!this.system.heads.length) return

    const writers = new Map()

    const { nodes, updates } = await this.system.history(this.indexedLength)
    const sys = await this.system.checkout(this.indexedLength)

    for (const node of nodes) {
      const hex = b4a.toString(node.key, 'hex')

      let w = writers.get(hex)

      if (w === undefined) { // TODO: we actually have all the writer info already but our current methods make it hard to reuse that
        w = await this.base._getWriterByKey(node.key, -1, 0, true, false, sys)
        writers.set(hex, w)
      }

      if (w === null) continue

      while (w.length < node.length) {
        await w.update(true)

        const node = w.advance()
        if (!node) break

        linearizer.addHead(node)
      }
    }

    await sys.close()

    this.updates = updates

    if (this.indexersUpdated) {
      this.indexersUpdated = false
      // if we updated the indexers, invalidate all the internal state and reapply it
      await this.truncate(this.indexedLength)
      this._rollbackViews()

      while (this.updates.length > 0 && this.updates[this.updates.length - 1].systemLength > this.indexedLength) {
        this.updates.pop()
      }
      return
    }

    // otherwise we know the internal state is correct, so we carry on
    linearizer.update()
  }

  async getIndexedSystem () {
    if (this.opened === false) await this.ready()

    const sys = await this.system.checkout(this.indexedLength)
    await sys.ready()
    return sys
  }

  bootstrap () {
    return this.system.add(this.base.bootstrap, { isIndexer: true, isPending: false })
  }

  async undo (popped) {
    if (!popped) return

    let indexersUpdated = false
    while (popped > 0) {
      const u = this.updates.pop()
      popped -= u.batch
      if (u.indexers) indexersUpdated = true
    }

    const u = this.updates.length === 0 ? null : this.updates[this.updates.length - 1]
    const systemLength = u ? u.systemLength : this.indexedLength

    await this.truncate(systemLength)
    if (indexersUpdated) this._rollbackViews()
  }

  async truncate (systemLength) {
    if (this.opened === false) await this.ready()
    if (systemLength === this.system.core.length) return

    const { views } = await this.system.getIndexedInfo(systemLength)

    for (let i = 0; i < this.views.length; i++) {
      const { core } = this.views[i]

      if (core.length === views[i].length) continue
      await core.truncate(views[i].length)
    }

    await this.system.core.truncate(systemLength)
    await this._updateSystem()
  }

  async _refreshWriters () {
    for (const w of this.base.activeWriters) {
      const data = await this.system.get(w.core.key)
      const bootstrapper = this.system.core.length === 0 && b4a.equals(w.core.key, this.base.bootstrap)
      const isRemoved = data ? data.isRemoved : !bootstrapper
      w.isRemoved = isRemoved
    }
  }

  async _updateSystem () {
    if (!(await this.system.update())) return
    await this._refreshWriters()
    this._rollbackViews()
  }

  async _checkpointCoreAt (core, length) {
    const batch = await core.restoreBatch(length)
    batch.fork = 0 // views never fork, so always sign the non batch state by force
    const signature = crypto.sign(batch.signable(core.key), this.base.local.keyPair.secretKey)
    return { signature, length }
  }

  async _checkpoint () {
    const info = await this.system.getIndexedInfo(this.indexedLength)
    const checkpoint = []

    checkpoint.push({
      checkpointer: 0,
      checkpoint: await this._checkpointCoreAt(this.system.core, this.indexedLength)
    })

    for (let i = 0; i < info.views.length; i++) {
      const { core } = this.views[i]

      if (core) {
        checkpoint.push({
          checkpointer: 0,
          checkpoint: await this._checkpointCoreAt(core, info.views[i].length)
        })
      } else {
        checkpoint.push({
          checkpointer: 0,
          checkpoint: null
        })
      }
    }

    return checkpoint
  }

  async finalize (key) {
    const local = this.store.getLocal()
    const pointer = c.encode(messages.BootRecord, {
      key,
      indexedLength: this.indexedLength,
      indexersUpdated: true,
      heads: null // just compat field, never used
    })

    await local.setUserData('autobase/boot', pointer)
    await this.store.flush()
    await this.close()
  }

  async _flush (localNodes) {
    if (localNodes) await this._appendLocalNodes(localNodes)

    const local = this.store.getLocal()

    const pointer = c.encode(messages.BootRecord, {
      key: this.system.core.key,
      indexedLength: this.indexedLength,
      indexersUpdated: false
    })

    await local.setUserData('autobase/boot', pointer)

    if (this.localCheckpoint) {
      await this.store.flush() // seems like a bug that this is needed...
      await this.localCheckpoint.update()
    }

    if (this.pendingViews) {
      const views = this.pendingViews
      this.pendingViews = null

      for (const v of views) {
        if (!v || v.length <= v.core.signedLength) continue
        const signature = v.core.core.verifier.assemble(v.partials)
        await v.ref.commit(this.store.atom, v.length, signature)
        this.base.updating = true
      }
    }

    await this.store.flush()
    if (this.pendingViews === null) this.dirty = false
  }

  _indexUpdates (indexed) {
    let shift = 0
    while (indexed > 0) indexed -= this.updates[shift++].batch

    this.indexedLength = this.updates[shift - 1].systemLength
    this.updates.splice(0, shift)

    // we should prop have a flag or similar to know if this is needed
    // ie only if the signer bailed before due to missing indexedlength
    this.maybeSigned().catch(noop)
  }

  async _assertNode (node, batch) {
    // helpful dag helper, so kept here

    const v = (await this.system.get(node.writer.core.key)) || { length: 0, isRemoved: false }
    const expected = v.length + batch
    if (node.length === expected) return

    console.trace('INVALID_INSERTION',
      'length=', node.length,
      'key=', node.writer.core.key,
      'local=', node.writer.core.writable,
      'batch=', batch,
      'dag=', v
    )

    process.exit(1)
  }

  async update (u, localNodes) {
    let batch = 0
    let applyBatch = []
    let indexersUpdated = 0

    let j = 0
    let i = 0

    while (i < Math.min(u.indexed.length, u.shared)) {
      const node = u.indexed[i++]

      if (node.batch > 1) continue
      this.base._shiftWriter(node.writer)

      const update = this.updates[j++]

      if (update.indexers) {
        indexersUpdated = i
        break
      }
    }

    if (u.undo) await this.undo(u.undo)

    await this._updateSystem()

    for (i = u.shared; i < u.length; i++) {
      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      // if (node.writer === this.localWriter) {
      //   this._resetAckTick()
      // } else if (!indexed) {
      //   this._ackTick++
      // }

      batch++

      // we allow processing removed writers, incase someone linked to them during a rebase
      // but they can NEVER have a side-effect. only exception is nodes who are linked by other writers
      // who are allowed to write (indexers and active writers).
      if (node.writer.isRemoved && !indexed && !node.isLinked()) {
        if (node.batch > 1) continue
        // in case someone is linking this node and they are not removed
        this.updates.push({ batch, indexers: false, systemLength: this.system.core.length })
        batch = 0
        assert(applyBatch.length === 0, 'Apply batch should not have been modified')
        continue
      }

      // TODO: in prod we prop need to disable this assertion as its pretty expensive, but it catches a lot of bugs
      await this._assertNode(node, batch)

      this.system.addHead(node)

      if (node.value !== null && !node.writer.isRemoved) {
        applyBatch.push({
          indexed,
          from: node.writer.core,
          length: node.length,
          value: node.value,
          heads: node.actualHeads
        })
      }

      if (node.batch > 1) continue

      const update = {
        batch,
        views: [],
        indexers: false,
        systemLength: -1
      }

      this.updates.push(update)

      if (this.system.bootstrapping) await this.bootstrap()

      if (applyBatch.length && this.base._hasApply === true) {
        this.applying = true
        await this.base._handlers.apply(applyBatch, this.view, this.base)
        this.applying = false
      }

      update.indexers = !!this.system.indexerUpdate

      if (this.system.indexerUpdate) await this._generateNextViews()

      await this.system.flush(this.views)
      await this.system.update()

      batch = 0
      applyBatch = []
      update.systemLength = this.system.core.length

      if (!indexed || indexersUpdated) continue

      this.base._shiftWriter(node.writer)

      if (update.indexers) {
        indexersUpdated = i + 1
      }
    }

    if (u.indexed.length) {
      this._indexUpdates(indexersUpdated || u.indexed.length)
    }

    await this._flush(localNodes)

    return indexersUpdated !== 0
  }

  flush () {
    return this._flush(null)
  }

  async _appendLocalNodes (localNodes) {
    const checkpoint = this.localCheckpoint ? await this._checkpoint() : null

    const blocks = new Array(localNodes.length)
    const local = this.store.getLocal()

    if (!local.opened) await local.ready()

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch } = localNodes[i]
      const length = local.length + i + 1
      // const checkpoint = null //, this._addCheckpoints ? await generateCheckpoint(cores, indexed, info) : null

      blocks[i] = {
        version: 1,
        maxSupportedVersion: this.base.maxSupportedVersion,
        checkpoint: i === blocks.length - 1 ? checkpoint : null,
        digest: this.localCheckpoint ? this.localCheckpoint.makeDigest(this.system.core.key, length) : null,
        node: {
          heads,
          batch,
          value: value === null ? null : c.encode(this.base.valueEncoding, value)
        },
        trace: []
      }
    }

    await local.append(blocks)
  }

  _rollbackViews () {
    for (let i = 0; i < this.system.views.length; i++) {
      this.views[i].key = this.system.views[i].key
    }
  }

  async _generateNextViews () {
    // note, this is very state dependent so can ONLY be called at the exact time an indexer upgrade occurs
    const manifests = await this.store.getIndexerManifests(this.system.indexers)

    for (const v of this.views) {
      const prologue = await getPrologue(v)
      const key = await this.store.createView(manifests, prologue, v.name)

      v.key = key
    }
  }
}

async function getPrologue (view) {
  const length = view.core ? view.core.length : view.length
  if (!length) return null

  const batch = await view.core.restoreBatch(length)

  return {
    hash: batch.hash(),
    length
  }
}

function noop () {}

function cmpCheckpoints (a, b) {
  return a.signedLength() - b.signedLength()
}

async function setPartialSignature (view, index) {
  const sig = view.signatures[index]
  if (sig.length < view.length) await view.core.get(sig.length - 1)
  view.partials[index] = await partialSignature(view.core, sig.signer, view.length, sig.length, sig.signature)
}
