const SystemView = require('./system')
const ReadyResource = require('ready-resource')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const messages = require('./messages')
const { partialSignature } = require('hypercore/lib/multisig.js')

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
    if (this.core.length <= this.length || this.closed) return

    const length = this.core.length
    const value = await this.core.get(length - 1)

    if (length <= this.length || this.closed || !value.checkpoint) return

    const checkpoints = await this._inflateCheckpoint(length, value.checkpoint)

    if (length <= this.length || this.closed) return

    this.length = length
    this.checkpoints = checkpoints

    await this.view.maybeSigned()
  }

  signatures () { // if .checkpoints is mutated, copy here
    return this.checkpoints
  }

  updateBackground () {
    return this.update().catch(noop)
  }

  async ready () {
    await this.core.ready()
    this.core.on('download', this.updateBackground.bind(this))
    this.updateBackground()
  }

  signedLength () {
    return this.checkpoints.length ? this.checkpoints[0].length : 0
  }

  close () {
    this.closed = true
    return this.core.close()
  }

  async _inflateCheckpoint (length, checkpoint) {
    const resolved = new Array(checkpoint.length)
    for (let i = 0; i < resolved.length; i++) {
      // TODO: inflate if needed
      resolved[i] = checkpoint[i].checkpoint
    }

    return resolved
  }
}

module.exports = class ViewState extends ReadyResource {
  constructor (base) {
    super()

    this.base = base
    this.encryption = this.base.encryption
    this.valueEncoding = base.valueEncoding
    this.store = base._viewStore.atomize()
    this.system = null
    this.systemKey = null
    this.view = null
    this.views = []

    this.updates = []

    this.indexedLength = 0
    this.quorum = 0

    this.localCheckpoint = null
    this.checkpoints = []
    this.pendingViews = null
  }

  _createCheckpointCore (key, active, signer) {
    const encryption = this.encryption
    const session = this.base.store.get({
      key,
      valueEncoding: messages.OplogMessage,
      encryption: this.encryption,
      active
    })

    return new CheckpointCore(this, session, signer)
  }

  async _open () {
    const info = await this.base._getBootInfo()

    const sysCore = this.store.get({ name: '_system' })
    await sysCore.ready()

    // reset so we dont track the _system
    this.store.opened = []

    const system = new SystemView(sysCore)
    const view = this.base._hasOpen ? this.base._handlers.open(this.store, this.base) : null

    this.system = system
    this.systemKey = info.bootstrap

    this.view = view
    this.indexedLength = info.indexedLength
    this.quorum = sysCore.manifest ? sysCore.manifest.quorum : 0

    await system.ready()

    const views = system.views
    const added = new Set()

    for (let i = 0; i < system.views.length; i++) {
      const { key, length } = system.views[i]

      const v = await this.store.findViewByKey(key)

      if (v === null) {
        this.views.push({ name: null, key, length, core: null, ref: null })
        continue
      }

      this.views.push({ name: v.name, key, length, core: v.atomicBatch, ref: v })
      added.add(v)
    }

    for (const v of this.store.opened) {
      if (added.has(v)) continue
      this.views.push({ name: v.name, key: v.core.key, length: v.atomicBatch.length, core: v.atomicBatch, ref: v })
    }

    for (let i = 0; i < info.indexers.length; i++) {
      const idx = info.indexers[i]
      const chk = this._createCheckpointCore(idx.key, true, i)
      await chk.ready()
      this.checkpoints.push(chk)
    }

    const shouldSign = this.quorum > 0 // TODO: more conditions would be good

    if (shouldSign) await this._startLocalCheckpoint()

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
    await this.localCheckpoint.update()
  }

  async maybeSigned () {
    if (this.opened === false) await this.ready()

    const thres = this.checkpoints.length - this.quorum
    if (this.checkpoints.length <= thres) return

    this.checkpoints.sort(cmpCheckpoints)

    const signableLength = this.checkpoints[thres].signedLength()
    if (signableLength <= this.system.core.signedLength) return
    if (signableLength > this.indexedLength) return

    const chk = []
    for (let i = thres; i < this.checkpoints.length; i++) {
      chk.push({ signer: this.checkpoints[i].signer, signatures: this.checkpoints[i].signatures() })
    }

    await this._signAll(signableLength, chk)
  }

  async _signAll (signableLength, checkpoints) {
    const views = new Array(this.views.length + 1) // +1 is sys
    const info = await this.system.getIndexedInfo(signableLength)

    if (this.pendingViews && this.pendingViews[0].length >= signableLength) {
      return
    }

    for (let i = 0; i < views.length; i++) {
      const length = i === 0 ? signableLength : info.views[i - 1].length
      const core = i === 0 ? this.system.core : this.views[i - 1].core
      const ref = i === 0 ? this.store.getViewByName('_system') : this.views[i - 1].ref

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

    if (this.pendingViews && this.pendingViews[0].length >= signableLength) {
      return
    }

    this.pendingViews = views

    // TODO: only needed if not updating, wont crash so keeping for now, just less efficient
    this.base._queueBump()
  }

  async _close () {
    for (const chk of this.checkpoints) await chk.close()
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

  undo (popped) {
    if (!popped) return Promise.resolve()

    while (popped > 0) popped -= this.updates.pop().batch

    const u = this.updates.length === 0 ? null : this.updates[this.updates.length - 1]
    const systemLength = u ? u.systemLength : this.indexedLength

    return this.truncate(systemLength)
  }

  async truncate (systemLength) {
    if (this.opened === false) await this.ready()
    if (systemLength === this.system.core.length) return

    const { views } = await this.system.getIndexedInfo(systemLength)

    for (let i = 0; i < this.views.length; i++) {
      const v = this.views[i]
      await v.core.truncate(v.length)
    }

    await this.system.core.truncate(systemLength)
  }

  async preupdate () {
    if (this.opened === false) await this.ready()
    if (!(await this.system.update())) return

    for (const w of this.base.activeWriters) {
      const data = await this.system.get(w.core.key)
      w.isRemoved = data ? data.isRemoved : false
    }
  }

  async _checkpointCoreAt (core, length, sys) {
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

  flush () {
    return this._flush(null)
  }

  async finalize (key) {
    const local = this.store.getLocal()
    const pointer = c.encode(messages.BootRecord, { key, indexedLength: this.indexedLength, views: [] })
    await local.setUserData('autobase/boot', pointer)

    await this.store.flush()
  }

  async _flush (localNodes) {
    if (localNodes) await this._appendLocalNodes(localNodes)

    const local = this.store.getLocal()
    const pointer = c.encode(messages.BootRecord, { key: this.systemKey, indexedLength: this.indexedLength, views: [] })
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
      }
    }

    await this.store.flush()
  }

  _indexUpdates (indexed) {
    let shift = 0
    while (indexed > 0) indexed -= this.updates[shift++].batch

    this.indexedLength = this.updates[shift - 1].systemLength
    this.updates.splice(0, shift)
  }

  async update (u, localNodes) {
    let batch = 0
    let applyBatch = []

    let j = 0
    let i = 0

    while (i < Math.min(u.indexed.length, u.shared)) {
      const node = u.indexed[i++]

      if (node.batch > 1) continue
      this.base._shiftWriter(node.writer)

      const update = this.updates[j++]
      if (!update.indexers) continue

      this._indexUpdates(i)

      await this._flush(localNodes)

      return true
    }

    if (u.undo) await this.undo(u.undo)

    await this.preupdate()

    for (i = u.shared; i < u.length; i++) {
      const indexed = i < u.indexed.length
      const node = indexed ? u.indexed[i] : u.tip[i - u.indexed.length]

      // if (node.writer === this.localWriter) {
      //   this._resetAckTick()
      // } else if (!indexed) {
      //   this._ackTick++
      // }

      batch++

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
        await this.base._handlers.apply(applyBatch, this.view, this.base)
      }

      update.indexers = !!this.system.indexerUpdate

      if (this.system.indexerUpdate) await this._updateViews()

      await this.system.flush(this.views)
      await this.system.update()

      batch = 0
      applyBatch = []
      update.systemLength = this.system.core.length

      if (!indexed) continue

      this.base._shiftWriter(node.writer)

      if (!update.indexers) continue

      // indexer set has updated
      this._indexUpdates(i + 1)

      await this._flush(localNodes)

      return true
    }

    if (u.indexed.length) {
      this._indexUpdates(u.indexed.length)
    }

    await this._flush(localNodes)

    return false
  }

  async _appendLocalNodes (localNodes) {
    const checkpoint = this.localCheckpoint ? await this._checkpoint() : null
    const digest = { pointer: 0, key: this.systemKey }

    const blocks = new Array(localNodes.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch } = localNodes[i]
      // const checkpoint = null //, this._addCheckpoints ? await generateCheckpoint(cores, indexed, info) : null

      blocks[i] = {
        version: 1,
        maxSupportedVersion: this.base.maxSupportedVersion,
        checkpoint: i === blocks.length - 1 ? checkpoint : null,
        digest: i === blocks.length - 1 ? digest : null,
        node: {
          heads,
          batch,
          value: value === null ? null : c.encode(this.base.valueEncoding, value)
        },
        trace: []
      }
    }

    await this.store.getLocal().append(blocks)
  }

  async _updateViews () {
    const manifests = await this.store.getIndexerManifests(this.system.indexers)

    for (const v of this.views) {
      const prologue = await getPrologue(v)
      const key = await this.store.createView(manifests, prologue, v.name)

      v.key = key
    }
  }
}

async function getPrologue (view) {
  if (!view.length) return null

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
