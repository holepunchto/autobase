const ReadyResource = require('ready-resource')
const assert = require('nanoassert')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const safetyCatch = require('safety-catch')

const SystemView = require('./system')
const UpdateChanges = require('./updates.js')
const messages = require('./messages.js')
const { PrivateApplyCalls } = require('./apply-calls.js')
const { partialSignature } = require('hypercore/lib/multisig.js')

// must not be set too low (stretched with num of indexers)
const SHOULD_WRITE_THRESHOLD = 16

class CheckpointCore {
  constructor (view, core, signer, paused) {
    this.view = view
    this.core = core
    this.signer = signer
    this.length = 0
    this.digest = null
    this.signatures = []
    this.closed = false
    this.paused = paused
  }

  pause () {
    this.paused = true
  }

  resume () {
    if (!this.paused) return
    this.paused = false
    this.updateBackground()
  }

  async update () {
    if (await this.updateCheckpoints()) await this.view.maybeSigned()
  }

  async updateCheckpoints () {
    if (this.core.length <= this.length || this.closed || this.paused) return false

    const length = this.core.length
    const value = await this.core.get(length - 1)

    if (length <= this.length || this.closed || !value.digest || this.paused) return false

    const [
      digest,
      checkpoints
    ] = await Promise.all([
      this._inflateDigest(length, value.digest),
      this._inflateCheckpoints(length, value.checkpoint)
    ])

    if (length <= this.length || this.closed || !digest || !checkpoints || this.paused) return false
    for (let i = 0; i < checkpoints.length; i++) {
      if (checkpoints[i] === null) return false
    }

    this.length = length
    this.digest = digest
    this.signatures = checkpoints

    return true
  }

  updateBackground () {
    return this.update().catch(safetyCatch)
  }

  updateSignatures (length, signatures) {
    const updated = new Array(signatures.length)

    for (let i = 0; i < updated.length; i++) {
      const sig = signatures[i]
      updated[i] = sig
        ? { signature: sig.signature, length: sig.length, at: length }
        : this.signatures[i]
    }

    this.signatures = updated
  }

  makeCheckpoints (length) {
    const checkpoints = new Array(this.signatures.length)

    for (let i = 0; i < this.signatures.length; i++) {
      const chk = this.signatures[i]
      const checkpointer = length - chk.at

      checkpoints[i] = {
        checkpointer,
        checkpoint: checkpointer === 0
          ? { signature: chk.signature, length: chk.length }
          : null
      }
    }

    return checkpoints
  }

  makeDigest (length, key) {
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
    return this.signatures.length ? this.signatures[0].length : 0
  }

  close () {
    this.closed = true
    return this.core.close()
  }

  async _inflateDigest (length, dig) {
    if (!dig) return null

    if (dig.pointer === 0) {
      return { key: dig.key, at: length }
    }

    const len = length - dig.pointer

    if (this.digest && len === this.digest.at) return this.digest
    if (len <= 0) return null

    const { digest } = await this.core.get(len - 1)
    if (!digest || !digest.key) return null

    return { key: digest.key, at: len }
  }

  _sameSignatures (b) {
    const a = this.signatures
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i++) {
      if (a[i].length !== b[i].length) return false
    }
    return true
  }

  _inflateCheckpoints (length, checkpoints) {
    if (!checkpoints) return []
    if (this._sameSignatures(checkpoints)) return this.signatures

    const promises = new Array(checkpoints.length)
    for (let i = 0; i < promises.length; i++) {
      const chk = checkpoints[i]
      promises[i] = this._inflateCoreCheckpoint(chk, i, length)
    }

    return Promise.all(promises)
  }

  async _inflateCoreCheckpoint (chk, i, coreLength) {
    if (chk.checkpoint) {
      const { signature, length } = chk.checkpoint
      return { signature, length, at: coreLength }
    }

    const len = coreLength - chk.checkpointer
    if (i < this.signatures.length && this.signatures[i].at === len) {
      return this.signatures[i]
    }

    if (len <= 0) return null

    const { checkpoint } = await this.core.get(len - 1)
    if (!checkpoint || i >= checkpoint.length || !checkpoint[i].checkpoint) return null

    const { signature, length } = checkpoint[i].checkpoint
    return { signature, length, at: len }
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
    this.hostcalls = null
    this.changes = base._hasUpdate ? new UpdateChanges(base) : null

    this.updates = []

    this.local = null
    this.tx = null // for local use

    this.fastForwarding = false
    this.indexersUpdated = false
    this.indexedLength = 0
    this.quorum = 0
    this.stretchedThreshold = 0
    this.needsIndexedLengthUpdate = false
    this.interrupted = false
    this.applying = false

    this.localCheckpoint = null
    this.localIndexer = false

    this.systemUpgrade = null

    this.checkpoints = []
    this.pendingViews = null
    this.dirty = false
  }

  shouldFlush () {
    return this.dirty
  }

  shouldWrite () {
    if (!this.localIndexer || !this.localCheckpoint || !this.localCheckpoint.opened) return false
    if (!this.localCheckpoint.signedLength() < this.indexedLength + this.stretchedThreshold) return false

    // system passed the threshold, see if a view has changed
    const len = Math.min(this.localCheckpoint.signatures.length, this.views.length + 1)

    for (let i = 1; i < len; i++) {
      const sig = this.localCheckpoint.signatures[i]
      const core = this.views[i - 1].core
      if (core && sig.length > core.length) return true
    }

    return false
  }

  async shouldMigrate () {
    if (!this.fastForwarding && !this.indexersUpdated) return false
    if (this.system.indexers.length === 0) return false // genesis

    // sanity, prefer migration
    if (!this.system.core.manifest || !this.system.core.manifest.signers) return true

    // easy mode
    if (this.system.indexers.length !== this.system.core.manifest.signers.length) {
      return true
    }

    const manifests = await this.store.getIndexerManifests(this.system.indexers)

    for (let i = 0; i < this.system.core.manifest.signers.length; i++) {
      const { publicKey } = this.system.core.manifest.signers[i]
      if (!b4a.equals(publicKey, manifests[i].signers[0].publicKey)) return true
    }

    return false
  }

  isLocalPendingIndexer () {
    if (this.system.pendingIndexers.length === 0) return false
    const key = this.base.local.key
    for (const k of this.system.pendingIndexers) {
      if (b4a.equals(k, key)) return !b4a.equals(this.base.key, key) && this.base.local.length === 0
    }
    return false
  }

  removeable (key) {
    if (this.system.indexers.length !== 1) return true
    return !b4a.equals(this.system.indexers[0].key, key)
  }

  isLocalIndexer () {
    return !!this.localCheckpoint
  }

  _createCheckpointCore (key, active, signer, paused) {
    const encryption = this.base.getWriterEncryption(key)
    const session = this.base.store.get({
      key,
      valueEncoding: messages.OplogMessage,
      encryption,
      active
    })

    return new CheckpointCore(this, session, signer, paused)
  }

  async _open () {
    const boot = await this.base._getBootRecord()

    const sysCore = this.store.get({ name: '_system' })
    await sysCore.ready()
    await sysCore.setUserData('referrer', this.base.key)
    await sysCore.setUserData('autobase/view', b4a.from('_system'))

    this.systemRef = this.store.getViewByName('_system')

    const system = new SystemView(sysCore)
    await system.ready()

    // reset so we dont track the _system
    this.store.opened = []

    this.hostcalls = new PrivateApplyCalls(this)
    const view = this.base._hasOpen ? this.base._handlers.open(this.store, this.hostcalls) : null

    // ensure all are ready
    for (const v of this.store.opened) await v.atomicBatch.ready()

    this.system = system

    this.view = view
    this.fastForwarding = boot.fastForwarding
    this.indexedLength = boot.indexedLength
    this.indexersUpdated = boot.indexersUpdated
    this.stretchedThreshold = this.system.indexers.length * SHOULD_WRITE_THRESHOLD
    this.quorum = sysCore.manifest ? sysCore.manifest.quorum : 0

    const added = new Set()

    for (let i = 0; i < system.views.length; i++) {
      const { key, length } = system.views[i]
      const v = await this.store.findViewByKey(key, system.indexers)

      if (v === null) {
        this.views.push({ name: null, key, length, core: null, ref: null, mappedIndex: i })
        continue
      }

      await v.atomicBatch.ready()
      await v.core.setUserData('referrer', this.base.key)
      await v.core.setUserData('autobase/view', b4a.from(v.name))

      this.views.push({ name: v.name, key, length, core: v.atomicBatch, ref: v, mappedIndex: i })
      added.add(v)
    }

    for (const v of this.store.opened) {
      if (added.has(v)) continue
      this.views.push({ name: v.name, key: v.atomicBatch.key, length: v.atomicBatch.length, core: v.atomicBatch, ref: v, mappedIndex: -1 })
    }

    this.local = this.store.getLocal()
    await this.local.ready()

    let isLocalIndexer = false

    const paused = this.base.isFastForwarding()

    for (let i = 0; i < system.indexers.length; i++) {
      const idx = system.indexers[i]
      const chk = this._createCheckpointCore(idx.key, true, i, paused)
      await chk.ready()
      this.checkpoints.push(chk)
      if (b4a.equals(idx.key, this.base.local.key)) isLocalIndexer = true
    }

    const shouldSign = this.quorum > 0 && isLocalIndexer

    if (shouldSign) {
      this.localIndexer = true
      await this._startLocalCheckpoint()
    }

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

    this.localCheckpoint = this._createCheckpointCore(this.base.local.key, false, 0, false)
    await this.localCheckpoint.ready()
  }

  interrupt () {
    this.interrupted = true
    this.pause()
  }

  pause () {
    for (const chk of this.checkpoints) {
      if (chk !== this.localCheckpoint) chk.pause()
    }
  }

  resume () {
    for (const chk of this.checkpoints) {
      if (chk !== this.localCheckpoint) chk.resume()
    }
  }

  _checkSystemUpgrade () {
    if (this.systemUpgrade) return

    const tally = new Map()

    for (const chk of this.checkpoints) {
      if (chk.signedLength() <= this.system.core.signedLength) continue
      if (!chk.digest) continue

      const id = b4a.toString(chk.digest.key, 'hex')
      const cnt = (tally.get(id) || 0) + 1

      if (cnt >= this.quorum) {
        this.systemUpgrade = chk.digest.key
        this.base._queueStaticFastForward(this.systemUpgrade)
        return
      }

      tally.set(id, cnt)
    }
  }

  mapIndexToView (index) {
    for (const { mappedIndex, ref } of this.views) {
      if (mappedIndex === index) return ref
    }
  }

  async maybeSigned () {
    if (this.opened === false) await this.ready()
    if (this.interrupted) return

    const thres = this.checkpoints.length - this.quorum
    if (thres < 0 || this.checkpoints.length <= thres) return

    this.checkpoints.sort(cmpCheckpoints)

    const signableLength = this.checkpoints[thres].signedLength()
    if (signableLength <= this.system.core.signedLength) return

    const expected = this.system.core.key

    if (signableLength > this.indexedLength) {
      if (!b4a.equals(expected, this.checkpoints[thres].digest.key)) this._checkSystemUpgrade()
      this.needsIndexedLengthUpdate = true
      return
    }

    for (let i = thres; i < this.checkpoints.length; i++) {
      const chk = this.checkpoints[i]
      // we must agree on what the system is obvs
      if (!b4a.equals(expected, chk.digest.key)) {
        this._checkSystemUpgrade()
        return
      }
      // if we dont have the indexed state ourself, then no way for us to verify / patch
      if (chk.signatures.length === 0 || chk.signatures[0].length > this.indexedLength) {
        this.needsIndexedLengthUpdate = true
        return
      }
    }

    this.needsIndexedLengthUpdate = false

    const chk = []
    for (let i = thres; i < this.checkpoints.length; i++) {
      chk.push({ signer: this.checkpoints[i].signer, signatures: this.checkpoints[i].signatures })
    }

    await this._assembleMultisig(signableLength, chk)
  }

  async _assembleMultisig (signableLength, checkpoints) {
    const views = new Array(this.views.length + 1) // +1 is sys
    const sys = await this.system.getIndexedInfo(signableLength)

    if (this.interrupted) return

    if (this.pendingViews && this.pendingViews[0].length >= signableLength) {
      return
    }

    for (let i = 0; i < views.length; i++) {
      const view = i === 0 ? null : this.views[i - 1]
      const core = i === 0 ? this.system.core : view.core
      const ref = i === 0 ? this.systemRef : view.ref

      const v = i === 0 ? null : this.getViewFromSystem(view, sys)
      const length = i === 0 ? signableLength : v ? v.length : 0

      if (!core || length <= core.signedLength) continue

      const viewIndex = i === 0 ? 0 : view.mappedIndex + 1

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

      views[viewIndex] = sign
    }

    const promises = []
    for (const view of views) {
      if (!view) continue

      for (let i = 0; i < view.signatures.length; i++) {
        promises.push(setPartialSignature(view, i))
      }
    }

    await Promise.all(promises)

    // check that the state still looks good, couple replicate inbetween...
    for (const v of views) {
      if (!v) continue

      for (let i = 0; i < v.signatures.length; i++) {
        if (!v.partials[i]) return
      }
    }

    if (this.interrupted) return

    if (this.pendingViews && this.pendingViews[0].length >= signableLength) {
      return
    }
    // skipped system for whatever reason...
    if (!views[0]) return

    this.pendingViews = views
    this.dirty = true

    // TODO: only needed if not updating, wont crash so keeping for now, just less efficient
    this.base._queueBump()
  }

  async _close () {
    this.interrupted = true
    for (const chk of this.checkpoints) await chk.close()
    if (this.localCheckpoint) await this.localCheckpoint.close()
    if (this.base._hasClose) await this.base._handlers.close(this.view)
    if (this.system) await this.system.close()

    const promises = []
    for (const v of this.views) {
      if (v.ref) promises.push(v.ref.release())
    }
    promises.push(this.systemRef.release())

    await Promise.all(promises)

    await this.store.close()
  }

  _deleteUpdate (u) {
    this.tx.deleteLocal(c.encode(messages.LinearizerKey, u.seq))
  }

  _pushUpdate (u) {
    u.seq = this.updates.length === 0 ? 0 : this.updates[this.updates.length - 1].seq + 1
    u.systemLength = this.system.core.length
    this.updates.push(u)
    this._insertUpdate(u)
  }

  _insertUpdate (u) {
    this.tx.putLocal(c.encode(messages.LinearizerKey, u.seq), c.encode(messages.LinearizerUpdate, u))
  }

  async catchup (linearizer) {
    if (!this.opened) await this.ready()
    if (!this.system.heads.length) return

    const writers = new Map()

    // load linearizer...
    const updates = []
    const gte = b4a.from([messages.LINEARIZER_PREFIX])
    const lt = b4a.from([messages.LINEARIZER_PREFIX + 1])
    for await (const data of this.local.state.storage.createLocalStream({ gte, lt })) {
      const seq = c.decode(messages.LinearizerKey, data.key)
      const upd = c.decode(messages.LinearizerUpdate, data.value)
      upd.seq = seq
      updates.push(upd)
    }

    const sys = await this.system.checkout(this.indexedLength)

    for (const node of updates) {
      const hex = b4a.toString(node.key, 'hex')

      let w = writers.get(hex)

      if (w === undefined) { // TODO: we actually have all the writer info already but our current methods make it hard to reuse that
        w = await this.base._getWriterByKey(node.key, -1, 0, true, false, sys)
        writers.set(hex, w)
      }

      assert(w.length < node.length, 'Update expects writer to be consumed here')

      while (w.length < node.length) {
        await w.update(sys)

        const next = w.advance()
        assert(next, 'Node must exist for catchup')

        linearizer.addHead(next)
      }
    }

    await sys.close()

    this.updates = updates

    if (this.indexersUpdated) {
      this.indexersUpdated = false
      // if we updated the indexers, invalidate all the internal state and reapply it
      await this.truncate(this.indexedLength)
      await this._rollbackViews()

      if (this.tx === null) this.tx = this.local.state.storage.write()

      while (this.updates.length > 0 && this.updates[this.updates.length - 1].systemLength >= this.indexedLength) {
        const u = this.updates.pop()
        this._deleteUpdate(u)
      }
    } else {
      // otherwise we know the internal state is correct, so we carry on
      linearizer.update()
    }

    // must refresh the writers here so isRemoved is up to date
    await this._refreshWriters()
  }

  async getIndexedSystem () {
    if (this.opened === false) await this.ready()

    const sys = await this.system.checkout(this.indexedLength)
    await sys.ready()
    return sys
  }

  getViewFromSystem (view, sys = this.system) {
    if (view.mappedIndex === -1 || view.mappedIndex >= sys.views.length) return null
    return sys.views[view.mappedIndex]
  }

  async recoverAt () {
    await this.systemRef.core.ready()

    const lt = this.systemRef.core.signedLength

    for await (const { length, info } of SystemView.flushes(this.systemRef.core, { reverse: true, lt })) {
      for (let i = 0; i < info.views.length; i++) {
        if (i >= this.views.length) continue
        if (!b4a.equals(info.views[i].key, this.views[i].key)) continue
        if (info.views[i].length > this.views[i].core.signedLength) continue
        return { length, force: true, key: this.system.core.key, indexers: info.indexers, views: info.views }
      }
    }

    return null
  }

  bootstrap () {
    return this.system.add(this.base.key, { isIndexer: true, isPending: false })
  }

  async undo (popped) {
    if (!popped) return

    let indexersUpdated = false
    while (popped > 0) {
      const u = this.updates.pop()
      this._deleteUpdate(u)
      popped -= u.batch
      if (u.indexers) indexersUpdated = true
    }

    const u = this.updates.length === 0 ? null : this.updates[this.updates.length - 1]
    const systemLength = u ? u.systemLength : this.indexedLength

    await this.truncate(systemLength)
    if (indexersUpdated) await this._rollbackViews()
  }

  async truncate (systemLength) {
    if (this.opened === false) await this.ready()
    if (systemLength === this.system.core.length) return

    await this.system.core.truncate(systemLength)

    const migrated = await this.system.update()

    for (const view of this.views) {
      if (!view.core) continue

      const v = this.getViewFromSystem(view)

      if (v && view.core.length === v.length) continue
      if (v === null) view.mappedIndex = -1 // unmap

      await view.core.truncate(v ? v.length : 0)
    }

    if (!migrated) return

    await this._refreshWriters()
    await this._rollbackViews()
  }

  async _refreshWriters () {
    // TODO: add the seq at which we updated the state to the writer instance.
    // then we know if it was truncated out without the lookup, meaning faster truncations
    for (const w of this.base.activeWriters) {
      const data = await this.system.get(w.core.key)
      const bootstrapper = this.system.core.length === 0 && b4a.equals(w.core.key, this.base.key)
      const isRemoved = data ? data.isRemoved : !bootstrapper
      w.isRemoved = isRemoved
    }
  }

  async _updateSystem () {
    if (!(await this.system.update())) return
    await this._refreshWriters()
    await this._rollbackViews()
  }

  async _signViewCore (core, length) {
    const s = await core.signable(length, 0)
    const signature = crypto.sign(s, this.base.local.keyPair.secretKey)
    return { signature, length }
  }

  async _signAllViewCores () {
    const sys = await this.system.getIndexedInfo(this.indexedLength)
    const promises = new Array(this.views.length + 1)

    promises[0] = this._signViewCore(this.system.core, this.indexedLength)

    for (let i = 0; i < this.views.length; i++) {
      const view = this.views[i]

      const v = this.getViewFromSystem(view, sys)
      const indexedLength = v ? v.length : 0

      promises[i + 1] = this._signViewCore(view.ref.atomicBatch, indexedLength)
    }

    return Promise.all(promises)
  }

  async finalize (key) {
    const local = this.local
    const pointer = c.encode(messages.BootRecord, {
      key,
      indexedLength: this.indexedLength,
      indexersUpdated: true,
      fastForwarding: false,
      recoveries: this.base.recoveries
    })

    await local.setUserData('autobase/boot', pointer)

    if (this.tx) {
      await this.tx.flush()
      this.tx = null
    }

    await this.store.flush()
    await this.close()
  }

  async _flush (localNodes) {
    if (localNodes) await this._appendLocalNodes(localNodes)

    const local = this.local

    const pointer = c.encode(messages.BootRecord, {
      key: this.system.core.key,
      indexedLength: this.indexedLength,
      indexersUpdated: false,
      fastForwarding: false,
      recoveries: this.base.recoveries
    })

    await local.setUserData('autobase/boot', pointer)

    if (this.tx) {
      await this.tx.flush()
      this.tx = null
    }

    if (this.localCheckpoint) {
      // we have to flush here so the chkpoint can update
      // could run the checkpoint on the batch but also no big dead
      // as the "should we sign" check is rerun on boot...
      await this.store.flush()
      await this.localCheckpoint.update()
    }

    if (this.pendingViews) {
      const views = this.pendingViews
      this.pendingViews = null

      for (let i = views.length - 1; i >= 0; i--) {
        const v = views[i]
        if (!v || v.length <= v.core.signedLength) continue
        const signature = v.core.core.verifier.assemble(v.partials)
        try {
          await v.ref.commit(this.store.atom, v.length, signature)
        } catch (err) {
          // TODO: we should prevalidate all signatures instead of during commit so it can be all or nothing, just slighly
          // friendlier ux. missing hc api for that
          this.base._warn(err)
        }
      }
    }

    await this.store.flush()

    this.fastForwarding = false
    if (this.pendingViews === null) this.dirty = false
  }

  _indexUpdates (indexed) {
    let shift = 0
    while (indexed > 0) indexed -= this.updates[shift++].batch

    this.indexedLength = this.updates[shift - 1].systemLength

    for (let i = 0; i < shift; i++) this._deleteUpdate(this.updates[i])
    this.updates.splice(0, shift)

    if (!this.needsIndexedLengthUpdate) return
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

  async _optimisticApply (node, indexed) {
    const checkpoint = this.system.checkpoint()

    this.system.addHead(node)

    const applyBatch = []
    const key = node.writer.core.key

    applyBatch.push({
      indexed,
      optimistic: true,
      from: node.writer.core,
      length: node.length,
      value: node.value,
      heads: node.actualHeads
    })

    const pre = await this.system.get(key)
    const preLength = pre ? pre.length : 0

    let failed = false

    this.applying = true
    try {
      await this.base._handlers.apply(applyBatch, this.view, this.hostcalls)
    } catch {
      failed = true
    }
    this.applying = false

    if (!failed) {
      const post = await this.system.get(key)
      // check if acked by addWriter/removeWriter/ackWriter
      if (!post || preLength === post.length) failed = true
    }

    if (!failed) {
      // technically we only need to to this is the writer was removed, but hey, tricky logic
      await this._refreshWriters()
      return true
    }

    // it failed! rollback...

    this.system.applyCheckpoint(checkpoint)

    for (const view of this.views) {
      const viewLength = view.core ? view.core.length : view.length
      const viewLookup = this.getViewFromSystem(view)
      const viewSystemLength = viewLookup ? viewLookup.length : 0
      if (viewSystemLength === viewLength) continue
      await view.core.truncate(viewSystemLength)
    }

    await this._refreshWriters()

    await this._rollbackViews()
    return false
  }

  async update (u, localNodes) {
    if (this.changes !== null) this.changes.track(this)
    if (this.tx === null) this.tx = this.local.state.storage.write()

    if (this.system.bootstrapping) await this.bootstrap()

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

      batch++

      const optimist = node.writer.isRemoved && node.optimistic && batch === 1 &&
        this.base._hasOptimisticApply === true && await this._optimisticApply(node, indexed)

      if (!optimist) {
        if (node.writer.isRemoved && !node.writer.isActiveIndexer) {
          if (node.batch > 1) continue
          // in case someone is linking this node and they are not removed
          const u = { seq: 0, key: node.writer.core.key, length: node.length, batch, systemLength: 0, indexers: false }
          this._pushUpdate(u)
          batch = 0
          assert(applyBatch.length === 0, 'Apply batch should not have been modified')
          continue
        }

        // in prod we prop need to disable this assertion as its pretty expensive,
        // but it catches a lot of bugs, so here for debugs
        // await this._assertNode(node, batch)

        const deps = node.causalDependencies()

        // oldest -> newest
        for (let i = deps.length - 1; i >= 1; i--) {
          const d = deps[i]
          if (await this.system.linkable(d.writer.core.key, d.length, true)) {
            this.system.addHead(deps[i])
          }
        }

        this.system.addHead(deps[0])

        if (node.value !== null && !node.writer.isRemoved) {
          applyBatch.push({
            indexed,
            optimistic: false,
            from: node.writer.core,
            length: node.length,
            value: node.value,
            heads: node.actualHeads
          })
        }

        if (node.batch > 1) continue

        if (applyBatch.length && this.base._hasApply === true) {
          this.applying = true
          await this.base._handlers.apply(applyBatch, this.view, this.hostcalls)
          this.applying = false
        }
      }

      const update = {
        seq: 0,
        key: node.writer.core.key,
        length: node.length,
        batch,
        systemLength: 0,
        indexers: false
      }

      update.indexers = !!this.system.indexerUpdate

      if (this.system.indexerUpdate) await this._generateNextViews()

      await this.system.flush(this.views)
      await this.system.update()

      batch = 0
      applyBatch = []

      this._pushUpdate(update)

      if (!indexed || indexersUpdated) continue

      this.base._shiftWriter(node.writer)

      if (update.indexers) {
        indexersUpdated = i + 1
      }
    }

    if (u.indexed.length) {
      this._indexUpdates(indexersUpdated || u.indexed.length)
    }

    if (this.changes !== null) {
      this.changes.finalise()
      await this.base._handlers.update(this.view, this.changes)
    }

    await this._flush(localNodes)

    return indexersUpdated !== 0
  }

  flush () {
    return this._flush(null)
  }

  async _appendLocalNodes (localNodes) {
    if (localNodes.length === 0) return // just in case

    const blocks = new Array(localNodes.length)
    const local = this.local

    if (!local.opened) await local.ready()

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch, optimistic } = localNodes[i]

      blocks[i] = {
        version: 1,
        maxSupportedVersion: this.base.maxSupportedVersion,
        checkpoint: null,
        digest: null,
        optimistic,
        node: {
          heads,
          batch,
          value: value === null ? null : c.encode(this.base.valueEncoding, value)
        },
        trace: []
      }
    }

    if (this.localIndexer) {
      const signedLength = this.localCheckpoint.signedLength()
      const local = this.local

      let signedAt = 0
      let signatures = null

      if (this.indexedLength > signedLength) {
        signatures = await this._signAllViewCores()
        signedAt = local.length + blocks.length
      }

      for (let i = 0; i < blocks.length; i++) {
        const length = local.length + i + 1
        if (length === signedAt) this.localCheckpoint.updateSignatures(length, signatures)

        const blk = blocks[i]

        blk.checkpoint = this.localCheckpoint.makeCheckpoints(length)
        blk.digest = this.localCheckpoint.makeDigest(length, this.system.core.key)
      }
    } else {
      if (!this.localCheckpoint) await this._startLocalCheckpoint()

      for (let i = 0; i < blocks.length; i++) {
        const length = local.length + i + 1
        blocks[i].digest = this.localCheckpoint.makeDigest(length, this.system.core.key)
      }
    }

    await local.append(blocks)
  }

  async _rollbackViews () {
    for (const view of this.views) {
      const v = this.getViewFromSystem(view)

      if (v) {
        view.key = v.key
      } else {
        await this._resetView(view)
      }
    }
  }

  async _resetView (view) {
    const manifests = await this.store.getIndexerManifests(this.system.indexers)
    view.key = await this.store.createView(manifests, view.name, null)
    view.length = 0
  }

  async _generateNextViews () {
    // note, this is very state dependent so can ONLY be called at the exact time an indexer upgrade occurs
    const manifests = await this.store.getIndexerManifests(this.system.indexers)

    for (const v of this.views) {
      const prologue = await getPrologue(v)
      const key = await this.store.createView(manifests, v.name, prologue)

      v.key = key
    }
  }
}

async function getPrologue (view) {
  const length = view.core ? view.core.length : view.length
  if (!length) return null

  const hash = await view.core.treeHash(length)

  return {
    hash,
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
