const SystemView = require('./system')
const ReadyResource = require('ready-resource')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')

module.exports = class ViewState extends ReadyResource {
  constructor (base) {
    super()

    this.base = base
    this.valueEncoding = base.valueEncoding
    this.store = base._viewStore.atomize()
    this.system = null
    this.view = null
    this.views = []

    this.updates = []
    this.indexedLength = 0
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
    this.view = view
    this.indexedLength = info.indexedLength

    await system.ready()

    const views = system.views
    const added = new Set()

    for (let i = 0; i < system.views.length; i++) {
      const { key, length } = system.views[i]

      let found = false

      for (const v of this.store.opened) {
        await v.core.ready()

        if (v.core.key.equals(key)) {
          found = true
          added.add(v)
          this.views.push({ name, key, length, core: v.atomicBatch })
          break
        }
      }

      if (!found) {
        this.views.push({ name: null, key, length, core: null })
      }
    }

    for (const v of this.store.opened) {
      if (added.has(v)) continue
      this.views.push({ name: v.name, key: v.core.key, length: v.atomicBatch.length, core: v.atomicBatch })
    }
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

    const { views } = await this.system.getIndexedInfo(systemLength)

    for (const [name, core] of this.openByName) {
      let view = null

      for (const v of views) {
        if (v.key.equals(core.key)) {
          view = v
          break
        }
      }

      if (!view) {
        throw new Error('TODO: need to mine for the view')
      }

      await core.truncate(v.length)
    }

    // todo: update internal views

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

  async _signCoreAt (core, len) {
    const batch = await core.restoreBatch(len)
    const signature = crypto.sign(batch.signable(core.key), this.base.local.keyPair.secretKey)
    return signature
  }

  async _sign () {
    const info = await this.system.getIndexedInfo(this.indexedLength)
    const checkpoint = []

    checkpoint.push({
      checkpointer: 0,
      signature: await this._signCoreAt(this.system.core, this.indexedLength)
    })

    for (let i = 0; i < info.views.length; i++) {
      const { core } = this.views[i]

      if (core) {
        checkpoint.push({
          checkpointer: 0,
          signature: await this._signCoreAt(core, info.views[i].length)
        })
      } else {
        checkpoint.push({
          checkpointer: 0,
          signature: null
        })
      }
    }
  }

  async _flush (localNodes) {
    await this._appendLocalNodes(localNodes)

    console.log('should update boot record...', this.store.getLocal().length, this.indexedLength)
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

      await this._sign()
      await this._flush(localNodes)

      return true
    }

    if (u.undo) await this._undo(u.undo)

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

      await this._sign()
      await this._flush(localNodes)

      return true
    }

    if (u.indexed.length) {
      this._indexUpdates(u.indexed.length)
      await this._sign()
    }

    await this._flush(localNodes)

    return false
  }

  async _appendLocalNodes (localNodes) {
    const blocks = new Array(localNodes.length)

    for (let i = 0; i < blocks.length; i++) {
      const { value, heads, batch } = localNodes[i]
      const checkpoint = null //, this._addCheckpoints ? await generateCheckpoint(cores, indexed, info) : null

      blocks[i] = {
        version: 1,
        maxSupportedVersion: this.base.maxSupportedVersion,
        checkpoint,
        digest: null,
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
      const key = this.store.getViewKey(manifests, prologue, v.name)

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
