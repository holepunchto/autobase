import Autobase from '../../index.js'
import Corestore from 'corestore'
import Hyperblobs from 'hyperblobs'
import Hyperbee from 'hyperbee'
import ReadyResource from 'ready-resource'
import safetyCatch from 'safety-catch'
import c from 'compact-encoding'
import { replicate, sync } from 'autobase-test-helpers'

class BlobBase extends ReadyResource {
  constructor(store, opts = {}) {
    super()
    this.store = store
    this.bootstrap = opts.bootstrap || null
    this.blobs = new Map()

    this.ready().catch(safetyCatch)
  }

  async _open() {
    this.base = new Autobase(this.store, this.bootstrap, {
      valueEncoding: c.any,
      open: this.open.bind(this),
      apply: this.apply.bind(this)
    })
    await this.base.ready()

    const blobsCore = this.store.get({ name: 'blobs' })
    this.localBlobs = new Hyperblobs(blobsCore)
    // TODO maybe remove
    await this.localBlobs.ready()
  }

  open(store) {
    const core = store.get('view')
    return new Hyperbee(core, {
      extension: false,
      valueEncoding: c.any
    })
  }

  async apply(nodes, view, base) {
    for (const { value } of nodes) {
      const { op } = value
      switch (op) {
        case 'add':
          await base.addWriter(value.writer)
          continue
        case 'put':
          await view.put(value.key, value.value)
          break
      }
    }
  }

  async addWriter(writerKey) {
    this.base.append({
      op: 'add',
      writer: writerKey
    })
  }

  async put(filename, buffer) {
    const id = await this.localBlobs.put(buffer)

    await this.base.append({
      op: 'put',
      key: filename,
      value: {
        blobKey: this.localBlobs.key,
        id
      }
    })
  }

  async get(filename) {
    const idNode = await this.base.view.get(filename)
    const remoteBlobsKey = idNode.value.blobKey
    const remoteBlobString = remoteBlobsKey.toString('hex')

    let remoteBlobs
    if (this.blobs.has(remoteBlobString)) {
      remoteBlobs = this.blobs.get(remoteBlobString)
    } else {
      const blobCore = this.store.get(remoteBlobsKey)
      remoteBlobs = new Hyperblobs(blobCore)
      this.blobs.set(remoteBlobString, remoteBlobs)
    }

    return await remoteBlobs.get(idNode.value.id)
  }
}

const a = new BlobBase(makeStore('A'))
await a.ready()

const largeBuffer = Buffer.alloc(100 * 1024 ** 2, 'largebuffer') // 100 MB
await a.put('foo.txt', largeBuffer)
const contentA = await a.get('foo.txt')
console.log('content from A', contentA.length)

const b = new BlobBase(makeStore('B'), { bootstrap: a.base.key })
await b.ready()

const bases = [a.base, b.base]

// Replicate between autobases
const done = replicate(bases)
await sync(bases)

const contentB = await b.get('foo.txt')
console.log('content from B', contentB.length)

// Safe to close replication
await done()

function makeStore(seed) {
  return new Corestore('./peer-' + seed, { primaryKey: Buffer.alloc(32).fill(seed) })
}
