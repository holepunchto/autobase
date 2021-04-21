const test = require('tape')
const Hyperbee = require('hyperbee')
const Corestore = require('corestore')
const ram = require('random-access-memory')

const { Manifest, User } = require('../lib/manifest')
const Autobase = require('..')

class Autobee {
  constructor (corestore, manifest, local, opts) {
    this.autobase = new Autobase(corestore, manifest, local)
    this.opts = opts

    this.output = null // Set in _open
    this.bee = null // Set in _open

    this._opening = this._open()
    this._opening.catch(noop)
    this.ready = () => this._opening
  }

  async _open () {
    await this.autobase.ready()
    this.output = this.autobase.createIndex({
      apply: this._apply.bind(this)
    })
    this.bee = new Hyperbee(this.output, {
      ...this.opts,
      extension: false,
      readonly: true
    })
  }

  _encodeOp (op) {
    return Buffer.from(JSON.stringify(op))
  }

  _decodeOp (raw) {
    return JSON.parse(raw.toString())
  }

  _apply (node) {
    console.log('APPLYING NODES:', nodes)
  }

  async put (key, value) {
    if (this._opening) await this._opening
    return this.autobase.append(this._encodeOp({ type: 'put', key, value }))
  }

  async get (key) {
    if (this._opening) await this._opening
    return this.bee.get(key)
  }
}

test('simple autobee', async t => {
  const store1 = await Corestore.fromStorage(ram)
  const store2 = await Corestore.fromStorage(ram)
  // Replicate both corestores
  replicate(store1, store2)

  const { user: firstUser } = await Autobase.createUser(store1)
  const { user: secondUser } = await Autobase.createUser(store2)
  console.log('first user:', firstUser)
  console.log('second user:', secondUser)
  const manifest = [firstUser, secondUser]

  const bee1 = new Autobee(store1, manifest, firstUser, {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  const bee2 = new Autobee(store2, Manifest.deflate(manifest), User.deflate(secondUser), {
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })
  await bee1.ready()
  await bee2.ready()

  await bee1.put('a', 'b')
  await bee2.put('c', 'd')

  {
    const node = await bee2.get('a')
    t.true(node)
    t.same(node.value, 'b')
  }

  {
    const node = await bee1.get('c')
    t.true(node)
    t.same(node.value, 'd')
  }

  t.end()
})

function replicate (store1, store2) {
  const s1 = store1.replicate(true)
  const s2 = store2.replicate(false)
  s1.pipe(s2).pipe(s1)
}

function noop () {}
