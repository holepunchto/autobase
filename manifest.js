const Autobase = require('./index')
const { Manifest } = require('./lib/manifest')

const INPUT_NAME = '@autobase/input'
const INDEX_NAME = '@autobase/index'

const TOKEN = Buffer.from('46d08fc8fa0344bd5d6a09d5e119584e31c553812ea506e5e56bd0d41e9eb0e2', 'hex')

function fromManifest (store, manifest, opts = {}) {
  manifest = Manifest.inflate(store, manifest)
  return new Autobase(manifest.inputs, {
    ...opts,
    defaultIndexes: manifest.indexes
  })
}

function createUser (store, opts = {}) {
  const user = {
    input: opts.input !== false ? store.get({ name: INPUT_NAME, token: TOKEN }) : null,
    index: opts.index !== false ? store.get({ name: INDEX_NAME, token: TOKEN }) : null
  }
  await Promise.allSettled([user.input.ready(), user.index.ready()])
  // TODO: Store in immutable-store-extension by default
  // const id = await store.immutable.put(User.deflate(user))
  return { user, id: null }
}

module.exports = {
  fromManifest,
  createUser
}
