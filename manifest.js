const Autobase = require('./index')
const { Manifest, User } = require('./lib/manifest')

const INPUT_NAME = '@autobase/input'
const INDEX_NAME = '@autobase/index'

const TOKEN = Buffer.from('46d08fc8fa0344bd5d6a09d5e119584e31c553812ea506e5e56bd0d41e9eb0e2', 'hex')

function fromManifest (store, manifest, opts = {}) {
  manifest = Manifest.inflate(store, manifest, TOKEN)

  const inputs = []
  const indexes = []
  for (const user of manifest) {
    if (user.input) inputs.push(user.input)
    if (user.index) indexes.push(user.index)
  }

  const localInput = getLocalInput(store)
  const localIndex = getLocalIndex(store)

  const inputsReady = readyAndReplaceLocal(inputs, localInput)
  const indexesReady = readyAndReplaceLocal(indexes, localIndex)

  return new Autobase(inputsReady.then(() => inputs), {
    ...opts,
    indexes: indexesReady.then(() => indexes)
  })
}

async function createUser (store, opts = {}) {
  const user = {
    input: opts.input !== false ? getLocalInput(store) : null,
    index: opts.index !== false ? getLocalIndex(store) : null
  }
  await Promise.allSettled([user.input.ready(), user.index.ready()])
  const id = await store.gossip.put(User.deflate(user))
  return { user, id }
}

function getLocalInput (store) {
  return store.get({ name: INPUT_NAME, token: TOKEN })
}

function getLocalIndex (store) {
  return store.get({ name: INDEX_NAME, token: TOKEN })
}

async function readyAndReplaceLocal (cores, localCore) {
  await Promise.all(cores.map(c => c.ready()))
  await localCore.ready()

  let replaced = false
  for (let i = 0; i < cores.length; i++) {
    if (!cores[i].key.equals(localCore.key)) continue
    cores[i].close()
    cores[i] = localCore
    replaced = true
  }

  if (!replaced) localCore.close()

  return cores
}

module.exports = {
  fromManifest,
  createUser
}
