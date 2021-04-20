const test = require('tape')
const Hyperbee = require('hyperbee')
const Corestore = require('corestore')
const ram = require('random-access-memory')

const { Manifest, User } = require('../lib/manifest')
const Autobase = require('..')

test('simple autobase', async t => {
  const store1 = await Corestore.fromStorage(ram)
  const store2 = await Corestore.fromStorage(ram)
  replicate(store1, store2)

  const { user: firstUser } = await Autobase.createUser(store1)
  const { user: secondUser } = await Autobase.createUser(store2)
  const manifest = [firstUser, secondUser]

  const a1 = new Autobase(store1, manifest, firstUser)
  const a2 = new Autobase(store2, Manifest.deflate(manifest), User.deflate(secondUser))

  const output = await a2.createIndex(

  await a1.append(
})

function replicate (store1, store2) {
  const s1 = store1.replicate(true)
  const s2 = store2.replicate(false)
  s1.pipe
}
