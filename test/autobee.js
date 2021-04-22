const test = require('tape')
const ram = require('random-access-memory')
const Corestore = require('corestore')

const { Manifest, User } = require('../lib/manifest')
const Autobee = require('../examples/autobee')
const Autobase = require('..')

test('simple autobee', async t => {
  const store1 = await Corestore.fromStorage(ram)
  const store2 = await Corestore.fromStorage(ram)
  // Replicate both corestores
  replicate(store1, store2)

  const { user: firstUser } = await Autobase.createUser(store1)
  const { user: secondUser } = await Autobase.createUser(store2)
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
