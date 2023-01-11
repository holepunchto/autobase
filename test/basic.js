const test = require('brittle')
const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('..')

const {
  create,
  sync,
  confirm
} = require('./helpers')

test('basic - two writers', async t => {
  const [base1, base2, base3] = await create(3, apply)

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await confirm(base1, base2, base3)

  await base2.append({
    add: base3.local.key.toString('hex'),
    debug: 'this is adding c'
  })

  await confirm(base1, base2, base3)

  t.is(base2.system.digest.writers.length, 3)
  t.is(base2.system.digest.writers.length, base3.system.digest.writers.length)
  t.is(base2.system.digest.writers.length, base2.writers.length)
  t.is(base3.system.digest.writers.length, base3.writers.length)

  t.alike(await base1.system.checkpoint(), await base2.system.checkpoint())
  t.alike(await base1.system.checkpoint(), await base3.system.checkpoint())

  async function apply (batch, view, base) {
    for (const { value } of batch) {
      if (value === null) continue
      if (value.add) {
        await base.system.addWriter(Buffer.from(value.add, 'hex'))
      }
    }
  }
})

async function list (name, base) {
  console.log('**** list ' + name + ' ****')
  for (let i = 0; i < base.length; i++) {
    console.log(i, (await base.get(i)).value)
  }
  console.log('')
}
