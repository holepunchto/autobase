const test = require('brittle')
const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('..')

const {
  create,
  sync,
  confirm,
  compare
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
})

test('basic - view', async t => {
  const [base] = await create(1, apply, store => store.get('test'))

  const block = { message: 'hello, world!' }
  await base.append(block)

  t.is(base.system.digest.writers.length, 1)
  t.is(base.view.indexedLength, 1)
  t.alike(await base.view.get(0), block)
})

test('basic - compare views', async t => {
  const bases = await create(2, apply, store => store.get('test'))

  const [a, b] = bases
  await a.append({ add: b.local.key.toString('hex') })

  await confirm(...bases)

  for (let i = 0; i < 6; i++) await bases[i % 2].append('msg' + i)

  await confirm(...bases)

  t.is(a.system.digest.writers.length, b.system.digest.writers.length)
  t.is(a.view.indexedLength, b.view.indexedLength)

  try {
    await compare(a, b)
  } catch (e) {
    t.fail(e.message)
  }
})

test('basic - online majority', async t => {
  const bases = await create(3, apply, store => store.get('test'))

  const [a, b, c] = bases

  await a.append({ add: b.local.key.toString('hex') })
  await a.append({ add: c.local.key.toString('hex') })

  await confirm(...bases)

  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })

  await confirm(...bases)

  const indexed = a.view.indexedLength

  for (let i = 0; i < 6; i++) await bases[i % 3].append('msg' + i)

  await confirm(a, b)

  t.not(a.view.indexedLength, indexed)
  t.is(c.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)

  try {
    await compare(a, b)
  } catch (e) {
    t.fail(e.message)
  }

  await sync(b, c)

  t.is(a.view.indexedLength, c.view.indexedLength)

  try {
    await compare(a, c)
  } catch (e) {
    t.fail(e.message)
  }
})

test('basic - rotating majority', async t => {
  const bases = await create(3, apply, store => store.get('test'))

  const [a, b, c] = bases

  await a.append({ add: b.local.key.toString('hex') })
  await a.append({ add: c.local.key.toString('hex') })

  await confirm(...bases)

  await a.append({ message: 'msg a' })
  await b.append({ message: 'msg b' })
  await c.append({ message: 'msg c' })

  await confirm(...bases)

  let indexed = a.view.indexedLength

  for (let i = 0; i < 6; i++) await bases[i % 3].append('msg' + i)

  await confirm(a, b)

  t.not(a.view.indexedLength, indexed)
  t.is(c.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)

  indexed = a.view.indexedLength

  for (let i = 0; i < 6; i++) await bases[i % 3].append('msg' + i)

  await confirm(b, c)

  t.not(b.view.indexedLength, indexed)
  t.is(a.view.indexedLength, indexed)
  t.is(b.view.indexedLength, c.view.indexedLength)

  indexed = b.view.indexedLength

  for (let i = 0; i < 6; i++) await bases[i % 3].append('msg' + i)

  await confirm(a, c)

  t.not(c.view.indexedLength, indexed)
  t.is(b.view.indexedLength, indexed)
  t.is(a.view.indexedLength, c.view.indexedLength)

  indexed = a.view.indexedLength

  for (let i = 0; i < 6; i++) await bases[i % 3].append('msg' + i)

  await confirm(...bases)

  t.not(a.view.indexedLength, indexed)
  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(a.view.indexedLength, c.view.indexedLength)

  try {
    await compare(a, b)
    await compare(a, c)
  } catch (e) {
    t.fail(e.message)
  }
})

test('basic - throws', async t => {
  const bases = await create(2, apply, store => store.get('test'))

  const [a, b] = bases

  await a.append({ message: 'msg1' })
  await a.append({ message: 'msg2' })
  await a.append({ message: 'msg3' })

  await confirm(a, b)

  await t.exception(b.append({ message: 'not writable' }))
  await t.exception(a.view.append({ message: 'append outside apply' }))
  t.exception(() => a.system.addWriter(b.local.key))
})

test('basic - online minorities', async t => {
  const bases = await create(5, apply, store => store.get('test'))

  const [a, b, c, d, e] = bases

  await a.append({ add: b.local.key.toString('hex') })
  await a.append({ add: c.local.key.toString('hex') })
  await a.append({ add: d.local.key.toString('hex') })
  await a.append({ add: e.local.key.toString('hex') })

  await confirm(...bases)

  t.is(a.view.indexedLength, c.view.indexedLength)

  for (let i = 0; i < 10; i++) await bases[i % 5].append('msg' + i)
  for (let i = 0; i < 5; i++) await bases[i % 2].append('msg' + i)
  for (let i = 0; i < 8; i++) await bases[i % 2 + 2].append('msg' + i)

  await confirm(a, b)
  await confirm(c, d)

  t.is(a.view.indexedLength, b.view.indexedLength)
  t.is(a.view.indexedLength, c.view.indexedLength)
  t.is(a.view.indexedLength, d.view.indexedLength)
  t.is(a.view.indexedLength, e.view.indexedLength)

  t.not(a.view.length, a.view.indexedLength)
  t.is(a.view.length, b.view.length)
  t.not(c.view.length, a.view.length)
  t.is(c.view.length, d.view.length)

  try {
    await compare(a, b, true)
    await compare(c, d, true)
  } catch (e) {
    t.fail(e.message)
  }

  await confirm(...bases)

  t.is(a.view.length, c.view.length)
  t.is(a.view.indexedLength, c.view.indexedLength)

  try {
    await compare(a, b, true)
    await compare(a, c, true)
    await compare(a, d, true)
    await compare(a, e, true)
  } catch (e) {
    t.fail(e.message)
  }
})

async function apply (batch, view, base) {
  for (const { value } of batch) {
    if (value === null) continue
    if (value.add) {
      await base.system.addWriter(Buffer.from(value.add, 'hex'))
    }

    if (view) await view.append(value)
  }
}