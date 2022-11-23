const test = require('brittle')
const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('..')

test('basic - two writers', async t => {
  const [base1, base2, base3] = await create(3, apply)

  await base1.append({
    add: base2.local.key.toString('hex'),
    debug: 'this is adding b'
  })

  await sync(base1, base2, base3)

  await base2.append({
    add: base3.local.key.toString('hex'),
    debug: 'this is adding c'
  })

  await sync(base1, base2, base3)

  console.log('after last sync')

  console.log(base2._writersQuorum.size, base2._writers.length)
  console.log(base3._writersQuorum.size, base3._writers.length)

  console.log(await base1.latestCheckpoint())
  console.log(await base2.latestCheckpoint())
  console.log(await base3.latestCheckpoint())

  async function apply (batch, base) {
    for (const { value } of batch) {
      if (value.add) {
        await base.addWriter(Buffer.from(value.add, 'hex'))
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

async function create (n, apply) {
  const bases = [new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(0) }), null, { apply })]
  await bases[0].ready()
  if (n === 1) return bases
  for (let i = 1; i < n; i++) {
    const base = new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(i) }), bases[0].local.key, { apply })
    await base.ready()
    bases.push(base)
  }
  return bases
}

async function sync (...bases) {
  const streams = []

  for (const a of bases) {
    for (const b of bases) {
      if (a === b) continue
      const s1 = a.store.replicate(true)
      const s2 = b.store.replicate(false)

      s1.on('error', () => {})
      s2.on('error', () => {})

      s1.pipe(s2).pipe(s1)

      streams.push(s1)
      streams.push(s2)
    }
  }

  await Promise.all(bases.map(b => b.update()))

  for (const stream of streams) {
    stream.destroy()
  }
}
