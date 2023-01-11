const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('..')

module.exports = {
  create,
  sync,
  confirm
}

async function create (n, apply) {
  const opts = { apply, valueEncoding: 'json' }
  const bases = [new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(0) }), null, opts)]
  await bases[0].ready()
  if (n === 1) return bases
  for (let i = 1; i < n; i++) {
    const base = new Autobase(new Corestore(ram, { primaryKey: Buffer.alloc(32).fill(i) }), bases[0].local.key, opts)
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

async function confirm (...bases) {
  const writers = bases.filter(b => !!b.localWriter)
  const maj = Math.floor(writers.length / 2) + 1

  await sync(...bases)
  for (let i = 0; i < maj; i++) await writers[i].append(null)
  await sync(...bases)
  for (let i = 0; i < maj; i++) await writers[i].append(null)
  return sync(...bases)
}