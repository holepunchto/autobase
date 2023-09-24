const ram = require('random-access-memory')
const Corestore = require('corestore')
const helpers = require('autobase-test-helpers')
const same = require('same-data')
const b4a = require('b4a')

const Autobase = require('../..')
const encryptionKey = process.env && process.env.ENCRYPT_ALL ? b4a.alloc(32).fill('autobase-encryption-test') : undefined

module.exports = {
  create,
  addWriter,
  addWriterAndSync,
  apply,
  confirm,
  compare,
  ...helpers
}

async function create (n, apply, open, close, opts = {}) {
  const moreOpts = { apply, open, close, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, ...opts }
  const bases = [new Autobase(new Corestore(ram.reusable(), { primaryKey: Buffer.alloc(32).fill(0), encryptionKey }), null, moreOpts)]
  await bases[0].ready()
  if (n === 1) return bases
  for (let i = 1; i < n; i++) {
    const base = new Autobase(new Corestore(ram.reusable(), { primaryKey: Buffer.alloc(32).fill(i), encryptionKey }), bases[0].local.key, moreOpts)
    await base.ready()
    bases.push(base)
  }
  return bases
}

async function addWriter (base, add) {
  return base.append({ add: add.local.key.toString('hex') })
}

async function addWriterAndSync (base, add, bases = [base, add]) {
  await addWriter(base, add)
  await helpers.replicateAndSync(bases)
  await base.ack()
  await helpers.replicateAndSync(bases)
}

async function confirm (bases, options = {}) {
  await helpers.replicateAndSync(bases)

  for (let i = 0; i < 2; i++) {
    const writers = bases.filter(b => !!b.localWriter)
    const maj = options.majority || (Math.floor(writers.length / 2) + 1)
    for (let j = 0; j < maj; j++) {
      await writers[j].append(null)
      await helpers.replicateAndSync(bases)
    }
  }

  await helpers.replicateAndSync(bases)
}

async function compare (a, b, full = false) {
  const alen = full ? a.view.length : a.view.indexedLength
  const blen = full ? b.view.length : b.view.indexedLength

  if (alen !== blen) throw new Error('Views are different lengths')

  for (let i = 0; i < alen; i++) {
    const left = await a.view.get(i)
    const right = await b.view.get(i)

    if (!same(left, right)) throw new Error('Views differ at block ' + i)
  }
}

async function apply (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'))
      continue
    }

    if (view) await view.append(value)
  }
}
