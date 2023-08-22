const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('../..')

module.exports = {
  create,
  eventFlush,
  downloadAll,
  sync,
  synced,
  addWriter,
  apply,
  confirm,
  replicate,
  compare
}

async function create (n, apply, open, close, opts = {}) {
  const moreOpts = { apply, open, close, valueEncoding: 'json', ackInterval: 0, ackThreshold: 0, ...opts }
  const bases = [new Autobase(new Corestore(ram.reusable(), { primaryKey: Buffer.alloc(32).fill(0) }), null, moreOpts)]
  await bases[0].ready()
  if (n === 1) return bases
  for (let i = 1; i < n; i++) {
    const base = new Autobase(new Corestore(ram.reusable(), { primaryKey: Buffer.alloc(32).fill(i) }), bases[0].local.key, moreOpts)
    await base.ready()
    bases.push(base)
  }
  return bases
}

function replicate (...b) {
  const [bases] = parse(b)
  const streams = []
  const missing = bases.slice()

  while (missing.length) {
    const a = missing.pop()

    for (const b of missing) {
      const s1 = a.store.replicate(true)
      const s2 = b.store.replicate(false)

      s1.on('error', () => {})
      s2.on('error', () => {})

      s1.pipe(s2).pipe(s1)

      streams.push(s1)
      streams.push(s2)
    }
  }

  return close

  function close () {
    return Promise.all(streams.map(s => {
      s.destroy()
      return new Promise(resolve => s.on('close', resolve))
    }))
  }
}

function eventFlush () {
  return new Promise(resolve => setImmediate(resolve))
}

function isAutobase (x) {
  return x.constructor.name === 'Autobase'
}

function isOptions (x) {
  return !!x && x.constructor.name === 'Object'
}

function parse (b) {
  if (b.length === 1 && Array.isArray(b[0])) return [b[0], {}]

  const bases = b.flat().filter(isAutobase)
  const options = b.length > 0 && isOptions(b[b.length - 1]) ? b.pop() : {}

  return [bases, options]
}

async function sync (...b) {
  const [bases] = parse(b)

  if (bases.length === 1) {
    await downloadAll(bases)
    return
  }

  const close = replicate(bases)

  await downloadAll(bases)

  return close()
}

async function downloadAll (bases, flush = eventFlush) {
  do {
    await flush()
    await Promise.all(bases.map(downloadAllWriters))
    await flush()
  } while (!synced(...bases))
}

function synced (...b) {
  const [bases] = parse(b)

  for (let i = 0; i < bases.length; i++) {
    for (const w of bases[i].writers) {
      if (w.core.length !== w.core.contiguousLength) return false
    }
  }
  return true
}

async function addWriter (base, add) {
  return base.append({ add: add.local.key.toString('hex') })
}

async function confirm (...b) {
  const [bases, options] = parse(b)
  await sync(bases)

  for (let i = 0; i < 2; i++) {
    const writers = bases.filter(b => !!b.localWriter)
    const maj = options.majority || (Math.floor(writers.length / 2) + 1)

    for (let j = 0; j < maj; j++) {
      await writers[j].append(null)
      await sync(bases)
    }
  }

  await sync(bases)
}

async function compare (a, b, full = false) {
  const alen = full ? a.view.length : a.view.indexedLength
  const blen = full ? b.view.length : b.view.indexedLength

  if (alen !== blen) throw new Error('Views are different lengths')

  for (let i = 0; i < alen; i++) {
    const left = await a.view.get(i)
    const right = await b.view.get(i)

    if (!equal(left, right)) throw new Error('Views differ at block ' + i)
  }
}

function equal (a, b) {
  if (typeof a !== typeof b) return false
  if (a === null) return b === null
  if (typeof a === 'object') {
    const entries = Object.entries(a)

    if (entries.length !== Object.entries(b).length) return false

    for (const [k, v] of entries) {
      if (!equal(b[k], v)) return false
    }

    return true
  }

  return a === b
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

async function downloadAllWriters (base) {
  await base.ready()
  let writers = 0

  do {
    writers = base.writers.length
    for (const w of base.writers) {
      await eventFlush()
      await w.core.update({ wait: true })
      await coreDownloadAll(w.core)
    }
    await base.update()
  } while (writers !== base.writers.length)
}

function coreDownloadAll (core) {
  const start = core.contiguousLength
  const end = core.length
  return core.download({ start, end }).done()
}
