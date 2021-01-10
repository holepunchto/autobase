const p = require('path')
const test = require('tape')
const Corestore = require('corestore')
const Omega = require('omega')
const Sandbox = require('module-sandbox')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const { toPromises } = require('hypercore-promisifier')

const { indexedValues } = require('./helpers')
const { IndexNode } = require('../lib/nodes')
const Autobase = require('..')

const UPPERCASE_MACHINE_PATH = p.join(__dirname, 'machines', 'uppercase.js')
const DOUBLER_MACHINE_PATH = p.join(__dirname, 'machines', 'doubler.js')

const INDEXER_MACHINE_DIR = p.join(__dirname, 'machines', 'indexer')
const { Op } = require(p.join(INDEXER_MACHINE_DIR, 'messages.js'))

test('rebase with mapping machine', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

  const machine = new Sandbox(UPPERCASE_MACHINE_PATH)
  await machine.ready()

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  {
    await base.rebase(output, {
      map: async (node) => {
        const rsp = await machine.rpc.uppercase(node)
        return Buffer.from(rsp, 'utf-8')
      }
    })
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), ['A0', 'B1', 'B0', 'C2', 'C1', 'C0'])
  }

  t.end()
})

test('rebase with multi-value batches', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const writerC = toPromises(store.get({ name: 'writer-c' }))

  const machine = new Sandbox(DOUBLER_MACHINE_PATH)
  await machine.ready()

  const base = new Autobase([writerA, writerB, writerC])

  // Create three independent forks
  for (let i = 0; i < 1; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }
  for (let i = 0; i < 2; i++) {
    await base.append(writerB, `b${i}`, await base.latest(writerB))
  }
  for (let i = 0; i < 3; i++) {
    await base.append(writerC, `c${i}`, await base.latest(writerC))
  }

  const mapper = async (node) => {
    const rsp = await machine.rpc.double(node)
    return rsp.map(r => Buffer.from(r, 'utf-8'))
  }

  {
    await base.rebase(output, { map: mapper })
    const expected = []
    for (const val of ['a0', 'b1', 'b0', 'c2', 'c1', 'c0']) {
      expected.push(val + ':second', val + ':first')
    }
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), expected)
  }

  // Add 3 more records to A -- should switch fork ordering
  for (let i = 1; i < 4; i++) {
    await base.append(writerA, `a${i}`, await base.latest(writerA))
  }

  {
    await base.rebase(output, { map: mapper })
    const expected = []
    for (const val of ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']) {
      expected.push(val + ':second', val + ':first')
    }
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), expected)
  }

  t.end()
})

test('hyperbee indexer example', async t => {
  const store = new Corestore(ram)
  const output = new Omega(ram)
  const writerA = toPromises(store.get({ name: 'writer-a' }))
  const writerB = toPromises(store.get({ name: 'writer-b' }))
  const base = new Autobase([writerA, writerB])

  // TODO: This is a temporary fix.
  const outputReadProxy = new Proxy(output, {
    get (target, prop) {
      if (prop !== 'get') return target[prop]
      return async (idx, opts) => {
        const block = await target.get(idx)
        let val = IndexNode.decode(block).value
        if (opts.valueEncoding) val = opts.valueEncoding.decode(val)
        return val
      }
    }
  })
  const db = new Hyperbee(outputReadProxy, {
    keyEncoding: 'utf-8',
    extension: false,
    readonly: true
  })
  await db.ready()

  const machine = new Sandbox(p.join(INDEXER_MACHINE_DIR, 'index.js'), {
    hostcalls: {
      get: async (_, idx) => {
        const node = await output.get(idx)
        return IndexNode.decode(node).value
      }
    }
  })
  const mapper = async node => {
    const coreOpts = {
      length: output.length,
      byteLength: output.byteLength
    }
    const blocks = await machine.rpc.index(coreOpts, node)
    return blocks.map(b => Buffer.from(b.buffer, b.byteOffset, b.byteLength))
  }
  await machine.ready()

  await base.append(writerA, Op.encode({
    type: Op.Type.Put,
    key: 'hello',
    value: 'world'
  }))

  await base.append(writerB, Op.encode({
    type: Op.Type.Put,
    key: 'another',
    value: 'other'
  }))

  // This put will be causally-dependent on writerA's first put to 'hello'
  // So the final kv-pair should be 'hello' -> 'other'
  await base.append(writerB, Op.encode({
    type: Op.Type.Put,
    key: 'hello',
    value: 'other'
  }), await base.latest())

  await base.append(writerA, Op.encode({
    type: Op.Type.Del,
    key: 'another'
  }), await base.latest())

  await base.rebase(output, { map: mapper })

  console.log('\n ========== \n')
  console.log('All Hyperbee Nodes:\n')

  for await (const { key, value } of db.createReadStream()) {
    console.log(`${key} -> ${value}`)
  }

  t.end()
})
