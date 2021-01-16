const p = require('path')
const test = require('tape')
const Corestore = require('corestore')
const Omega = require('omega')
const Sandbox = require('module-sandbox')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const { toPromises } = require('hypercore-promisifier')

const { IndexNode } = require('../lib/nodes')
const Autobase = require('..')

const INDEXER_MACHINE_DIR = p.join(__dirname, 'machines', 'indexer')
const { Op } = require(p.join(INDEXER_MACHINE_DIR, 'messages.js'))

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

  /*
  await base.append(writerA, Op.encode({
    type: Op.Type.Del,
    key: 'another'
  }), await base.latest())
  */

  const reduce = await createReducerMachine(output, p.join(INDEXER_MACHINE_DIR, 'index.js'))
  await base.localRebase(output, { map: reduce })

  console.log('\n ========== \n')
  console.log('All Hyperbee Nodes:\n')

  for await (const { key, value } of db.createReadStream()) {
    console.log(`${key} -> ${value}`)
  }

  t.end()
})

async function createReducerMachine (output, machinePath) {
  const machine = new Sandbox(p.join(INDEXER_MACHINE_DIR, 'index.js'), {
    hostcalls: {
      get: async (_, idx) => {
        const node = await output.get(idx)
        return IndexNode.decode(node).value
      },
      registerExtension: () => {}
    }
  })
  await machine.ready()
  await machine.rpc.init()

  return async node => {
    const indexState = { length: output.length, byteLength: output.byteLength }
    const blocks = await machine.rpc.reduce(indexState, node)
    return blocks.map(b => Buffer.from(b.buffer, b.byteOffset, b.byteLength))
  }
}
