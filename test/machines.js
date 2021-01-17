const p = require('path')
const test = require('tape')
const Omega = require('omega')
const Sandbox = require('module-sandbox')
const ram = require('random-access-memory')

const { indexedValues } = require('./helpers')
const Autobase = require('..')

const UPPERCASE_MACHINE_PATH = p.join(__dirname, 'machines', 'uppercase.js')
const DOUBLER_MACHINE_PATH = p.join(__dirname, 'machines', 'doubler.js')

test('rebase with mapping machine', async t => {
  const output = new Omega(ram)
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

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
    await base.localRebase(output, {
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
  const output = new Omega(ram)
  const writerA = new Omega(ram)
  const writerB = new Omega(ram)
  const writerC = new Omega(ram)

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
    await base.localRebase(output, { map: mapper })
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
    await base.localRebase(output, { map: mapper })
    const expected = []
    for (const val of ['b1', 'b0', 'c2', 'c1', 'c0', 'a3', 'a2', 'a1', 'a0']) {
      expected.push(val + ':second', val + ':first')
    }
    const indexed = await indexedValues(output)
    t.same(indexed.map(v => v.value), expected)
  }

  t.end()
})
