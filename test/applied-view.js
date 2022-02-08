const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const { bufferize, causalValues } = require('./helpers')
const Autobase = require('../')
const AppliedView = require('../lib/linearizer/applied-view')

test('applied view - applied view on empty autobase', async t => {
  const base = new Autobase()
  const view = new AppliedView(base, {
    applied: [],
    clock: new Map()
  })
  t.same(await view.pop(), null)
  t.same(view.popped, 0)
  t.same(view.head, null)
  t.end()
})

test('applied view - pops causal nodes', async t => {
  const writerA = new Hypercore(ram)
  const base = new Autobase({
    inputs: [writerA],
    localInput: writerA
  })

  await base.append('a0')
  const clock1 = await base.latest()
  await base.append('a1')
  const clock2 = await base.latest()

  const applied = []
  for await (const node of base.createCausalStream()) {
    applied.unshift(node)
  }
  console.log('applied:', applied)

  await base.append('a2')

  const view = new AppliedView(base, {
    applied,
    clock: clock2
  })

  const expected = [clock2, clock1]

  let node = await view.pop()
  while (node) {
    t.same(node.clock, expected.pop())
    node = await view.pop()
  }
  t.same(expected.length, 0)
  t.same(view.clock, new Map())

  t.end()
})
