const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')
const c = require('compact-encoding')

const HypercoreBisector = require('../lib/view/bisect')

const BatchBlock = {
  preencode (state, b) {
    c.uint.preencode(state, b.pos)
    if (b.pos === 0) c.uint.preencode(state, b.value)
  },
  encode (state, b) {
    c.uint.encode(state, b.pos)
    if (b.pos === 0) c.uint.encode(state, b.value)
  },
  decode (state) {
    const pos = c.uint.decode(state)
    return {
      value: pos === 0 ? c.uint.decode(state) : null,
      pos
    }
  }
}

test('bisector - simple incrementing core', async t => {
  const CORE_LENGTH = 1000

  const core = new Hypercore(ram, { valueEncoding: encoding(c.uint) })
  const expected = []
  for (let i = 0; i < CORE_LENGTH; i++) {
    expected.push(i)
    await core.append(i)
  }
  const values = []
  for (let i = 0; i < CORE_LENGTH; i++) {
    const bisect = new HypercoreBisector(core, {
      cmp: block => {
        return i - block
      }
    })
    values.push(await bisect.search())
  }

  t.same(values, expected)
})

test('bisector - incrementing core with batch skipping', async t => {
  const BATCH_SIZE = 7
  const CORE_LENGTH = 1000

  const core = new Hypercore(ram, { valueEncoding: encoding(BatchBlock) })
  const expected = []
  for (let i = 0; i < CORE_LENGTH; i++) {
    expected.push(i)
    const batch = []
    for (let j = 0; j < BATCH_SIZE; j++) {
      batch.push({
        pos: j,
        value: j === 0 ? i : null
      })
    }
    await core.append(batch)
  }

  const values = []
  for (let i = 0; i < CORE_LENGTH; i++) {
    const bisect = new HypercoreBisector(core, {
      skip: block => {
        return block.pos
      },
      cmp: block => {
        return i - block.value
      }
    })
    values.push((await bisect.search()).value)
  }

  t.same(values, expected)
})

test('bisector - short-circuit with invalid head', async t => {
  const core = new Hypercore(ram, { valueEncoding: encoding(c.uint) })
  await core.append([1, 2, 3, 4, 5])
  const target = 2

  const bisect = new HypercoreBisector(core, {
    cmp: block => {
      return target - block
    },
    validate: block => {
      // If we encounter a block with value 4, assume this core won't contain the target
      return block !== 4
    }
  })
  t.same(await bisect.search(), null)
})

test('bisector - searching for a value larger than the larget value in the core', async t => {
  const core = new Hypercore(ram, { valueEncoding: encoding(c.uint) })
  await core.append([1, 2, 3, 4, 5])
  const target = 6

  const bisect = new HypercoreBisector(core, {
    cmp: block => {
      return target - block
    }
  })
  t.same(await bisect.search(), null)
})

test('bisector - searching for a value smaller than the smallest value in the core', async t => {
  const core = new Hypercore(ram, { valueEncoding: encoding(c.uint) })
  await core.append([1, 2, 3, 4, 5])
  const target = 0

  const bisect = new HypercoreBisector(core, {
    cmp: block => {
      return target - block
    }
  })
  t.same(await bisect.search(), null)
})

test('bisector - searching for a value in a gap', async t => {
  const core = new Hypercore(ram, { valueEncoding: encoding(c.uint) })
  await core.append([1, 2, 4, 5])
  const target = 3

  const bisect = new HypercoreBisector(core, {
    cmp: block => {
      return target - block
    }
  })
  t.same(await bisect.search(), null)
})

function encoding (enc) {
  return { encode: v => c.encode(enc, v), decode: v => c.decode(enc, v) }
}
