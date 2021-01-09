const tape = require('tape')
const bitfield = require('./')

tape('basic', function (t) {
  const bits = bitfield()
  t.notOk(bits.get(0))
  t.ok(bits.set(0, true))
  t.notOk(bits.set(0, true))
  t.ok(bits.get(0))
  t.end()
})

tape('search', function (t) {
  const bits = bitfield()

  bits.set(1, true)
  bits.set(4, true)
  bits.set(42, true)
  bits.set(10004, true)

  const ite = bits.iterator()

  t.same(ite.next(true), 1)
  t.same(ite.next(true), 4)
  t.same(ite.next(true), 42)
  t.same(ite.next(true), 10004)
  t.same(ite.next(true), -1)

  ite.seek(0)

  t.same(ite.next(false), 0)
  t.same(ite.next(false), 2)
  t.same(ite.next(false), 3)
  t.same(ite.next(false), 5)

  t.end()
})

tape('random bits (true)', function (t) {
  const len = 100000
  const bits = bitfield()
  const set = []

  for (var i = 0; i < 50; i++) {
    const idx = randomBit(bits, len, false)
    set.push(idx)
    t.notOk(bits.get(idx))
    bits.set(idx, true)
  }

  set.sort((a, b) => a - b)
  const ite = bits.iterator()

  while (set.length) {
    t.same(ite.next(true), set.shift())
  }

  t.end()
})

tape('random bits (false)', function (t) {
  const len = 100000
  const bits = bitfield()
  const set = []

  for (var j = 0; j < len; j++) {
    bits.set(j, true)
  }

  for (var i = 0; i < 50; i++) {
    const idx = randomBit(bits, len, true)
    set.push(idx)
    t.ok(bits.get(idx))
    bits.set(idx, false)
  }

  set.sort((a, b) => a - b)
  const ite = bits.iterator()

  while (set.length) {
    t.same(ite.next(false), set.shift())
  }

  t.end()
})

tape('sparse and false iterator', function (t) {
  const bits = bitfield()

  bits.set(1000000000, true)
  t.same(bits.iterator().next(false), 0)
  t.same(bits.iterator().seek(100000).next(false), 100000)

  const ite = bits.iterator().seek(1000000)
  ite.next()
  bits.set(1000001, true)
  t.same(ite.next(), 1000002)

  t.same(bits.last(), 1000000000)

  t.end()
})

tape('fill', function (t) {
  const bits = bitfield()

  bits.set(10000000, true)
  bits.set(10000001, true)
  bits.fill(false, 0, 10000001)
  t.notOk(bits.get(10000000))
  t.ok(bits.get(10000001))
  bits.set(10000001, false)
  bits.fill(true, 10, 10000001)
  t.notOk(bits.get(9))
  t.ok(bits.get(10))
  t.ok(bits.get(10000000))
  t.notOk(bits.get(10000001))
  t.end()
})

tape('fill buffer', function (t) {
  const bits = bitfield()

  bits.set(42, true)
  bits.set(100000, true)
  bits.fill(Buffer.alloc(87654), 0)
  t.notOk(bits.get(42))
  t.notOk(bits.get(100000))
  t.end()
})

tape('fill buffer offset', function (t) {
  const bits = bitfield()

  const buf = Buffer.alloc(128 * 1024)
  buf.fill(0xff)
  bits.fill(buf, 9437184)

  t.ok(bits.get(1e7))
  t.end()
})

function randomBit (bits, len, bit) {
  const ite = bits.iterator().seek(Math.floor(Math.random() * len))
  const i = ite.next(bit)
  return i === -1 ? ite.seek(0).next(bit) : i
}
