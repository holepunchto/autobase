const test = require('brittle')
const b4a = require('b4a')
const Topolist = require('../lib/topolist')

test('stable ordering', function (t) {
  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [a0])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(c1)
    tip.add(b0)

    t.alike(tip.tip, [a0, b0, c0, c1])
  }

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(b0)
    tip.add(c1)

    t.alike(tip.tip, [a0, b0, c0, c1])
  }

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(c1)
    tip.add(b0)

    t.alike(tip.tip, [a0, b0, c0, c1])
  }
})

test('can track changes', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)

  tip.mark()

  const b0 = makeNode('b', 0, [a0])
  tip.add(b0)

  t.is(tip.undo, 2)
  t.is(tip.shared, 1)

  const b1 = makeNode('b', 1, [c1])

  tip.add(b1)

  t.is(tip.undo, 2)
  t.is(tip.shared, 1)

  tip.mark()

  const d0 = makeNode('d', 0, [c1])

  tip.add(d0)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  const c2 = makeNode('c', 2, [c1])

  tip.add(c2)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  const u = tip.flush()
  t.is(u.shared, 5)
  t.is(u.undo, 0)
  t.is(u.length, 7)
})

test('can shift', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  a0.yielded = true

  const u = tip.flush([a0])

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 1)
  t.is(u.length, 7)
})

test('can shift out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  c0.yielded = true

  const u = tip.flush([c0])

  t.is(u.undo, 5)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 1)
  t.is(u.tip.length, 6)
  t.is(u.length, 7)
})

test('can multiple shift', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  a0.yielded = true
  b0.yielded = true

  const u = tip.flush([a0, b0])

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 2)
  t.is(u.length, 7)
})

test('can multiple shift partially out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  a0.yielded = true
  c0.yielded = true

  const u = tip.flush([a0, c0])

  t.is(u.undo, 4)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 2)
  t.is(u.length, 7)
})

test('can multiple shift out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [c0])
  const b1 = makeNode('b', 1, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  const d0 = makeNode('d', 0, [c1])

  tip.add(d0)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  const c2 = makeNode('c', 2, [c1])

  tip.add(c2)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  c0.yielded = true
  b0.yielded = true

  const u = tip.flush([c0, b0])

  t.is(u.undo, 5)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 2)
  t.is(u.length, 7)
})

test('can shift multiple times', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const b0 = makeNode('b', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0])

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 1)

  b0.yielded = true

  const u2 = tip.flush([b0])

  t.is(u2.undo, 0)
  t.is(u2.shared, 6)
  t.is(u2.indexed.length, 1)
  t.is(u.length, 7)
})

test('shift whole chain', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const b0 = makeNode('b', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const yielded = [a0, b0, c0, c1, b1, c2, d0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 7)
  t.is(u.length, 7)
})

test('shift whole chain out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const b0 = makeNode('b', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const yielded = [a0, c0, b0, c1, b1, c2, d0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 4)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 7)
  t.is(u.length, 7)
})

function makeNode (key, length, dependencies, value = null) {
  return {
    writer: { core: { key: b4a.from(key) } },
    length,
    dependencies,
    value
  }
}
