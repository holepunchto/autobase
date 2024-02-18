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

    t.alike(tip.ordered, [a0, b0, c0, c1])
  }

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(b0)
    tip.add(c1)

    t.alike(tip.ordered, [a0, b0, c0, c1])
  }

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(c1)
    tip.add(b0)

    t.alike(tip.ordered, [a0, b0, c0, c1])
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

  t.is(tip.pushed, 3)
  t.is(tip.popped, 2)
  t.is(tip.shared, 1)

  const b1 = makeNode('b', 1, [c1])

  tip.add(b1)

  t.is(tip.pushed, 4)
  t.is(tip.popped, 2)
  t.is(tip.shared, 1)

  tip.mark()

  const d0 = makeNode('d', 0, [c1])

  tip.add(d0)

  t.is(tip.pushed, 1)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  const c2 = makeNode('c', 2, [c1])

  tip.add(c2)

  t.is(tip.pushed, 2)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)
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

  t.is(tip.pushed, 0)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0])

  t.is(u.pushed, 2)
  t.is(u.popped, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 1)
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

  t.is(tip.pushed, 0)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const length = tip.ordered.length

  const u = tip.flush([c0])

  t.is(u.pushed, length)
  t.is(u.popped, 5)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 1)
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

  t.is(tip.pushed, 0)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0, b0])

  t.is(u.pushed, 2)
  t.is(u.popped, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 2)
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

  t.is(tip.pushed, 0)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0, c0])

  t.is(u.popped, 4)
  t.is(u.pushed, 6)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 2)
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

  t.is(tip.pushed, 1)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  const c2 = makeNode('c', 2, [c1])

  tip.add(c2)

  t.is(tip.pushed, 2)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  const u = tip.flush([c0, b0])

  t.is(u.popped, 5)
  t.is(u.pushed, 7)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 2)
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

  t.is(tip.pushed, 0)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0])

  t.is(u.popped, 0)
  t.is(u.pushed, 2)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 1)

  const u2 = tip.flush([b0])

  t.is(u2.popped, 0)
  t.is(u2.pushed, 0)
  t.is(u2.shared, 6)
  t.is(u2.indexed.length, 1)
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

  t.is(tip.pushed, 0)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0, b0, c0, c1, b1, c2, d0])

  t.is(u.popped, 0)
  t.is(u.pushed, 2)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 7)
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

  t.is(tip.pushed, 0)
  t.is(tip.popped, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0, c0, b0, c1, b1, c2, d0])

  t.is(u.popped, 4)
  t.is(u.pushed, 6)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 7)
})

function makeNode (key, length, dependencies, value = null) {
  return {
    writer: { core: { key: b4a.from(key) } },
    length,
    dependencies,
    value
  }
}
