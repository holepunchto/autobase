const test = require('brittle')
const b4a = require('b4a')
const Topolist = require('../lib/topolist')

test('topolist - stable ordering', function (t) {
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

test('topolist - can track changes', function (t) {
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

test('topolist - can shift', function (t) {
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

test('topolist - can shift out of order', function (t) {
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

test('topolist - can multiple shift', function (t) {
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

test('topolist - can multiple shift partially out of order', function (t) {
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

test('topolist - can multiple shift out of order', function (t) {
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

test('topolist - can shift multiple times', function (t) {
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

test('topolist - shift whole chain', function (t) {
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

test('topolist - shift whole chain out of order', function (t) {
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

test('topolist - reorder then shift', function (t) {
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
  tip.add(d0)
  tip.add(c2)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(b0)
  tip.add(b1)

  t.is(tip.undo, 4)
  t.is(tip.shared, 1)

  const yielded = [a0, b0, c0, c1, b1, c2, d0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 4)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 7)
  t.is(u.length, 7)
})

test('topolist - add reorder then shift', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [])
  const c0 = makeNode('c', 0, [b0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(b0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b1)
  tip.add(d0)
  tip.add(c2)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 6)

  tip.add(a0)

  t.is(tip.undo, 6)
  t.is(tip.shared, 0)

  const yielded = [a0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 6)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 1)
  t.is(u.length, 7)
})

test('topolist - reorder then shift reordered', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [])
  const c0 = makeNode('c', 0, [b0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(b0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b1)
  tip.add(d0)
  tip.add(c2)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 6)

  tip.add(a0)

  t.is(tip.undo, 6)
  t.is(tip.shared, 0)

  const yielded = [a0, b0, c0, c1, c2, b1]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 6)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 6)
  t.is(u.length, 7)
})

test('topolist - with versions', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [])
  const c0 = makeNode('c', 0, [b0])
  const c1 = makeNode('c', 1, [], { version: 1 })
  const b1 = makeNode('b', 1, [c0])
  const d0 = makeNode('d', 0, [c0])
  const c2 = makeNode('c', 2, [d0], { version: 1 })
  const d1 = makeNode('d', 1, [])

  tip.add(b0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b1)
  tip.add(d0)
  tip.add(c2)
  tip.add(a0)

  t.is(tip.tip[tip.tip.length - 1], c2)

  tip.mark()

  tip.add(d1)

  t.is(tip.undo, 0)
})

test('topolist - optimistic', function (t) {
  const tip = new Topolist()

  const x0 = makeNode('x', 0, [])
  const h0 = makeNode('h', 0, [], { optimistic: true })
  const i0 = makeNode('i', 0, [h0])
  const a0 = makeNode('a', 0, [], { optimistic: true })
  const b0 = makeNode('b', 0, [], { optimistic: true })
  const d0 = makeNode('d', 0, [a0, x0])
  const c0 = makeNode('c', 0, [a0, b0, i0])
  const e0 = makeNode('e', 0, [a0])

  tip.add(x0)
  tip.add(h0)
  tip.add(i0)
  tip.add(b0)
  tip.add(a0)
  tip.add(c0)

  t.alike(tip.tip, [h0, i0, a0, b0, c0, x0])

  tip.mark()
  tip.add(d0)

  t.alike(tip.tip, [h0, i0, a0, b0, c0, x0, d0])

  t.is(tip.undo, 0)
  t.is(tip.shared, 6)

  tip.mark()
  tip.add(e0)

  t.alike(tip.tip, [a0, e0, h0, i0, b0, c0, x0, d0])
  t.is(tip.undo, 7)
  t.is(tip.shared, 0)

  const tip2 = new Topolist()

  tip2.add(h0)
  tip2.add(x0)
  tip2.add(a0)
  tip2.add(i0)
  tip2.add(e0)
  tip2.add(d0)
  tip2.add(b0)
  tip2.add(c0)

  t.alike(tip.tip, tip2.tip, 'sanity check')
})

test('topolist - optimistic 2', function (t) {
  const tip = new Topolist()

  const h0 = makeNode('h', 0, [])
  const i0 = makeNode('i', 0, [h0])
  const a0 = makeNode('a', 0, [], { optimistic: true })
  const b0 = makeNode('b', 0, [a0], { optimistic: true })
  const c0 = makeNode('c', 0, [a0], { optimistic: true })
  const d0 = makeNode('d', 0, [c0, b0], { optimistic: true })
  const e0 = makeNode('e', 0, [d0])

  tip.add(h0)
  tip.add(i0)
  tip.add(a0)
  tip.add(c0)
  tip.add(b0)
  tip.add(d0)
  tip.add(e0)

  t.alike(tip.tip, [a0, b0, c0, d0, e0, h0, i0])
})

test('topolist - optimistic 3', function (t) {
  const n0o = makeNode('n0o', 0, [], { value: 'n0o', optimistic: true })
  const n1o = makeNode('n1o', 0, [], { value: 'n1o', optimistic: true })
  const n2 = makeNode('n2', 0, [n1o], { value: 'n2', optimistic: false })
  const n3 = makeNode('n3', 0, [], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [], { value: 'n4', optimistic: false })
  const n5 = makeNode('n5', 0, [n4, n2, n3], { value: 'n5', optimistic: false })
  const n6 = makeNode('n6', 0, [n4, n1o, n2], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n0o, n2], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [n3, n1o], { value: 'n8', optimistic: false })

  const ref = new Topolist()
  ref.add(n0o)
  ref.add(n1o)
  ref.add(n2)
  ref.add(n3)
  ref.add(n4)
  ref.add(n5)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)

  const bug = new Topolist()
  bug.add(n0o)
  bug.add(n4)
  bug.add(n3)
  bug.add(n1o)
  bug.add(n8)
  bug.add(n2)
  bug.add(n6)
  bug.add(n5)
  bug.add(n7)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 4', function (t) {
  const n0o = makeNode('n0o', 0, [], { value: 'n0o', optimistic: true })
  const n1o = makeNode('n1o', 0, [], { value: 'n1o', optimistic: true })
  const n2 = makeNode('n2', 0, [n1o], { value: 'n2', optimistic: false })
  const n3 = makeNode('n3', 0, [n0o, n2], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [], { value: 'n4', optimistic: false })

  const ref = new Topolist()

  ref.add(n0o)
  ref.add(n1o)
  ref.add(n2)
  ref.add(n3)
  ref.add(n4)

  const bug = new Topolist()
  bug.add(n0o)
  bug.add(n1o)
  bug.add(n4)
  bug.add(n2)
  bug.add(n3)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 5', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [], { value: 'n1', optimistic: false })
  const n2 = makeNode('n2', 0, [n1], { value: 'n2', optimistic: false })
  const n3o = makeNode('n3o', 0, [n1], { value: 'n3o', optimistic: true })
  const n4 = makeNode('n4', 0, [n2, n1, n3o], { value: 'n4', optimistic: false })
  const n5 = makeNode('n5', 0, [n0, n1], { value: 'n5', optimistic: false })
  const n6 = makeNode('n6', 0, [n2, n5], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n4], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [n2, n3o], { value: 'n8', optimistic: false })
  const n9o = makeNode('n9o', 0, [n4], { value: 'n9o', optimistic: true })
  const n10o = makeNode('n10o', 0, [n1, n9o], { value: 'n10o', optimistic: true })
  const n11o = makeNode('n11o', 0, [n1], { value: 'n11o', optimistic: true })
  const n12 = makeNode('n12', 0, [n3o, n7], { value: 'n12', optimistic: false })
  const n13 = makeNode('n13', 0, [n8, n4, n6], { value: 'n13', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2)
  ref.add(n3o)
  ref.add(n4)
  ref.add(n5)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)
  ref.add(n9o)
  ref.add(n10o)
  ref.add(n11o)
  ref.add(n12)
  ref.add(n13)

  const bug = new Topolist()
  bug.add(n1)
  bug.add(n11o)
  bug.add(n0)
  bug.add(n3o)
  bug.add(n2)
  bug.add(n8)
  bug.add(n4)
  bug.add(n5)
  bug.add(n7)
  bug.add(n6)
  bug.add(n12)
  bug.add(n13)
  bug.add(n9o)
  bug.add(n10o)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 6', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [], { value: 'n1', optimistic: false })
  const n2 = makeNode('n2', 0, [n1, n0], { value: 'n2', optimistic: false })
  const n3 = makeNode('n3', 0, [n1, n2, n0], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [n3, n1], { value: 'n4', optimistic: false })
  const n5o = makeNode('n5o', 0, [n1], { value: 'n5o', optimistic: true })
  const n6 = makeNode('n6', 0, [], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n6], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [], { value: 'n8', optimistic: false })
  const n9o = makeNode('n9o', 0, [n5o], { value: 'n9o', optimistic: true })
  const n10 = makeNode('n10', 0, [n5o], { value: 'n10', optimistic: false })
  const n11 = makeNode('n11', 0, [], { value: 'n11', optimistic: false })
  const n12 = makeNode('n12', 0, [n3, n7], { value: 'n12', optimistic: false })
  const n13o = makeNode('n13o', 0, [n12, n9o], { value: 'n13o', optimistic: true })
  const n14 = makeNode('n14', 0, [n6, n0], { value: 'n14', optimistic: false })
  const n15 = makeNode('n15', 0, [n10, n3], { value: 'n15', optimistic: false })
  const n16 = makeNode('n16', 0, [n6, n3, n10], { value: 'n16', optimistic: false })
  const n17o = makeNode('n17o', 0, [n3, n11], { value: 'n17o', optimistic: true })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2)
  ref.add(n3)
  ref.add(n4)
  ref.add(n5o)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)
  ref.add(n9o)
  ref.add(n10)
  ref.add(n11)
  ref.add(n12)
  ref.add(n13o)
  ref.add(n14)
  ref.add(n15)
  ref.add(n16)
  ref.add(n17o)

  const bug = new Topolist()
  bug.add(n8)
  bug.add(n6)
  bug.add(n11)
  bug.add(n7)
  bug.add(n0)
  bug.add(n14)
  bug.add(n1)
  bug.add(n2)
  bug.add(n5o)
  bug.add(n10)
  bug.add(n9o)
  bug.add(n3)
  bug.add(n4)
  bug.add(n15)
  bug.add(n16)
  bug.add(n17o)
  bug.add(n12)
  bug.add(n13o)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 7', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [n0], { value: 'n1', optimistic: false })
  const n2o = makeNode('n2o', 0, [n1], { value: 'n2o', optimistic: true })
  const n3o = makeNode('n3o', 0, [n2o, n1], { value: 'n3o', optimistic: true })
  const n4 = makeNode('n4', 0, [], { value: 'n4', optimistic: false })
  const n5o = makeNode('n5o', 0, [n4], { value: 'n5o', optimistic: true })
  const n6 = makeNode('n6', 0, [n5o], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n0], { value: 'n7', optimistic: false })
  const n8o = makeNode('n8o', 0, [n6], { value: 'n8o', optimistic: true })
  const n9 = makeNode('n9', 0, [n0], { value: 'n9', optimistic: false })
  const n10 = makeNode('n10', 0, [n7], { value: 'n10', optimistic: false })
  const n11 = makeNode('n11', 0, [], { value: 'n11', optimistic: false })
  const n12 = makeNode('n12', 0, [n0], { value: 'n12', optimistic: false })
  const n13 = makeNode('n13', 0, [n2o], { value: 'n13', optimistic: false })
  const n14o = makeNode('n14o', 0, [n1, n7, n2o], { value: 'n14o', optimistic: true })
  const n15 = makeNode('n15', 0, [n4, n2o], { value: 'n15', optimistic: false })
  const n16o = makeNode('n16o', 0, [n1], { value: 'n16o', optimistic: true })
  const n17 = makeNode('n17', 0, [n13, n9, n16o], { value: 'n17', optimistic: false })
  const n18 = makeNode('n18', 0, [n0], { value: 'n18', optimistic: false })
  const n19 = makeNode('n19', 0, [], { value: 'n19', optimistic: false })
  const n20 = makeNode('n20', 0, [n19, n10, n13], { value: 'n20', optimistic: false })
  const n21 = makeNode('n21', 0, [n1, n18], { value: 'n21', optimistic: false })
  const n22 = makeNode('n22', 0, [n18, n7], { value: 'n22', optimistic: false })
  const n23 = makeNode('n23', 0, [n3o, n0], { value: 'n23', optimistic: false })
  const n24o = makeNode('n24o', 0, [n11, n2o, n19], { value: 'n24o', optimistic: true })
  const n25 = makeNode('n25', 0, [], { value: 'n25', optimistic: false })
  const n26 = makeNode('n26', 0, [n17, n9], { value: 'n26', optimistic: false })
  const n27 = makeNode('n27', 0, [n23], { value: 'n27', optimistic: false })
  const n28 = makeNode('n28', 0, [n13, n24o], { value: 'n28', optimistic: false })
  const n29 = makeNode('n29', 0, [n13], { value: 'n29', optimistic: false })
  const n30 = makeNode('n30', 0, [n2o], { value: 'n30', optimistic: false })
  const n31 = makeNode('n31', 0, [n0], { value: 'n31', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2o)
  ref.add(n3o)
  ref.add(n4)
  ref.add(n5o)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8o)
  ref.add(n9)
  ref.add(n10)
  ref.add(n11)
  ref.add(n12)
  ref.add(n13)
  ref.add(n14o)
  ref.add(n15)
  ref.add(n16o)
  ref.add(n17)
  ref.add(n18)
  ref.add(n19)
  ref.add(n20)
  ref.add(n21)
  ref.add(n22)
  ref.add(n23)
  ref.add(n24o)
  ref.add(n25)
  ref.add(n26)
  ref.add(n27)
  ref.add(n28)
  ref.add(n29)
  ref.add(n30)
  ref.add(n31)

  const bug = new Topolist()
  bug.add(n4)
  bug.add(n19)
  bug.add(n25)
  bug.add(n0)
  bug.add(n18)
  bug.add(n9)
  bug.add(n31)
  bug.add(n7)
  bug.add(n10)
  bug.add(n12)
  bug.add(n5o)
  bug.add(n6)
  bug.add(n1)
  bug.add(n16o)
  bug.add(n21)
  bug.add(n2o)
  bug.add(n3o)
  bug.add(n23)
  bug.add(n30)
  bug.add(n15)
  bug.add(n11)
  bug.add(n8o)
  bug.add(n13)
  bug.add(n27)
  bug.add(n17)
  bug.add(n14o)
  bug.add(n22)
  bug.add(n20)
  bug.add(n26)
  bug.add(n24o)
  bug.add(n28)
  bug.add(n29)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 8', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [], { value: 'n1', optimistic: false })
  const n2 = makeNode('n2', 0, [n1], { value: 'n2', optimistic: false })
  const n3 = makeNode('n3', 0, [n1, n2], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [n0, n1, n3], { value: 'n4', optimistic: false })
  const n5 = makeNode('n5', 0, [n0, n4], { value: 'n5', optimistic: false })
  const n6 = makeNode('n6', 0, [n5, n3, n4], { value: 'n6', optimistic: false })
  const n7o = makeNode('n7o', 0, [], { value: 'n7o', optimistic: true })
  const n8 = makeNode('n8', 0, [n2], { value: 'n8', optimistic: false })
  const n9 = makeNode('n9', 0, [], { value: 'n9', optimistic: false })
  const n10 = makeNode('n10', 0, [n5], { value: 'n10', optimistic: false })
  const n11 = makeNode('n11', 0, [n6, n9], { value: 'n11', optimistic: false })
  const n12 = makeNode('n12', 0, [n4], { value: 'n12', optimistic: false })
  const n13 = makeNode('n13', 0, [n8, n4], { value: 'n13', optimistic: false })
  const n14 = makeNode('n14', 0, [n1, n2], { value: 'n14', optimistic: false })
  const n15 = makeNode('n15', 0, [n4, n8], { value: 'n15', optimistic: false })
  const n16 = makeNode('n16', 0, [n12], { value: 'n16', optimistic: false })
  const n17 = makeNode('n17', 0, [n7o, n13], { value: 'n17', optimistic: false })
  const n18 = makeNode('n18', 0, [n15, n10, n0], { value: 'n18', optimistic: false })
  const n19 = makeNode('n19', 0, [n7o], { value: 'n19', optimistic: false })
  const n20o = makeNode('n20o', 0, [n12, n1], { value: 'n20o', optimistic: true })
  const n21 = makeNode('n21', 0, [n5, n9], { value: 'n21', optimistic: false })
  const n22 = makeNode('n22', 0, [n2, n18, n15], { value: 'n22', optimistic: false })
  const n23o = makeNode('n23o', 0, [n8], { value: 'n23o', optimistic: true })
  const n24 = makeNode('n24', 0, [n21, n7o], { value: 'n24', optimistic: false })
  const n25 = makeNode('n25', 0, [n0, n7o], { value: 'n25', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2)
  ref.add(n3)
  ref.add(n4)
  ref.add(n5)
  ref.add(n6)
  ref.add(n7o)
  ref.add(n8)
  ref.add(n9)
  ref.add(n10)
  ref.add(n11)
  ref.add(n12)
  ref.add(n13)
  ref.add(n14)
  ref.add(n15)
  ref.add(n16)
  ref.add(n17)
  ref.add(n18)
  ref.add(n19)
  ref.add(n20o)
  ref.add(n21)
  ref.add(n22)
  ref.add(n23o)
  ref.add(n24)
  ref.add(n25)

  const bug = new Topolist()
  bug.add(n7o)
  bug.add(n1)
  bug.add(n2)
  bug.add(n0)
  bug.add(n14)
  bug.add(n19)
  bug.add(n8)
  bug.add(n9)
  bug.add(n25)
  bug.add(n23o)
  bug.add(n3)
  bug.add(n4)
  bug.add(n13)
  bug.add(n15)
  bug.add(n17)
  bug.add(n5)
  bug.add(n21)
  bug.add(n10)
  bug.add(n18)
  bug.add(n24)
  bug.add(n22)
  bug.add(n6)
  bug.add(n11)
  bug.add(n12)
  bug.add(n16)
  bug.add(n20o)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 9', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [n0], { value: 'n1', optimistic: false })
  const n2 = makeNode('n2', 0, [], { value: 'n2', optimistic: false })
  const n3o = makeNode('n3o', 0, [], { value: 'n3o', optimistic: true })
  const n4o = makeNode('n4o', 0, [n2, n3o, n1], { value: 'n4o', optimistic: true })
  const n5o = makeNode('n5o', 0, [n4o, n0], { value: 'n5o', optimistic: true })
  const n6 = makeNode('n6', 0, [n5o, n0], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n1], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [n4o, n3o], { value: 'n8', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2)
  ref.add(n3o)
  ref.add(n4o)
  ref.add(n5o)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)

  const bug = new Topolist()
  bug.add(n3o)
  bug.add(n2)
  bug.add(n0)
  bug.add(n1)
  bug.add(n4o)
  bug.add(n5o)
  bug.add(n7)
  bug.add(n6)
  bug.add(n8)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 10', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1o = makeNode('n1o', 0, [n0], { value: 'n1o', optimistic: true })
  const n2o = makeNode('n2o', 0, [n1o], { value: 'n2o', optimistic: true })
  const n3 = makeNode('n3', 0, [], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [n1o, n2o], { value: 'n4', optimistic: false })
  const n5 = makeNode('n5', 0, [n4, n3], { value: 'n5', optimistic: false })
  const n6 = makeNode('n6', 0, [], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n1o], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [n0, n3, n5], { value: 'n8', optimistic: false })
  const n9 = makeNode('n9', 0, [n7, n3], { value: 'n9', optimistic: false })
  const n10o = makeNode('n10o', 0, [n2o], { value: 'n10o', optimistic: true })
  const n11 = makeNode('n11', 0, [], { value: 'n11', optimistic: false })
  const n12 = makeNode('n12', 0, [n3], { value: 'n12', optimistic: false })
  const n13 = makeNode('n13', 0, [n7], { value: 'n13', optimistic: false })
  const n14 = makeNode('n14', 0, [], { value: 'n14', optimistic: false })
  const n15 = makeNode('n15', 0, [n10o, n11], { value: 'n15', optimistic: false })
  const n16 = makeNode('n16', 0, [n11], { value: 'n16', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1o)
  ref.add(n2o)
  ref.add(n3)
  ref.add(n4)
  ref.add(n5)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)
  ref.add(n9)
  ref.add(n10o)
  ref.add(n11)
  ref.add(n12)
  ref.add(n13)
  ref.add(n14)
  ref.add(n15)
  ref.add(n16)

  const bug = new Topolist()
  bug.add(n14)
  bug.add(n3)
  bug.add(n12)
  bug.add(n6)
  bug.add(n11)
  bug.add(n0)
  bug.add(n1o)
  bug.add(n2o)
  bug.add(n7)
  bug.add(n10o)
  bug.add(n13)
  bug.add(n15)
  bug.add(n4)
  bug.add(n5)
  bug.add(n8)
  bug.add(n9)
  bug.add(n16)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - optimistic 11', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [], { value: 'n1', optimistic: false })
  const n2o = makeNode('n2o', 0, [], { value: 'n2o', optimistic: true })
  const n3 = makeNode('n3', 0, [n1, n2o, n0], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [n0, n3], { value: 'n4', optimistic: false })
  const n5 = makeNode('n5', 0, [], { value: 'n5', optimistic: false })
  const n6 = makeNode('n6', 0, [], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [], { value: 'n7', optimistic: false })
  const n8o = makeNode('n8o', 0, [], { value: 'n8o', optimistic: true })
  const n9o = makeNode('n9o', 0, [n4], { value: 'n9o', optimistic: true })
  const n10 = makeNode('n10', 0, [n1, n7, n4], { value: 'n10', optimistic: false })
  const n11 = makeNode('n11', 0, [n7, n3, n0], { value: 'n11', optimistic: false })
  const n12 = makeNode('n12', 0, [n3], { value: 'n12', optimistic: false })
  const n13 = makeNode('n13', 0, [n3, n1, n7], { value: 'n13', optimistic: false })
  const n14 = makeNode('n14', 0, [n11, n1, n5], { value: 'n14', optimistic: false })
  const n15o = makeNode('n15o', 0, [n7, n10, n9o], { value: 'n15o', optimistic: true })
  const n16o = makeNode('n16o', 0, [n5, n9o], { value: 'n16o', optimistic: true })
  const n17 = makeNode('n17', 0, [n7], { value: 'n17', optimistic: false })
  const n18 = makeNode('n18', 0, [n15o, n2o, n17], { value: 'n18', optimistic: false })
  const n19 = makeNode('n19', 0, [], { value: 'n19', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2o)
  ref.add(n3)
  ref.add(n4)
  ref.add(n5)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8o)
  ref.add(n9o)
  ref.add(n10)
  ref.add(n11)
  ref.add(n12)
  ref.add(n13)
  ref.add(n14)
  ref.add(n15o)
  ref.add(n16o)
  ref.add(n17)
  ref.add(n18)
  ref.add(n19)

  const bug = new Topolist()
  bug.add(n1)
  bug.add(n2o)
  bug.add(n6)
  bug.add(n19)
  bug.add(n7)
  bug.add(n8o)
  bug.add(n0)
  bug.add(n5)
  bug.add(n17)
  bug.add(n3)
  bug.add(n4)
  bug.add(n10)
  bug.add(n11)
  bug.add(n12)
  bug.add(n14)
  bug.add(n9o)
  bug.add(n15o)
  bug.add(n13)
  bug.add(n18)
  bug.add(n16o)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test('topolist - fuzz', function (t) {
  const runs = 1e1 // adjust if mining
  const tests = 1e3

  if (runAll()) t.pass('worked for ' + (runs * tests) + ' runs')

  function runAll () {
    for (let i = 0; i < runs; i++) {
      t.comment('fuzz #' + i)
      const nodes = []

      const max = Math.random() * 20
      while (nodes.length < max) makeRandomNode(nodes)

      const tip = new Topolist()
      for (const n of nodes) {
        tip.add(n)
      }

      const ref = tip.print().join(' ')

      for (let j = 0; j < tests; j++) {
        if (!run(ref, nodes)) return false
      }
    }

    return true
  }

  function run (ref, nodes) {
    const tip = new Topolist()
    const all = [...nodes]
    const replay = []

    while (all.length) {
      const r = all[(Math.random() * all.length) | 0]
      let bad = false
      for (const d of r.dependencies) {
        if (all.indexOf(d) !== -1) {
          bad = true
          break
        }
      }
      if (bad) continue
      replay.push(r.value)
      all.splice(all.indexOf(r), 1)
      tip.add(r)
    }

    const order = tip.print().join(' ')

    if (order !== ref) {
      for (const n of nodes) {
        console.log('const ' + n.value + ' = makeNode(\'' + n.value + '\', 0, [' + n.dependencies.map(x => x.value).join(', ') + '], { value: \'' + n.value + '\', optimistic: ' + n.optimistic + ' })')
      }

      console.log('const ref = new Topolist()')
      for (const n of nodes) {
        console.log('ref.add(' + n.value + ')')
      }
      console.log()
      console.log('const bug = new Topolist()')
      for (const r of replay) {
        console.log('bug.add(' + r + ')')
      }
      console.log()
      console.log('console.log(bug.print().join(\' \') === ref.print().join(\' \'))')

      t.comment('ref: ' + ref)
      t.comment('run: ' + order)
      t.fail('bad order')
      return false
    }

    return true
  }

  function makeRandomNode (nodes) {
    const heads = Math.round(Math.random() * Math.min(nodes.length, 3))

    const h = []
    while (h.length < heads) {
      const r = nodes[(Math.random() * nodes.length) | 0]
      if (h.indexOf(r) !== -1) continue
      h.push(r)
    }

    const hasOptimistic = nodes.filter(n => n.optimistic).length

    const optimistic = hasOptimistic === 100 ? false : Math.random() < 0.15
    const v = 'n' + nodes.length + (optimistic ? 'o' : '')
    nodes.push(makeNode(v, 0, h, { value: v, optimistic }))
  }
})

function makeNode (key, length, dependencies, { version = 0, value = key + length, optimistic = false } = {}) {
  const node = {
    writer: { core: { key: b4a.from(key) } },
    length,
    dependents: new Set(),
    dependencies,
    version,
    optimistic,
    value
  }

  for (const dep of dependencies) dep.dependents.add(node)

  return node
}
