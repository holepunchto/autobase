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

  tip.add(x0)
  tip.add(h0)
  tip.add(i0)
  tip.add(b0)
  tip.add(a0)
  tip.add(c0)

  t.alike(tip.tip, [x0, h0, i0, a0, b0, c0])

  tip.mark()
  tip.add(d0)

  t.alike(tip.tip, [x0, a0, d0, h0, i0, b0, c0])

  t.is(tip.undo, 5)
  t.is(tip.shared, 1)
})

test.solo('topolist - optimistic 2', function (t) {
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

  t.alike(tip.tip, [h0, i0, a0, b0, c0, d0, e0])
})

test.solo('topolist - optimistic 3', function (t) {
  const n0_o = makeNode('n0_o', 0, [], { value: 'n0_o', optimistic: true })
  const n1_o = makeNode('n1_o', 0, [], { value: 'n1_o', optimistic: true })
  const n2 = makeNode('n2', 0, [n1_o], { value: 'n2', optimistic: false })
  const n3 = makeNode('n3', 0, [], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [], { value: 'n4', optimistic: false })
  const n5 = makeNode('n5', 0, [n4, n2, n3], { value: 'n5', optimistic: false })
  const n6 = makeNode('n6', 0, [n4, n1_o, n2], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n0_o, n2], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [n3, n1_o], { value: 'n8', optimistic: false })

  const ref = new Topolist()
  ref.add(n0_o)
  ref.add(n1_o)
  ref.add(n2)
  ref.add(n3)
  ref.add(n4)
  ref.add(n5)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)

  const bug = new Topolist()
  bug.add(n0_o)
  bug.add(n4)
  bug.add(n3)
  bug.add(n1_o)
  bug.add(n8)
  bug.add(n2)
  bug.add(n6)
  bug.add(n5)
  bug.add(n7)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test.solo('topolist - optimistic 4', function (t) {
  const n0_o = makeNode('n0_o', 0, [], { value: 'n0_o', optimistic: true })
  const n1_o = makeNode('n1_o', 0, [], { value: 'n1_o', optimistic: true })
  const n2 = makeNode('n2', 0, [n1_o], { value: 'n2', optimistic: false })
  const n3 = makeNode('n3', 0, [n0_o, n2], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [], { value: 'n4', optimistic: false })

  const ref = new Topolist()

  ref.add(n0_o)
  ref.add(n1_o)
  ref.add(n2)
  ref.add(n3)
  ref.add(n4)

  const bug = new Topolist()
  bug.add(n0_o)
  bug.add(n1_o)
  bug.add(n4)
  bug.add(n2)
  bug.add(n3)

  // console.log('------')
  // sortOptimistic(n0_o, bug.tip, bug.tip.indexOf(n0_o), 0)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test.solo('topolist - optimistic 5', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [], { value: 'n1', optimistic: false })
  const n2 = makeNode('n2', 0, [n1], { value: 'n2', optimistic: false })
  const n3_o = makeNode('n3_o', 0, [n1], { value: 'n3_o', optimistic: true })
  const n4 = makeNode('n4', 0, [n2, n1, n3_o], { value: 'n4', optimistic: false })
  const n5 = makeNode('n5', 0, [n0, n1], { value: 'n5', optimistic: false })
  const n6 = makeNode('n6', 0, [n2, n5], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n4], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [n2, n3_o], { value: 'n8', optimistic: false })
  const n9_o = makeNode('n9_o', 0, [n4], { value: 'n9_o', optimistic: true })
  const n10_o = makeNode('n10_o', 0, [n1, n9_o], { value: 'n10_o', optimistic: true })
  const n11_o = makeNode('n11_o', 0, [n1], { value: 'n11_o', optimistic: true })
  const n12 = makeNode('n12', 0, [n3_o, n7], { value: 'n12', optimistic: false })
  const n13 = makeNode('n13', 0, [n8, n4, n6], { value: 'n13', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2)
  ref.add(n3_o)
  ref.add(n4)
  ref.add(n5)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)
  ref.add(n9_o)
  ref.add(n10_o)
  ref.add(n11_o)
  ref.add(n12)
  ref.add(n13)

  const bug = new Topolist()
  bug.add(n1)
  bug.add(n11_o)
  bug.add(n0)
  bug.add(n3_o)
  bug.add(n2)
  bug.add(n8)
  bug.add(n4)
  bug.add(n5)
  bug.add(n7)
  bug.add(n6)
  bug.add(n12)
  bug.add(n13)
  bug.add(n9_o)
  bug.add(n10_o)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test.solo('topolist - optimistic 6', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [], { value: 'n1', optimistic: false })
  const n2 = makeNode('n2', 0, [n1, n0], { value: 'n2', optimistic: false })
  const n3 = makeNode('n3', 0, [n1, n2, n0], { value: 'n3', optimistic: false })
  const n4 = makeNode('n4', 0, [n3, n1], { value: 'n4', optimistic: false })
  const n5_o = makeNode('n5_o', 0, [n1], { value: 'n5_o', optimistic: true })
  const n6 = makeNode('n6', 0, [], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n6], { value: 'n7', optimistic: false })
  const n8 = makeNode('n8', 0, [], { value: 'n8', optimistic: false })
  const n9_o = makeNode('n9_o', 0, [n5_o], { value: 'n9_o', optimistic: true })
  const n10 = makeNode('n10', 0, [n5_o], { value: 'n10', optimistic: false })
  const n11 = makeNode('n11', 0, [], { value: 'n11', optimistic: false })
  const n12 = makeNode('n12', 0, [n3, n7], { value: 'n12', optimistic: false })
  const n13_o = makeNode('n13_o', 0, [n12, n9_o], { value: 'n13_o', optimistic: true })
  const n14 = makeNode('n14', 0, [n6, n0], { value: 'n14', optimistic: false })
  const n15 = makeNode('n15', 0, [n10, n3], { value: 'n15', optimistic: false })
  const n16 = makeNode('n16', 0, [n6, n3, n10], { value: 'n16', optimistic: false })
  const n17_o = makeNode('n17_o', 0, [n3, n11], { value: 'n17_o', optimistic: true })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2)
  ref.add(n3)
  ref.add(n4)
  ref.add(n5_o)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8)
  ref.add(n9_o)
  ref.add(n10)
  ref.add(n11)
  ref.add(n12)
  ref.add(n13_o)
  ref.add(n14)
  ref.add(n15)
  ref.add(n16)
  ref.add(n17_o)

  const bug = new Topolist()
  bug.add(n8)
  bug.add(n6)
  bug.add(n11)
  bug.add(n7)
  bug.add(n0)
  bug.add(n14)
  bug.add(n1)
  bug.add(n2)
  bug.add(n5_o)
  bug.add(n10)
  bug.add(n9_o)
  bug.add(n3)
  bug.add(n4)
  bug.add(n15)
  bug.add(n16)
  bug.add(n17_o)
  bug.add(n12)
  bug.add(n13_o)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test.solo('topolist - optimistic 7', function (t) {
  const n0 = makeNode('n0', 0, [], { value: 'n0', optimistic: false })
  const n1 = makeNode('n1', 0, [n0], { value: 'n1', optimistic: false })
  const n2_o = makeNode('n2_o', 0, [n1], { value: 'n2_o', optimistic: true })
  const n3_o = makeNode('n3_o', 0, [n2_o, n1], { value: 'n3_o', optimistic: true })
  const n4 = makeNode('n4', 0, [], { value: 'n4', optimistic: false })
  const n5_o = makeNode('n5_o', 0, [n4], { value: 'n5_o', optimistic: true })
  const n6 = makeNode('n6', 0, [n5_o], { value: 'n6', optimistic: false })
  const n7 = makeNode('n7', 0, [n0], { value: 'n7', optimistic: false })
  const n8_o = makeNode('n8_o', 0, [n6], { value: 'n8_o', optimistic: true })
  const n9 = makeNode('n9', 0, [n0], { value: 'n9', optimistic: false })
  const n10 = makeNode('n10', 0, [n7], { value: 'n10', optimistic: false })
  const n11 = makeNode('n11', 0, [], { value: 'n11', optimistic: false })
  const n12 = makeNode('n12', 0, [n0], { value: 'n12', optimistic: false })
  const n13 = makeNode('n13', 0, [n2_o], { value: 'n13', optimistic: false })
  const n14_o = makeNode('n14_o', 0, [n1, n7, n2_o], { value: 'n14_o', optimistic: true })
  const n15 = makeNode('n15', 0, [n4, n2_o], { value: 'n15', optimistic: false })
  const n16_o = makeNode('n16_o', 0, [n1], { value: 'n16_o', optimistic: true })
  const n17 = makeNode('n17', 0, [n13, n9, n16_o], { value: 'n17', optimistic: false })
  const n18 = makeNode('n18', 0, [n0], { value: 'n18', optimistic: false })
  const n19 = makeNode('n19', 0, [], { value: 'n19', optimistic: false })
  const n20 = makeNode('n20', 0, [n19, n10, n13], { value: 'n20', optimistic: false })
  const n21 = makeNode('n21', 0, [n1, n18], { value: 'n21', optimistic: false })
  const n22 = makeNode('n22', 0, [n18, n7], { value: 'n22', optimistic: false })
  const n23 = makeNode('n23', 0, [n3_o, n0], { value: 'n23', optimistic: false })
  const n24_o = makeNode('n24_o', 0, [n11, n2_o, n19], { value: 'n24_o', optimistic: true })
  const n25 = makeNode('n25', 0, [], { value: 'n25', optimistic: false })
  const n26 = makeNode('n26', 0, [n17, n9], { value: 'n26', optimistic: false })
  const n27 = makeNode('n27', 0, [n23], { value: 'n27', optimistic: false })
  const n28 = makeNode('n28', 0, [n13, n24_o], { value: 'n28', optimistic: false })
  const n29 = makeNode('n29', 0, [n13], { value: 'n29', optimistic: false })
  const n30 = makeNode('n30', 0, [n2_o], { value: 'n30', optimistic: false })
  const n31 = makeNode('n31', 0, [n0], { value: 'n31', optimistic: false })

  const ref = new Topolist()
  ref.add(n0)
  ref.add(n1)
  ref.add(n2_o)
  ref.add(n3_o)
  ref.add(n4)
  ref.add(n5_o)
  ref.add(n6)
  ref.add(n7)
  ref.add(n8_o)
  ref.add(n9)
  ref.add(n10)
  ref.add(n11)
  ref.add(n12)
  ref.add(n13)
  ref.add(n14_o)
  ref.add(n15)
  ref.add(n16_o)
  ref.add(n17)
  ref.add(n18)
  ref.add(n19)
  ref.add(n20)
  ref.add(n21)
  ref.add(n22)
  ref.add(n23)
  ref.add(n24_o)
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
  bug.add(n5_o)
  bug.add(n6)
  bug.add(n1)
  bug.add(n16_o)
  bug.add(n21)
  bug.add(n2_o)
  bug.add(n3_o)
  bug.add(n23)
  bug.add(n30)
  bug.add(n15)
  bug.add(n11)
  bug.add(n8_o)
  bug.add(n13)
  bug.add(n27)
  bug.add(n17)
  bug.add(n14_o)
  bug.add(n22)
  bug.add(n20)
  bug.add(n26)
  bug.add(n24_o)
  bug.add(n28)
  bug.add(n29)

  t.is(bug.print().join(' '), ref.print().join(' '))
})

test.skip('topolist - fuzz', function (t) {
  const runs = 1e5
  const tests = 1e3

  if (runAll()) t.pass('worked for ' + (runs * tests) + ' runs')

  function runAll () {
    for (let i = 0; i < runs; i++) {
      console.log(i)
      const nodes = []

      const max = Math.random() * 32
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
    const v = 'n' + nodes.length + (optimistic ? '_o' : '')
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
