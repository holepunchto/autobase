const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz ba606a', function (t) {
  const l1 = makeGraph(true)
  const l2 = makeGraph(false)

  let n = 0

  while (true) {
    const p1 = l1.print()
    const p2 = l2.print()

    const n1 = l1.shift()
    const n2 = l2.shift()

    if (!n1 || !n2) break

    const ref1 = n1 ? n1.writer.key + n1.seq : null
    const ref2 = n2 ? n2.writer.key + n2.seq : null
    const tick = n++

    if (n1 && n2) {
      t.is(ref1, ref2, 'yield both #' + tick + ', ' + ref2)
      if (ref1 !== ref2) {
        console.log(p1)
        console.log(p2)
        break
      }
    } else if (n1) {
      t.comment('yield left #' + tick + ', ' + ref1)
    } else {
      t.comment('yield right #' + tick + ', ' + ref2)
    }
  }
})
function makeGraph (left) {
  if (left) {
    const a = new Writer('a')
    const b = new Writer('b')
    const c = new Writer('c')
    const d = new Writer('d')
    const e = new Writer('e')
    const l = new Linearizer([a, b, c, d, e])

    const c0 = l.addHead(c.add())
    const e0 = l.addHead(e.add())
    const d0 = l.addHead(d.add(e0))
    const b0 = l.addHead(b.add(c0))
    const c1 = l.addHead(c.add(b0))
    const a0 = l.addHead(a.add(d0))
    const b1 = l.addHead(b.add(e0, c1, a0))
    const e1 = l.addHead(e.add(e0, c1))
    const d1 = l.addHead(d.add(e1, d0))
    const e2 = l.addHead(e.add(b1, d1))
    const b2 = l.addHead(b.add(e2))
    const a1 = l.addHead(a.add(e2))
    const d2 = l.addHead(d.add(b2, a1))
    const e3 = l.addHead(e.add(b2))
    const a2 = l.addHead(a.add(d2, e3))
    const c2 = l.addHead(c.add(a2))
    const e4 = l.addHead(e.add(a1, e3))
    const b3 = l.addHead(b.add(e4))
    const a3 = l.addHead(a.add(b3, a2))
    const b4 = l.addHead(b.add(c2, a3))
    const a4 = l.addHead(a.add(b4))
    const e5 = l.addHead(e.add(a4))
    const c3 = l.addHead(c.add(e5))
    const a5 = l.addHead(a.add(e5))
    const b5 = l.addHead(b.add(c3, a5))
    const d3 = l.addHead(d.add(c3))
    const b6 = l.addHead(b.add(b5, d3))
    const d4 = l.addHead(d.add(b6))
    const c4 = l.addHead(c.add(d4))
    const e6 = l.addHead(e.add(c4))
    const a6 = l.addHead(a.add(e6))
    return l
  } else {
    const a = new Writer('a')
    const b = new Writer('b')
    const c = new Writer('c')
    const d = new Writer('d')
    const e = new Writer('e')
    const l = new Linearizer([a, b, c, d, e])

    const c0 = l.addHead(c.add())
    const e0 = l.addHead(e.add())
    const d0 = l.addHead(d.add(e0))
    const b0 = l.addHead(b.add(c0))
    const c1 = l.addHead(c.add(b0))
    const a0 = l.addHead(a.add(d0))
    const b1 = l.addHead(b.add(e0, c1, a0))
    const e1 = l.addHead(e.add(e0, c1))
    const d1 = l.addHead(d.add(e1, d0))
    const e2 = l.addHead(e.add(b1, d1))
    const b2 = l.addHead(b.add(e2))
    const a1 = l.addHead(a.add(e2))
    const d2 = l.addHead(d.add(b2, a1))
    const e3 = l.addHead(e.add(b2))
    const a2 = l.addHead(a.add(d2, e3))
    const c2 = l.addHead(c.add(a2))
    const e4 = l.addHead(e.add(a1, e3))
    const b3 = l.addHead(b.add(e4))
    const a3 = l.addHead(a.add(b3, a2))
    const b4 = l.addHead(b.add(c2, a3))
    const a4 = l.addHead(a.add(b4))
    const e5 = l.addHead(e.add(a4))
    const c3 = l.addHead(c.add(e5))
    const d3 = l.addHead(d.add(c3))
    return l
  }
}
