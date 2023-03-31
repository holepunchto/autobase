const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz c8db58', function (t) {
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

    const b0 = l.addHead(b.add())
    const a0 = l.addHead(a.add())
    const e0 = l.addHead(e.add(b0))
    const d0 = l.addHead(d.add(b0, a0))
    const c0 = l.addHead(c.add(b0))
    const c1 = l.addHead(c.add(e0, d0, c0))
    const d1 = l.addHead(d.add(c1))
    const a1 = l.addHead(a.add(c1))
    const b1 = l.addHead(b.add(d1, a1))
    const e1 = l.addHead(e.add(b1))
    const d2 = l.addHead(d.add(b1))
    const c2 = l.addHead(c.add(e1))
    const d3 = l.addHead(d.add(d2, c2))
    const b2 = l.addHead(b.add(d3))
    const d4 = l.addHead(d.add(b2))
    const e2 = l.addHead(e.add(d4))
    const c3 = l.addHead(c.add(e2))
    const b3 = l.addHead(b.add(c3))
    const e3 = l.addHead(e.add(b3))
    const a2 = l.addHead(a.add(e1))
    const c4 = l.addHead(c.add(e3))
    const a3 = l.addHead(a.add(a2, c4))
    const c5 = l.addHead(c.add(a3))
    const b4 = l.addHead(b.add(c5))
    const d5 = l.addHead(d.add(b4))
    const a4 = l.addHead(a.add(b4))
    const e4 = l.addHead(e.add(d5))
    const c6 = l.addHead(c.add(e4))
    const a5 = l.addHead(a.add(c6, a4))
    const b5 = l.addHead(b.add(a5))
    const c7 = l.addHead(c.add(b5))
    const b6 = l.addHead(b.add(c7))
    const c8 = l.addHead(c.add(b6))
    const b7 = l.addHead(b.add(c8))
    const a6 = l.addHead(a.add(b7))
    return l
  } else {
    const a = new Writer('a')
    const b = new Writer('b')
    const c = new Writer('c')
    const d = new Writer('d')
    const e = new Writer('e')
    const l = new Linearizer([a, b, c, d, e])

    const b0 = l.addHead(b.add())
    const a0 = l.addHead(a.add())
    const e0 = l.addHead(e.add(b0))
    const d0 = l.addHead(d.add(b0, a0))
    const c0 = l.addHead(c.add(b0))
    const c1 = l.addHead(c.add(e0, d0, c0))
    const d1 = l.addHead(d.add(c1))
    const a1 = l.addHead(a.add(c1))
    const b1 = l.addHead(b.add(d1, a1))
    const e1 = l.addHead(e.add(b1))
    const d2 = l.addHead(d.add(b1))
    const c2 = l.addHead(c.add(e1))
    const d3 = l.addHead(d.add(d2, c2))
    const b2 = l.addHead(b.add(d3))
    const d4 = l.addHead(d.add(b2))
    const e2 = l.addHead(e.add(d4))
    const c3 = l.addHead(c.add(e2))
    const b3 = l.addHead(b.add(c3))
    const e3 = l.addHead(e.add(b3))
    const a2 = l.addHead(a.add(e1))
    const c4 = l.addHead(c.add(e3))
    const a3 = l.addHead(a.add(a2, c4))
    const c5 = l.addHead(c.add(a3))
    const b4 = l.addHead(b.add(c5))
    const d5 = l.addHead(d.add(b4))
    const e4 = l.addHead(e.add(d5))
    return l
  }
}
