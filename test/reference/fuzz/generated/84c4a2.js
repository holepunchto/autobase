const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz 84c4a2', function (t) {
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

    const e0 = l.addHead(e.add())
    const c0 = l.addHead(c.add())
    const e1 = l.addHead(e.add(c0, e0))
    const c1 = l.addHead(c.add(e0, c0))
    const c2 = l.addHead(c.add(e1, c1))
    const d0 = l.addHead(d.add(c2))
    const c3 = l.addHead(c.add(d0))
    const d1 = l.addHead(d.add(c3))
    const a0 = l.addHead(a.add(d1))
    const c4 = l.addHead(c.add(a0))
    const e2 = l.addHead(e.add(c4))
    const a1 = l.addHead(a.add(c4))
    const b0 = l.addHead(b.add(e2))
    const c5 = l.addHead(c.add(a1, b0))
    const d2 = l.addHead(d.add(c5))
    const c6 = l.addHead(c.add(d2))
    const d3 = l.addHead(d.add(c6))
    const b1 = l.addHead(b.add(d3))
    const e3 = l.addHead(e.add(b1))
    const d4 = l.addHead(d.add(b1))
    const b2 = l.addHead(b.add(d4))
    const c7 = l.addHead(c.add(b2))
    const a2 = l.addHead(a.add(e3))
    const e4 = l.addHead(e.add(a2))
    const b3 = l.addHead(b.add(e3, b2))
    const c8 = l.addHead(c.add(e3, c7))
    const a3 = l.addHead(a.add(e4, b3, c8))
    const b4 = l.addHead(b.add(a3))
    const d5 = l.addHead(d.add(b4))
    const a4 = l.addHead(a.add(d5))
    const b5 = l.addHead(b.add(a4))
    const c9 = l.addHead(c.add(a4))
    const b6 = l.addHead(b.add(b5, c9))
    const c10 = l.addHead(c.add(b6))
    const b7 = l.addHead(b.add(c10))
    const a5 = l.addHead(a.add(b7))
    const b8 = l.addHead(b.add(a5))
    const c11 = l.addHead(c.add(b8))
    const a6 = l.addHead(a.add(c11))
    const c12 = l.addHead(c.add(a6))
    const a7 = l.addHead(a.add(c12))
    const c13 = l.addHead(c.add(a7))
    const b9 = l.addHead(b.add(c11))
    const b10 = l.addHead(b.add(c13, b9))
    const c14 = l.addHead(c.add(b10))
    const a8 = l.addHead(a.add(c13))
    const a9 = l.addHead(a.add(c14, a8))
    const c15 = l.addHead(c.add(a9))
    const b11 = l.addHead(b.add(a9))
    const b12 = l.addHead(b.add(c15, b11))
    const a10 = l.addHead(a.add(b12))
    const c16 = l.addHead(c.add(b12))
    const b13 = l.addHead(b.add(c16))
    const c17 = l.addHead(c.add(a10, b13))
    const a11 = l.addHead(a.add(c17))
    // console.log(l.print())
    l.debugs = true
    const b14 = l.addHead(b.add(c17))
    return l
  } else {
    const a = new Writer('a')
    const b = new Writer('b')
    const c = new Writer('c')
    const d = new Writer('d')
    const e = new Writer('e')
    const l = new Linearizer([a, b, c, d, e])

    const e0 = l.addHead(e.add())
    const c0 = l.addHead(c.add())
    const e1 = l.addHead(e.add(c0, e0))
    const c1 = l.addHead(c.add(e0, c0))
    const c2 = l.addHead(c.add(e1, c1))
    const d0 = l.addHead(d.add(c2))
    const c3 = l.addHead(c.add(d0))
    const d1 = l.addHead(d.add(c3))
    const a0 = l.addHead(a.add(d1))
    const c4 = l.addHead(c.add(a0))
    const e2 = l.addHead(e.add(c4))
    const a1 = l.addHead(a.add(c4))
    const b0 = l.addHead(b.add(e2))
    const c5 = l.addHead(c.add(a1, b0))
    const d2 = l.addHead(d.add(c5))
    const c6 = l.addHead(c.add(d2))
    const d3 = l.addHead(d.add(c6))
    const b1 = l.addHead(b.add(d3))
    const e3 = l.addHead(e.add(b1))
    const d4 = l.addHead(d.add(b1))
    const b2 = l.addHead(b.add(d4))
    const c7 = l.addHead(c.add(b2))
    const a2 = l.addHead(a.add(e3))
    const e4 = l.addHead(e.add(a2))
    const b3 = l.addHead(b.add(e3, b2))
    const c8 = l.addHead(c.add(e3, c7))
    const a3 = l.addHead(a.add(e4, b3, c8))
    const b4 = l.addHead(b.add(a3))
    const d5 = l.addHead(d.add(b4))
    const a4 = l.addHead(a.add(d5))
    const b5 = l.addHead(b.add(a4))
    const c9 = l.addHead(c.add(a4))
    const b6 = l.addHead(b.add(b5, c9))
    const c10 = l.addHead(c.add(b6))
    const b7 = l.addHead(b.add(c10))
    const a5 = l.addHead(a.add(b7))
    const b8 = l.addHead(b.add(a5))
    const c11 = l.addHead(c.add(b8))
    const a6 = l.addHead(a.add(c11))
    const c12 = l.addHead(c.add(a6))
    const a7 = l.addHead(a.add(c12))
    const c13 = l.addHead(c.add(a7))
    const b9 = l.addHead(b.add(c11))
    const b10 = l.addHead(b.add(c13, b9))
    const c14 = l.addHead(c.add(b10))
    const a8 = l.addHead(a.add(c13))
    const a9 = l.addHead(a.add(c14, a8))
    const c15 = l.addHead(c.add(a9))
    const b11 = l.addHead(b.add(a9))
    const b12 = l.addHead(b.add(c15, b11))
    const a10 = l.addHead(a.add(b12))
    const c16 = l.addHead(c.add(b12))
    const b13 = l.addHead(b.add(c16))
    const c17 = l.addHead(c.add(a10, b13))
    // console.log(l.print())
    // l.debugs= true
    const b14 = l.addHead(b.add(c17))
    return l
  }
}
