const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz c175e3', function (t) {
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

    const a0 = l.addHead(a.add())
    const e0 = l.addHead(e.add(a0))
    const a1 = l.addHead(a.add(e0))
    const d0 = l.addHead(d.add(a1))
    const c0 = l.addHead(c.add(d0))
    const a2 = l.addHead(a.add(c0))
    const d1 = l.addHead(d.add(a2))
    const b0 = l.addHead(b.add(d1))
    const e1 = l.addHead(e.add(b0))
    const d2 = l.addHead(d.add(e1))
    const e2 = l.addHead(e.add(d2))
    const c1 = l.addHead(c.add(e2))
    const b1 = l.addHead(b.add(c1))
    const a3 = l.addHead(a.add(e2))
    const c2 = l.addHead(c.add(b1))
    const b2 = l.addHead(b.add(a3, c2))
    const d3 = l.addHead(d.add(c2))
    const c3 = l.addHead(c.add(d3))
    const c4 = l.addHead(c.add(b2, c3))
    const a4 = l.addHead(a.add(c4))
    const b3 = l.addHead(b.add(a4))
    const d4 = l.addHead(d.add(b3))
    const a5 = l.addHead(a.add(d4))
    const e3 = l.addHead(e.add(c3))
    return l
  } else {
    const a = new Writer('a')
    const b = new Writer('b')
    const c = new Writer('c')
    const d = new Writer('d')
    const e = new Writer('e')
    const l = new Linearizer([a, b, c, d, e])

    const a0 = l.addHead(a.add())
    const e0 = l.addHead(e.add(a0))
    const a1 = l.addHead(a.add(e0))
    const d0 = l.addHead(d.add(a1))
    const c0 = l.addHead(c.add(d0))
    const a2 = l.addHead(a.add(c0))
    const d1 = l.addHead(d.add(a2))
    const b0 = l.addHead(b.add(d1))
    const e1 = l.addHead(e.add(b0))
    const d2 = l.addHead(d.add(e1))
    const e2 = l.addHead(e.add(d2))
    const c1 = l.addHead(c.add(e2))
    const b1 = l.addHead(b.add(c1))
    const a3 = l.addHead(a.add(e2))
    const c2 = l.addHead(c.add(b1))
    const b2 = l.addHead(b.add(a3, c2))
    const d3 = l.addHead(d.add(c2))
    const c3 = l.addHead(c.add(d3))
    const c4 = l.addHead(c.add(b2, c3))
    const a4 = l.addHead(a.add(c4))
    const b3 = l.addHead(b.add(a4))
    const d4 = l.addHead(d.add(b3))
    const a5 = l.addHead(a.add(d4))
    l.debugs = true
    const b4 = l.addHead(b.add(a5))
    return l
  }
}
