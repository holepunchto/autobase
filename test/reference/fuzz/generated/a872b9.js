const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz a872b9', function (t) {
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
    const c0 = l.addHead(c.add(b0))
    const a1 = l.addHead(a.add(b0, a0, c0))
    const e0 = l.addHead(e.add(c0))
    const c1 = l.addHead(c.add(a1, e0))
    const b1 = l.addHead(b.add(c1))
    const c2 = l.addHead(c.add(b1))
    c2.debug = true
    console.log(c2.dependencies.length, '<---')
    console.log(b1.dependents, '<-- b1 deps')
    const b2 = l.addHead(b.add(c2))
    const d0 = l.addHead(d.add(b2))
    const c3 = l.addHead(c.add(d0))
    const e1 = l.addHead(e.add(c3))
    const a2 = l.addHead(a.add(b1))
    const e2 = l.addHead(e.add(a2, e1))
    const d1 = l.addHead(d.add(e2))
    const a3 = l.addHead(a.add(e2))
    const c4 = l.addHead(c.add(d1))
    const a4 = l.addHead(a.add(d1, a3, c4))
    const d2 = l.addHead(d.add(c4))
    const a5 = l.addHead(a.add(d2, a4))
    const c5 = l.addHead(c.add(a5))

    // while (l.shift());
    console.log(c2.dependencies.length, '<---')
    console.log(b2)
    // console.log(e1.dependencies)
    // console.log(l.shifted)
    // const d3 = l.addHead(d.add(c5))
    console.log(l.print())
    console.log(l.tails)
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
    const c0 = l.addHead(c.add(b0))
    const a1 = l.addHead(a.add(b0, a0, c0))
    const e0 = l.addHead(e.add(c0))
    const c1 = l.addHead(c.add(a1, e0))
    const b1 = l.addHead(b.add(c1))
    const c2 = l.addHead(c.add(b1))
    const b2 = l.addHead(b.add(c2))
    const d0 = l.addHead(d.add(b2))
    const c3 = l.addHead(c.add(d0))
    const e1 = l.addHead(e.add(c3))
    const a2 = l.addHead(a.add(b1))
    const e2 = l.addHead(e.add(a2, e1))
    const d1 = l.addHead(d.add(e2))
    const a3 = l.addHead(a.add(e2))
    const c4 = l.addHead(c.add(d1))
    const a4 = l.addHead(a.add(d1, a3, c4))
    const d2 = l.addHead(d.add(c4))
    const a5 = l.addHead(a.add(d2, a4))
    const c5 = l.addHead(c.add(a5))
    const b3 = l.addHead(b.add(e1))
    return l
  }
}
