const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz a3945f', function (t) {
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
    const d0 = l.addHead(d.add(b0))
    const a1 = l.addHead(a.add(a0, d0))
    const b1 = l.addHead(b.add(a0, b0))
    const e0 = l.addHead(e.add())
    const e1 = l.addHead(e.add(a1, b1, e0))
    const a2 = l.addHead(a.add(e1))
    const b2 = l.addHead(b.add(a2))
    const d1 = l.addHead(d.add(b2))
    const c0 = l.addHead(c.add(a0))
    l.debug=1
    const e2 = l.addHead(e.add(d1))
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
    const d0 = l.addHead(d.add(b0))
    const a1 = l.addHead(a.add(a0, d0))
    const b1 = l.addHead(b.add(a0, b0))
    const e0 = l.addHead(e.add())
    const e1 = l.addHead(e.add(a1, b1, e0))
    const a2 = l.addHead(a.add(e1))
    const b2 = l.addHead(b.add(a2))
    const d1 = l.addHead(d.add(b2))
    const a3 = l.addHead(a.add(d1))
    return l
  }
}
