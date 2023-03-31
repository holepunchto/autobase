const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz 0a3513', function (t) {
  const l1 = makeGraph(true)
  const l2 = makeGraph(false)

  let n = 0

  while (true) {
    if (n === 26) {
      console.log(l1.print())
      console.log(l2.print())
    }
    const n1 = l1.shift()
    const n2 = l2.shift()

    if (!n1 || !n2) break

    const ref1 = n1 ? n1.writer.key + n1.seq : null
    const ref2 = n2 ? n2.writer.key + n2.seq : null
    const tick = n++

    if (n1 && n2) {
      t.is(ref1, ref2, 'yield both #' + tick + ', ' + ref2)
      if (ref1 !== ref2) break
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
    const l = new Linearizer([a, b, c])

    const b0 = l.addHead(b.add())
    const a0 = l.addHead(a.add())
    const b1 = l.addHead(b.add(b0, a0))
    const a1 = l.addHead(a.add(b1))
    const b2 = l.addHead(b.add(a1))
    const c0 = l.addHead(c.add(b2))
    const a2 = l.addHead(a.add(c0))
    const b3 = l.addHead(b.add(a2))
    const c1 = l.addHead(c.add(b3))
    const a3 = l.addHead(a.add(c1))
    const b4 = l.addHead(b.add(a3))
    const a4 = l.addHead(a.add(b4))
    const c2 = l.addHead(c.add(a4))
    const a5 = l.addHead(a.add(c2))
    const c3 = l.addHead(c.add(a5))
    const b5 = l.addHead(b.add(c3))
    const a6 = l.addHead(a.add(c3))
    const c4 = l.addHead(c.add(a6))
    const a7 = l.addHead(a.add(b5, a6))
    const b6 = l.addHead(b.add(a7))
    const a8 = l.addHead(a.add(b6))
    const b7 = l.addHead(b.add(c4, b6))
    const a9 = l.addHead(a.add(a8, b7))
    const b8 = l.addHead(b.add(a9))
    const a10 = l.addHead(a.add(b8))
    const b9 = l.addHead(b.add(a10))
    const c5 = l.addHead(c.add(b8))
    const a11 = l.addHead(a.add(c5, a10))
    const c6 = l.addHead(c.add(b9, c5))
    const b10 = l.addHead(b.add(b9, a11))
    const a12 = l.addHead(a.add(b10))
    const b11 = l.addHead(b.add(a12))
    return l
  } else {
    const a = new Writer('a')
    const b = new Writer('b')
    const c = new Writer('c')
    const l = new Linearizer([a, b, c])

    const b0 = l.addHead(b.add())
    const a0 = l.addHead(a.add())
    const b1 = l.addHead(b.add(b0, a0))
    const a1 = l.addHead(a.add(b1))
    const b2 = l.addHead(b.add(a1))
    const c0 = l.addHead(c.add(b2))
    const a2 = l.addHead(a.add(c0))
    const b3 = l.addHead(b.add(a2))
    const c1 = l.addHead(c.add(b3))
    const a3 = l.addHead(a.add(c1))
    const b4 = l.addHead(b.add(a3))
    const a4 = l.addHead(a.add(b4))
    const c2 = l.addHead(c.add(a4))
    const a5 = l.addHead(a.add(c2))
    const c3 = l.addHead(c.add(a5))
    const b5 = l.addHead(b.add(c3))
    const a6 = l.addHead(a.add(c3))
    const c4 = l.addHead(c.add(a6))
    const a7 = l.addHead(a.add(b5, a6))
    const b6 = l.addHead(b.add(a7))
    const a8 = l.addHead(a.add(b6))
    const b7 = l.addHead(b.add(c4, b6))
    const a9 = l.addHead(a.add(a8, b7))
    const b8 = l.addHead(b.add(a9))
    const a10 = l.addHead(a.add(b8))
    const b9 = l.addHead(b.add(a10))
    const c5 = l.addHead(c.add(b8))
    const a11 = l.addHead(a.add(c5, a10))
    const b10 = l.addHead(b.add(b9, a11))
    const a12 = l.addHead(a.add(b10))
    const b11 = l.addHead(b.add(a12))
    return l
  }
}
