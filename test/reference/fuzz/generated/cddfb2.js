const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz cddfb2', function (t) {
  const l1 = makeGraph(true)
  const l2 = makeGraph(false)

  let n = 0

  while (true) {
    // if (n === 0) {
    //   console.log(l1.print())
    //   console.log(l2.print())
    // }
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
    const c0 = l.addHead(c.add())
    const b1 = l.addHead(b.add(c0, b0))
    const a0 = l.addHead(a.add(b0))
    const b2 = l.addHead(b.add(b1, a0))
    const a1 = l.addHead(a.add(b2))
    const c1 = l.addHead(c.add(a0, c0))
    const a2 = l.addHead(a.add(c1, a1))
    const c2 = l.addHead(c.add(a2))
    const b3 = l.addHead(b.add(a2))
    const a3 = l.addHead(a.add(c2))
    return l
  } else {
    const a = new Writer('a')
    const b = new Writer('b')
    const c = new Writer('c')
    const l = new Linearizer([a, b, c])

    const b0 = l.addHead(b.add())
    const c0 = l.addHead(c.add())
    const b1 = l.addHead(b.add(c0, b0))
    const a0 = l.addHead(a.add(b0))
    const b2 = l.addHead(b.add(b1, a0))
    const a1 = l.addHead(a.add(b2))
    const c1 = l.addHead(c.add(a0, c0))
    const a2 = l.addHead(a.add(c1, a1))
    const c2 = l.addHead(c.add(a2))
    const b3 = l.addHead(b.add(a2))
    const b4 = l.addHead(b.add(c2, b3))
    const c3 = l.addHead(c.add(b4))
    const b5 = l.addHead(b.add(c3))
    return l
  }
}