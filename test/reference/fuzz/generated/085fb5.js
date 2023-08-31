const test = require('brittle')
const { Linearizer, Writer } = require('../../')

test('fuzz 085fb5', function (t) {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')
  const l = new Linearizer([a, b, c, d, e])


  const nodes = []

    const b0 = b.add()
    nodes.push(b0)
    const d0 = d.add(b0)
    nodes.push(d0)
    const e0 = e.add(d0)
    nodes.push(e0)
    const b1 = b.add(e0)
    nodes.push(b1)
    const d1 = d.add(b1)
    nodes.push(d1)
    const e1 = e.add(d1)
    nodes.push(e1)
    const b2 = b.add(e1)
    nodes.push(b2)

  while (nodes.length) {
    while (nodes.length) {
      l.addHead(nodes.shift())
    }
console.log(l.print())
console.log(l.shifted)
    const result = []
    let tick = 0

    while (true) {
      const n = l.shift()
      console.log('-->', n?.ref)
      if (!n) break
      t.comment('yield #' + ++tick + ', ' + n.ref)
    }

    // t.not(length, result.length, 'loop ' + i)
  }
})
