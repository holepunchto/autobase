const test = require('brittle')
const { Writer, Linearizer } = require('./')
const fixtures = require('./fixtures.json')

test('convergence', t => {
  const a = new Writer('a')
  const b = new Writer('b')

  const l = new Linearizer([a, b])

  const a0 = l.addHead(a.add())
  const b0 = l.addHead(b.add(a0))
  const a1 = l.addHead(a.add(b0))

  t.ok(l._isConfirmed(a0))

  const result = []
  while (true) {
    const node = l.shift()
    if (!node) break
    result.push(node)
  }

  t.alike(result, [a0])
  t.alike([...l.tails], [b0])
})

test('preferred fork', t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')

  const l = new Linearizer([a, b, c])

  const a0 = l.addHead(a.add())
  const b0 = l.addHead(b.add())
  const c0 = l.addHead(c.add(b0))
  const a1 = l.addHead(a.add(a0, c0))
  const c1 = l.addHead(c.add(a1))
  const a2 = l.addHead(a.add(c1))

  t.ok(l._isConfirmed(a1))

  const result = []
  while (true) {
    const node = l.shift()
    if (!node) break
    result.push(node)
  }

  t.alike(result, [b0, c0, a0, a1])
  t.alike([...l.tails], [c1])
})

test('nested merges', t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const l = new Linearizer([a, b, c, d, e])

  const b0 = l.addHead(b.add())
  const a0 = l.addHead(a.add())
  const e0 = l.addHead(e.add())

  const d0 = l.addHead(d.add(b0))
  const c0 = l.addHead(c.add(d0))

  t.ok(l._confirms(c0, b0))

  const b1 = l.addHead(b.add(a0, b0))
  const d1 = l.addHead(d.add(b1, d0))

  const d2 = l.addHead(d.add(d1, e0))
  const e1 = l.addHead(e.add(d2))
  const a1 = l.addHead(a.add(e1))
  const e2 = l.addHead(e.add(a1))
  const d3 = l.addHead(d.add(e2))

  t.ok(l._isConfirmed(d2))

  const result = []
  while (true) {
    const node = l.shift()
    if (!node) break
    result.push(node)
  }

  t.alike(result, [a0, b0, b1, d0, d1, e0, d2])
})

test('preferred shorter fork', t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')

  const l = new Linearizer([a, b, c])

  const a0 = l.addHead(a.add())
  const a1 = l.addHead(a.add(a0))
  const a2 = l.addHead(a.add(a1))
  const a3 = l.addHead(a.add(a2))
  const b0 = l.addHead(b.add())
  const c0 = l.addHead(c.add(b0))
  const a4 = l.addHead(a.add(a3, c0))
  const c1 = l.addHead(c.add(a4))
  const a5 = l.addHead(a.add(c1))

  // confirm
  t.ok(l._isConfirmed(a4))

  const result = []
  while (true) {
    const node = l.shift()
    if (!node) break
    result.push(node)
  }

  t.alike(result, [b0, c0, a0, a1, a2, a3, a4])
  t.alike([...l.tails], [c1])
})

test('fork and merge', t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const l = new Linearizer([a, b, c, d, e])

  const a0 = l.addHead(a.add())
  const b0 = l.addHead(b.add())
  const c0 = l.addHead(c.add(b0))
  const c1 = l.addHead(c.add(a0, c0))

  const d0 = l.addHead(d.add(a0))
  const e0 = l.addHead(e.add(d0, c1))

  const d1 = l.addHead(d.add(e0))
  const c2 = l.addHead(c.add(d1))
  const e1 = l.addHead(e.add(c2))
  const d2 = l.addHead(d.add(e1))

  t.ok(l._isConfirmed(e0))

  const result = []
  while (true) {
    const node = l.shift()
    if (!node) break
    result.push(node)
  }

  t.alike(result, [a0, b0, c0, c1, d0, e0])
  t.alike([...l.tails], [d1])
})

/*
d - e - c - d
             \
      b - a - d - a - b - d - a
*/

test('graph 1', t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const linearizer = new Linearizer([a, b, c, d, e])

  const d0 = linearizer.addHead(d.add())
  const e0 = linearizer.addHead(e.add(d0))
  const c0 = linearizer.addHead(c.add(e0))
  const d1 = linearizer.addHead(d.add(c0))

  const b0 = linearizer.addHead(b.add())
  const a0 = linearizer.addHead(a.add(b0))
  const d2 = linearizer.addHead(d.add(d1, a0))
  const a1 = linearizer.addHead(a.add(d2))
  const b1 = linearizer.addHead(b.add(a1))
  const d3 = linearizer.addHead(d.add(b1))
  const a2 = linearizer.addHead(a.add(d3))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*
a - b - c - a - b
         \
  e - d - c - e - d - c - e
*/

test('graph 2', t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const linearizer = new Linearizer([a, b, c, d, e])

  const a0 = linearizer.addHead(a.add())
  const b0 = linearizer.addHead(b.add(a0))
  const c0 = linearizer.addHead(c.add(b0))
  const a1 = linearizer.addHead(a.add(c0))
  const b1 = linearizer.addHead(b.add(a1))

  const e0 = linearizer.addHead(e.add())
  const d0 = linearizer.addHead(d.add(e0))
  const c1 = linearizer.addHead(c.add(d0, c0))
  const e1 = linearizer.addHead(e.add(c1))
  const d1 = linearizer.addHead(d.add(e1))
  const c2 = linearizer.addHead(c.add(d1))
  const e2 = linearizer.addHead(e.add(d1))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*

c - b - a - c - b - a

*/

test('simple 3', async t => {
  const a = new Writer('a', 2)
  const b = new Writer('b', 2)
  const c = new Writer('c', 2)

  const linearizer = new Linearizer([a, b, c])

  const c0 = linearizer.addHead(c.add())
  const b0 = linearizer.addHead(b.add(c0))
  const a0 = linearizer.addHead(a.add(b0))
  const c1 = linearizer.addHead(c.add(a0))
  const b1 = linearizer.addHead(b.add(c1))
  const a1 = linearizer.addHead(a.add(b1))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*

a   b
| / |
b   c
| / |
c   a
| / |
a   b
| / |
b   c
| / |
c   a

*/

test('non-convergence', async t => {
  const a = new Writer('a', 2)
  const b = new Writer('b', 2)
  const c = new Writer('c', 2)

  const linearizer = new Linearizer([a, b, c])

  const a0 = linearizer.addHead(a.add())
  const b0 = linearizer.addHead(b.add())
  const b1 = linearizer.addHead(b.add(b0, a0))
  const c0 = linearizer.addHead(c.add(b0))
  const c1 = linearizer.addHead(c.add(c0, b1))
  const a1 = linearizer.addHead(a.add(a0, c0))
  const a2 = linearizer.addHead(a.add(a1, c1))
  const b2 = linearizer.addHead(b.add(a1, b1))
  const b3 = linearizer.addHead(b.add(a2, b2))
  const c2 = linearizer.addHead(c.add(c1, b2))
  const c3 = linearizer.addHead(c.add(c2, b3))
  const a3 = linearizer.addHead(a.add(a2, c2))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*

    b   c   d
  / | x | x | \
 a  b   c   d  e
  \ | x | x | /
    b   c   d
    | /
    b

*/

test('inner majority', async t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const linearizer = new Linearizer([a, b, c, d, e])

  const b0 = linearizer.addHead(b.add())
  const c0 = linearizer.addHead(c.add())
  const d0 = linearizer.addHead(d.add())
  const a0 = linearizer.addHead(a.add(b0))
  const e0 = linearizer.addHead(e.add(d0))

  const b1 = linearizer.addHead(b.add(b0, c0))
  const c1 = linearizer.addHead(c.add(b0, c0, d0))
  const d1 = linearizer.addHead(d.add(c0, d0))

  const b2 = linearizer.addHead(b.add(a0, b1, c1))
  const c2 = linearizer.addHead(c.add(b1, c1, d1))
  const d2 = linearizer.addHead(d.add(c1, d1, e0))

  const b3 = linearizer.addHead(b.add(b2, c2))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*

  b - c - d - b - c - d

*/

test('majority alone - convergence', async t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const linearizer = new Linearizer([a, b, c, d, e])

  const b0 = linearizer.addHead(b.add())
  const c0 = linearizer.addHead(c.add(b0))
  const d0 = linearizer.addHead(d.add(c0))
  const b1 = linearizer.addHead(b.add(d0))
  const c1 = linearizer.addHead(c.add(b1))
  const d1 = linearizer.addHead(d.add(c1))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*

  b   c   d
  | x | x |
  b   c   d
  | x | x |
  b   c   d
  | /
  b

*/

test('majority alone - non-convergence', async t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const linearizer = new Linearizer([b, c, d])

  const b0 = linearizer.addHead(b.add())
  const c0 = linearizer.addHead(c.add())
  const d0 = linearizer.addHead(d.add())

  const b1 = linearizer.addHead(b.add(b0, c0))
  const c1 = linearizer.addHead(c.add(b0, c0, d0))
  const d1 = linearizer.addHead(d.add(c0, d0))

  const b2 = linearizer.addHead(b.add(b1, c1))
  const c2 = linearizer.addHead(c.add(b1, c1, d1))
  const d2 = linearizer.addHead(d.add(c1, d1))

  const b3 = linearizer.addHead(b.add(b2, c2))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*

    a0  e0
    |   |
    b0  d0
    |   |
    c0  |
    |   |
    a1  |
    |   |
    b1  |
    | \ |
    c1  b2
    |   |
    a2  d1
    | / |
    d2  e2
    |   |
    a3  b3
*/

test('double fork', async t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const linearizer = new Linearizer([a, b, c, d, e])

  const a0 = linearizer.addHead(a.add())
  const e0 = linearizer.addHead(e.add())
  const b0 = linearizer.addHead(b.add(a0))
  const d0 = linearizer.addHead(d.add(e0))
  const c0 = linearizer.addHead(c.add(b0))
  const a1 = linearizer.addHead(a.add(c0))
  const b1 = linearizer.addHead(b.add(a1))
  const c1 = linearizer.addHead(c.add(b1))
  const b2 = linearizer.addHead(b.add(b1, d0))
  const a2 = linearizer.addHead(a.add(c1))
  const d1 = linearizer.addHead(d.add(b2))
  const d2 = linearizer.addHead(d.add(a2, d1))
  const e2 = linearizer.addHead(e.add(d1))
  const a3 = linearizer.addHead(a.add(d2))
  const b3 = linearizer.addHead(b.add(e2))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

/*
  a   b
  | - | \
  a   b  e (points to a and b tail)
  | /    |
  b      d
  |      |
  a      e
  |      |
  c ---< c
  |
  a
  |
  b
*/

test('recompute after yield', async t => {
  const a = new Writer('a')
  const b = new Writer('b')
  const c = new Writer('c')
  const d = new Writer('d')
  const e = new Writer('e')

  const linearizer = new Linearizer([a, b, c, d, e])

  const a0 = linearizer.addHead(a.add())
  const b0 = linearizer.addHead(b.add())
  const a1 = linearizer.addHead(a.add(a0))
  const b1 = linearizer.addHead(b.add(b0))
  const b2 = linearizer.addHead(b.add(b1, a1))
  const a2 = linearizer.addHead(b.add(b2))

  const e0 = linearizer.addHead(e.add(a0, b0))
  const d0 = linearizer.addHead(d.add(e0))
  const e1 = linearizer.addHead(e.add(d0))
  const c0 = linearizer.addHead(c.add(e1))
  const c1 = linearizer.addHead(c.add(a2, c0))
  const a3 = linearizer.addHead(a.add(c1))
  const b3 = linearizer.addHead(b.add(a3))

  const graph = []
  while (true) {
    const node = linearizer.shift()
    if (!node) break
    graph.push(node)
  }

  if (!compare(fixtures.shift(), graph)) return t.fail()
  t.pass()
})

function printGraph (head) {
  let str = '```mermaid'
  str += 'graph TD;'
  for (const node of Node.visit(head.heads())) {
    for (const dep of node.dependencies) {
      str += '    ' + node.key + '' + node.seq + '-->' + dep.key + '' + dep.seq + ';'
    }
  }
  str += '```'
  return str
}

function traverseGraph (head) {
  const nodes = []
  for (const node of Node.visit(head.heads())) {
    if (node.isYieldable()) nodes.push(node.yield())
  }
  return nodes
}

function compare (fixtures, graph) {
  for (let i = 0; i < Math.min(fixtures.length, graph.length); i++) {
    if (!compareNode(fixtures[i], graph[i])) return false
  }

  return fixtures.length === graph.length
}

function compareNode (fixture, node) {
  const [key, seq, maj] = fixture
  return node.writer.key === key && node.seq === seq
}
