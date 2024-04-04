const test = require('brittle')
const b4a = require('b4a')
const Topolist = require('../lib/topolist')

test('topolist - stable ordering', function (t) {
  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [a0])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(c1)
    tip.add(b0)

    t.alike(tip.tip, [a0, b0, c0, c1])
  }

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(b0)
    tip.add(c1)

    t.alike(tip.tip, [a0, b0, c0, c1])
  }

  {
    const tip = new Topolist()

    tip.add(a0)
    tip.add(c0)
    tip.add(c1)
    tip.add(b0)

    t.alike(tip.tip, [a0, b0, c0, c1])
  }
})

test('topolist - can track changes', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)

  tip.mark()

  const b0 = makeNode('b', 0, [a0])
  tip.add(b0)

  t.is(tip.undo, 2)
  t.is(tip.shared, 1)

  const b1 = makeNode('b', 1, [c1])

  tip.add(b1)

  t.is(tip.undo, 2)
  t.is(tip.shared, 1)

  tip.mark()

  const d0 = makeNode('d', 0, [c1])

  tip.add(d0)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  const c2 = makeNode('c', 2, [c1])

  tip.add(c2)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  const u = tip.flush()
  t.is(u.shared, 5)
  t.is(u.undo, 0)
  t.is(u.length, 7)
})

test('topolist - can shift', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  a0.yielded = true

  const u = tip.flush([a0])

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 1)
  t.is(u.length, 7)
})

test('topolist - can shift out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  c0.yielded = true

  const u = tip.flush([c0])

  t.is(u.undo, 5)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 1)
  t.is(u.tip.length, 6)
  t.is(u.length, 7)
})

test('topolist - can multiple shift', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  a0.yielded = true
  b0.yielded = true

  const u = tip.flush([a0, b0])

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 2)
  t.is(u.length, 7)
})

test('topolist - can multiple shift partially out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [a0])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  a0.yielded = true
  c0.yielded = true

  const u = tip.flush([a0, c0])

  t.is(u.undo, 4)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 2)
  t.is(u.length, 7)
})

test('topolist - can multiple shift out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [])
  const c1 = makeNode('c', 1, [])
  const b0 = makeNode('b', 0, [c0])
  const b1 = makeNode('b', 1, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  const d0 = makeNode('d', 0, [c1])

  tip.add(d0)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  const c2 = makeNode('c', 2, [c1])

  tip.add(c2)

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  c0.yielded = true
  b0.yielded = true

  const u = tip.flush([c0, b0])

  t.is(u.undo, 5)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 2)
  t.is(u.length, 7)
})

test('topolist - can shift multiple times', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const b0 = makeNode('b', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const u = tip.flush([a0])

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 1)

  b0.yielded = true

  const u2 = tip.flush([b0])

  t.is(u2.undo, 0)
  t.is(u2.shared, 6)
  t.is(u2.indexed.length, 1)
  t.is(u.length, 7)
})

test('topolist - shift whole chain', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const b0 = makeNode('b', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const yielded = [a0, b0, c0, c1, b1, c2, d0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 0)
  t.is(u.shared, 5)
  t.is(u.indexed.length, 7)
  t.is(u.length, 7)
})

test('topolist - shift whole chain out of order', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const b0 = makeNode('b', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b0)
  tip.add(b1)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(d0)
  tip.add(c2)

  const yielded = [a0, c0, b0, c1, b1, c2, d0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 4)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 7)
  t.is(u.length, 7)
})

test('topolist - reorder then shift', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const c0 = makeNode('c', 0, [a0])
  const b0 = makeNode('b', 0, [a0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(a0)
  tip.add(c0)
  tip.add(c1)
  tip.add(d0)
  tip.add(c2)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 5)

  tip.add(b0)
  tip.add(b1)

  t.is(tip.undo, 4)
  t.is(tip.shared, 1)

  const yielded = [a0, b0, c0, c1, b1, c2, d0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 4)
  t.is(u.shared, 1)
  t.is(u.indexed.length, 7)
  t.is(u.length, 7)
})

test('topolist - add reorder then shift', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [])
  const c0 = makeNode('c', 0, [b0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(b0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b1)
  tip.add(d0)
  tip.add(c2)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 6)

  tip.add(a0)

  t.is(tip.undo, 6)
  t.is(tip.shared, 0)

  const yielded = [a0]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 6)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 1)
  t.is(u.length, 7)
})

test('topolist - reorder then shift reordered', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [])
  const c0 = makeNode('c', 0, [b0])
  const c1 = makeNode('c', 1, [])
  const b1 = makeNode('b', 1, [c1])
  const d0 = makeNode('d', 0, [c1])
  const c2 = makeNode('c', 2, [c1])

  tip.add(b0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b1)
  tip.add(d0)
  tip.add(c2)

  tip.mark()

  t.is(tip.undo, 0)
  t.is(tip.shared, 6)

  tip.add(a0)

  t.is(tip.undo, 6)
  t.is(tip.shared, 0)

  const yielded = [a0, b0, c0, c1, c2, b1]
  for (const n of yielded) n.yielded = true

  const u = tip.flush(yielded)

  t.is(u.undo, 6)
  t.is(u.shared, 0)
  t.is(u.indexed.length, 6)
  t.is(u.length, 7)
})

test('topolist - with versions', function (t) {
  const tip = new Topolist()

  const a0 = makeNode('a', 0, [])
  const b0 = makeNode('b', 0, [])
  const c0 = makeNode('c', 0, [b0])
  const c1 = makeNode('c', 1, [], { version: 1 })
  const b1 = makeNode('b', 1, [c0])
  const d0 = makeNode('d', 0, [c0])
  const c2 = makeNode('c', 2, [d0], { version: 1 })
  const d1 = makeNode('d', 1, [])

  tip.add(b0)
  tip.add(c0)
  tip.add(c1)
  tip.add(b1)
  tip.add(d0)
  tip.add(c2)
  tip.add(a0)

  t.is(tip.tip[tip.tip.length - 1], c2)

  tip.mark()

  tip.add(d1)

  t.is(tip.undo, 0)
})

test('topolist - version sort fuzz', function (t) {
  const nodes = []

  // writers
  // a -> version 0
  // b -> version 0
  // c -> version 1

  const writers = []
  writers.push(writer('a', 0))
  writers.push(writer('b', 1))
  writers.push(writer('c', 1))

  writers[1].version = 1 // bump version

  let lastVersionNode

  const NODES = 400
  while (nodes.length < NODES) {
    const writer = getRandom(writers)
    const deps = []
    for (const w of writers) {
      if (w === writer || w.version > writer.version || Math.random() < 0.5) continue
      if (w.head()) deps.push(w.head())
    }

    nodes.push(writer.add(deps))
    if (nodes.length === NODES / 2) {
      lastVersionNode = writer.head()
      writers[2].version = 1 // bump version
    }
  }

  // final node to add after ordering
  const last = writers[0].add([lastVersionNode])

  // store previous tip
  let prev

  // loop many times adding nodes randomly
  for (let i = 0; i < nodes.length * 2; i++) {
    const tip = new Topolist()
    const copy = nodes.slice()

    tip.add(copy.shift()) // add a0

    while (copy.length) {
      const node = getRandom(copy)
      let add = true
      for (const d of node.dependencies) {
        if (!tip.tip.includes(d)) {
          add = false
          break
        }
      }
      if (!add) continue
      tip.add(node)
      copy.splice(copy.indexOf(node), 1)
    }

    // check prev tip
    if (!prev) prev = tip.print()
    else t.alike(tip.print(), prev)

    tip.mark()
    tip.add(last)

    const u = tip.flush()
    u.tip = null

    t.alike(u, { shared: NODES, undo: 0, length: NODES + 1, indexed: [], tip: null })
  }
})

function makeNode (key, length, dependencies, { version = 0, value = key + length } = {}) {
  const node = {
    writer: { core: { key: b4a.from(key) } },
    length,
    dependents: new Set(),
    dependencies,
    version,
    value
  }

  for (const dep of dependencies) dep.dependents.add(node)

  return node
}

function writer (key, version = 0) {
  return {
    add (dependencies) {
      const head = this.head()
      if (head) dependencies.push(head)
      const node = makeNode(key, this.nodes.length, [...dependencies], { version: this.version })
      this.nodes.push(node)
      return node
    },
    version,
    head () {
      if (this.nodes.length === 0) return null
      return this.nodes[this.nodes.length - 1]
    },
    nodes: []
  }
}

function getRandom (arr) {
  return arr[Math.floor(Math.random() * arr.length)]
}
