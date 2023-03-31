const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const brittle = require('brittle')
const { Linearizer, Writer } = require('../')

module.exports = {
  fuzz,
  generate,
  rollBack,
  test,
  testSingle,
  // rollBackBatch,
  formatResult
}

class DummyNode {
  constructor (key, seq, links = []) {
    this.key = key
    this.seq = seq
    this.links = links
    this.clock = new Map()
  }

  get ref () {
    return `${this.key}:${this.seq}`
  }

  toDetails () {
    return {
      ref: this.ref,
      args: [this.key, this.seq],
      deps: this.links.map(l => l.ref)
      // clock: this.clock
    }
  }

  addLink (link) {
    if (clockContains(this.clock, link)) return

    updateClock(this.clock, link)
    this.links.push(link)
  }

  toJavaScript () {
    return `const ${this.key + this.seq} = l.addHead(${this.key}.add())\n`
  }
}

class DummyWriter {
  constructor (key, majority) {
    this.key = key
    this.seq = 0
    this.head = null
    this.majority = majority
  }

  add (links = []) {
    const check = updateClock(new Map(), ...links)
    if (this.head && !clockContains(check, this.head)) {
      links.push(this.head)
    }

    links.sort(nodeByClock)

    const node = new DummyNode(this.key, this.seq++)
    for (const link of links) node.addLink(link)

    if (node.links.length === 1 && node.links[0].key === this.key) {
      this.seq--
      return null
    }

    this.head = node

    return node
  }
}

function getLabel (result) {
  return crypto.randomBytes(3).toString('hex')
}

function fuzz (w, n, b, logLevel, dir) {
  const start = Date.now()
  for (let i = 0; true; i++) {
    // timing
    if (i && logLevel && i % logLevel === 0) {
      const interval = Date.now() - start
      console.log(timerLabel(w, n, i), (interval / i).toFixed(2), 'ms')
    }

    const { nodes, writers } = dagGen(w, n, b)
    const steps = nodes.map(n => n.toDetails())

    const result = rollBack(w, steps, 3)
    if (!result.pass) return fail(result, w, steps)

    // const resultBatch = rollBackBatch(w, steps, 3, result.nodes)
    // if (!resultBatch.pass) return fail(resultBatch, w, steps)

    const midway = result.nodes.length
    const majority = writers.slice(0, (writers.length >>> 1) + 1)
    continueWriting(nodes, majority, n, b)

    let more = rollBack(w, nodes.map(n => n.toDetails()), 3)
    if (!more.pass) return fail(more, w, nodes.map(n => n.toDetails()))

    let retry = 0
    while (more.nodes.length === midway) {
      continueWriting(nodes, majority, n, b)
      const dag = nodes.map(n => n.toDetails())
      more = rollBack(w, dag, 3)
      if (!more.pass) return fail(more, w, dag)

      if (++retry > 5) {
        return fail({ nodes: dag, writers: w, batch: n, deadlock: true })
      }
    }
  }

  function fail (result, w, steps) {
    const label = getLabel()

    console.log('----', label, '----')
    const js = result.deadlock
      ? formatDeadlock(result, label)
      : formatResult(result, label)

    fs.writeFileSync(path.join(dir, label + '.js'), js)

    const reason = result.deadlock ? 'deadlocked' : 'conflict'
    throw new Error(`Fuzzing failed: ${reason} - ${label}`)
  }
}

function individualTest (graph) {
  const writers = new Set()
  for (const { args } of graph) {
    writers.add(args[0])
  }

  const midway = graph.length / 2
  const half = graph.slice(0, midway)

  const result = rollBack(writers.size, half)
  if (!result.pass) {
    console.log(formatResult(result))
    return false
  }

  const more = rollBack(writers.size, graph, 3)
  if (!more.pass) {
    console.log(formatResult(more))
    // console.log(printTest(writers.size, graph))
    return false
  }

  if (more.nodes.length === result.nodes.length) {
    console.log(printDag(continued))
    return false
  }

  return true
}

function testSingle (label, graph) {
  brittle(label, t => {
    if (!individualTest(graph)) t.fail()
    t.pass()
  })
}

function test (label, graph) {
  brittle(label, t => {
    for (let i = 0; i < 1000; i++) {
      if (!individualTest(graph)) return t.fail()
    }
    t.pass()
  })
}

function generate (w, n, b) {
  const steps = dagGen(w, n, b)
  console.log(printDag(steps.map(n => n.toDetails())))
  const str = printGraphTest(w, steps.map(n => n.toDetails()))

  let result
  for (let i = 0; i < 1000; i++) {
    result = rollBack(w, steps.map(n => n.toDetails()))
    if (!result.pass) {
      console.log(str)
      throw new Error('dag failed')
    }
  }

  console.log('```\n', result.nodes.map(n => n.ref), '\n```')
  return str
}

function clockContains (clock, node) {
  return clock.has(node.key) && node.seq < clock.get(node.key)
}

function updateClock (clock, ...deps) {
  const stack = []

  stack.push(...deps)

  const visited = new Set()
  while (stack.length) {
    const node = stack.pop()

    if (visited.has(node)) continue
    visited.add(node)

    stack.push(...node.links)

    const current = clock.get(node.key)
    if (current && current > node.seq) continue

    clock.set(node.key, node.seq + 1)
  }

  return clock
}

function continueWriting (nodes, writers, size, branch) {
  const tails = nodes.reduce((tails, node) => {
    if (nodes.findIndex(n => n.links.includes(node)) < 0) {
      tails.add(node)
    }
    return tails
  }, new Set())

  const length = nodes.length + size
  while (nodes.length < length) {
    const links = []
    const writer = writers[Math.floor(Math.random() * writers.length)]

    for (const tail of tails) {
      if (Math.random() >= branch) links.push(tail)
    }

    const node = writer.add(links)
    if (!node) continue

    tails.add(node)
    nodes.push(node)

    for (const link of links) {
      if (Math.random() >= branch) {
        tails.delete(link)
      }
    }
  }

  return nodes
}

function dagGen (w = 3, size = 10, branchFactor = 0.3) {
  const writers = []
  const majority = Math.floor(w / 2) + 1
  for (let i = 0; i < w; i++) writers.push(new DummyWriter(String.fromCharCode(0x61 + i), majority))

  return createDag(writers, size, branchFactor)
}

function createDag (writers, size, branch) {
  const nodes = []

  const tails = new Set()

  while (nodes.length < size) {
    const links = []
    const writer = writers[Math.floor(Math.random() * writers.length)]

    for (const tail of tails) {
      if (Math.random() >= branch) links.push(tail)
    }

    const node = writer.add(links)
    if (!node) continue

    tails.add(node)
    nodes.push(node)

    for (const link of links) {
      if (Math.random() >= branch) {
        tails.delete(link)
      }
    }
  }

  return { nodes, writers }
}

function rollBack (n, steps, batch = 3, result = []) {
  const writers = {}
  const writersLength = n

  for (let i = 0; i < n; i++) {
    const key = String.fromCharCode(0x61 + i)
    writers[key] = new Writer(key)
  }

  const nodes = new Map()
  const graph = new Linearizer(Object.values(writers))

  let pos = 0

  for (let i = 0; i < steps.length; i++) {
    const { args, deps, ref } = steps[i]

    const dependencies = []
    for (const d of deps) {
      if (nodes.has(d)) dependencies.push(nodes.get(d))
    }

    const node = writers[args[0]].add(...dependencies)

    nodes.set(ref, node)
    graph.addHead(node)

    const yielded = []

    const current = steps.slice(0, i + 1)
    const view = graph.print()
    while (true) {
      // const view = graph.print()
      const node = graph.shift()
      if (!node) break
      node.steps = current
      node.view = view
      nodes.delete(node.ref)
      yielded.push(node)
    }

    if (!yielded) node.preview = null

    const tip = result.slice(0, pos)

    for (const n of yielded) {
      if (pos >= result.length) {
        result.push(n)
      } else if (!compareNode(result[pos], n)) {
        return {
          writers: writersLength,
          pass: false,
          left: result[pos],
          right: n
        }
      }

      tip.push(n)
      pos++
    }
  }

  const next = steps.slice()

  const remove = Math.random() * batch
  for (let i = 0; i < remove; i++) {
    const heads = getHeads(next)
    const del = Math.floor(Math.random() * heads.length)

    const head = next[heads[del]]
    if (!head.deps.length) continue
    next.splice(heads[del], 1)

    if (!next.filter(n => n.deps.length).length) {
      return { nodes: result, pass: true }
    }
  }

  return rollBack(n, next, batch, result)
}

function getHeads (nodes) {
  return nodes.reduce((heads, node, i) => {
    if (nodes.findIndex(n => n.deps.includes(node.ref)) < 0) {
      heads.push(i)
    }
    return heads
  }, [])
}

function getTails (nodes) {
  return nodes.filter(node => !node.dependencies.length)
}

function compareNode (a, b) {
  return a.writer.key === b.writer.key && a.seq === b.seq
}

function nodeByClock (a, b) {

  if (b.clock.get(a.key) > a.seq) return 1

  if (a.clock.get(b.key) <= b.seq) return 1

  if (a.clock.get(b.key) > b.seq) return -1

  if (b.clock.get(a.key) <= a.seq) return -1

  return 0

}

function printDag (steps) {
  let str = '```mermaid\n'
  str += 'graph TD;\n'
  for (const node of steps) {
    for (const dep of node.deps) {
      str += '    ' + node.ref + '-->' + dep + ';\n'
    }
  }
  str += '```'
  return str
}

function printTestBranch (w, steps) {
  let str = ''

  let writers = '['
  for (let i = 0; i < w; i++) {
    const key = String.fromCharCode(0x61 + i)
    str += `    const ${key} = new Writer('${key}')\n`
    writers += key + (i === w - 1 ? ']' : ', ')
  }

  str += `    const l = new Linearizer(${writers})\n\n`

  for (const step of steps) {
     str += `    const ${step.ref.replace(/:/, '')} = l.addHead(${step.args[0]}.add(${step.deps.map(d => d.replace(/:/, '')).join(', ')}))\n`
  }

  str += '    return l\n'

  return str
}

function printTest (w, steps) {
  let str = ''
  let writers = '['
  for (let i = 0; i < w; i++) {
    const key = String.fromCharCode(0x61 + i)
    str += `const ${key} = new Writer('${key}')\n`
    writers += key + (i === w - 1 ? ']' : ', ')
  }

  str += `const l = new Linearizer(${writers})\n\n`

  for (const step of steps) {
     str += `const ${step.ref.replace(/:/, '')} = l.addHead(${step.args[0]}.add(${step.deps.map(d => d.replace(/:/, '')).join(', ')}))\n`
    // str += `l.addHead(${step.args[0]}.add(${JSON.stringify(step.deps.map(d => d.replace(/:/, '')))}))\n`
    // str += step.toJavaScript()
  }

  str += '\nconst graph = []\n'
  str += 'while (true) {\n'
  str += '  const node = l.shift()\n'
  str += '  if (!node) break\n'
  str += '  graph.push(node)\n'
  str += '}\n'

  return str
}

function timerLabel (w, n, i) {
  return w + ' writers ' + n + ' nodes - ' + i + ' cases'
}

function formatResult ({ writers, left, right }, label = '(no label)') {
  // const yielded = left.yielded

  let js = `const test = require('brittle')\n`
  js += `const { Linearizer, Writer } = require('../../')\n\n`
  js += `test('fuzz ${label}', function (t) {
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
})`

  js += '\n'
  js += 'function makeGraph (left) {\n'
  js += '  if (left) {\n'
  js += printTestBranch(writers, left.steps)
  js += '  } else {\n'
  js += printTestBranch(writers, right.steps)
  js += '  }\n'
  js += '}\n'

  return js

  // let str = 'expected `' + left.ref + '`\n'
  // str += left.view + '\n'

  // str += 'received `' + right.ref + '`\n'
  // str += right.view + '\n'

  // str += 'preview `' + right.ref + '`\n'
  // str += preview + '\n'

  // return str
}


function formatDeadlock ({ nodes, writers, batch }, label = '(no label)') {
  let js = 'const test = require(\'brittle\')\n'
  js += 'const { Linearizer, Writer } = require(\'../../\')\n\n'
  js += `test('fuzz ${label}', function (t) {\n`
  let w = '['
  for (let i = 0; i < writers; i++) {
    const key = String.fromCharCode(0x61 + i)
    js += `  const ${key} = new Writer('${key}')\n`
    w += key + (i === writers - 1 ? ']' : ', ')
  }

  js += `  const l = new Linearizer(${w})\n\n`

  js +=`
  const nodes = []\n\n`

  for (const step of nodes) {
    js += `    const ${step.ref.replace(/:/, '')} = ${step.args[0]}.add(${step.deps.map(d => d.replace(/:/, '')).join(', ')})\n`
    js += `    nodes.push(${step.ref.replace(/:/, '')})\n`
  }

  js += `

  let length
  let pos = 0
  while (true) {
    while (pos < pos + ${batch}) {
      if (++pos >= ${nodes.length}) return t.end()
      l.addHead(nodes.shift())
    }

    const result = []
    while (true) {
      const n = l.shift()
      result.push(n)
      if (!n) break
      t.comment('yield #' + tick + ', ' + ref1)
    }

    t.not(length, result.length, 'loop ' + i)
  }
})`

  return js

  // let str = 'expected `' + left.ref + '`\n'
  // str += left.view + '\n'

  // str += 'received `' + right.ref + '`\n'
  // str += right.view + '\n'

  // str += 'preview `' + right.ref + '`\n'
  // str += preview + '\n'

  // return str
}

function printGraphTest (w, steps) {
  return `module.exports = ${JSON.stringify(steps).replace(/},{/g, '},\n{')}\n`
}

function writeTest (path, w, steps) {
  fs.writeFileSync(path, printGraphTest(w, steps))
}
