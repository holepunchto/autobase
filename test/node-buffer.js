const test = require('brittle')
const NodeBuffer = require('../lib/node-buffer')

test('node buffer - simple operation', (t) => {
  const nodes = new NodeBuffer()

  t.is(nodes.push('0'), 0)
  t.is(nodes.push('1'), 1)
  t.is(nodes.push('2'), 2)

  t.is(nodes.get(0), '0')
  t.is(nodes.get(1), '1')
  t.is(nodes.get(2), '2')

  t.is(nodes.shift(), '0')

  t.is(nodes.get(0), null)
  t.is(nodes.get(1), '1')
  t.is(nodes.get(2), '2')

  t.is(nodes.shift(), '1')

  t.is(nodes.get(0), null)
  t.is(nodes.get(1), null)
  t.is(nodes.get(2), '2')

  t.is(nodes.shift(), '2')

  t.is(nodes.get(0), null)
  t.is(nodes.get(1), null)
  t.is(nodes.get(2), null)
  t.is(nodes.get(3), null)

  t.is(nodes.shift(), null)
})

test('node buffer - grow', (t) => {
  const nodes = new NodeBuffer(0, 1)

  t.is(nodes.buffer.length, 1)

  t.is(nodes.push('0'), 0)
  t.is(nodes.push('1'), 1)
  t.is(nodes.push('2'), 2)

  t.is(nodes.length, 3)
  t.is(nodes.buffer.length, 4)

  t.is(nodes.get(0), '0')
  t.is(nodes.get(1), '1')
  t.is(nodes.get(2), '2')

  t.is(nodes.shift(), '0')

  t.is(nodes.get(0), null)
  t.is(nodes.get(1), '1')
  t.is(nodes.get(2), '2')

  t.is(nodes.shift(), '1')

  t.is(nodes.get(0), null)
  t.is(nodes.get(1), null)
  t.is(nodes.get(2), '2')

  t.is(nodes.shift(), '2')

  t.is(nodes.get(0), null)
  t.is(nodes.get(1), null)
  t.is(nodes.get(2), null)

  t.is(nodes.shift(), null)
})

test('node buffer - many entries', (t) => {
  const nodes = new NodeBuffer(0, 1)

  for (let i = 0; i < 0x1000001; i++) {
    nodes.push(i.toString(10))
  }

  t.is(nodes.length, 0x1000001)
  t.is(nodes.buffer.length, 0x2000000)

  for (let i = 0; i < 0x1000001; i++) {
    if (Number(nodes.shift()) !== i) {
      t.fail(i)
      break
    }
  }

  t.is(nodes.length, 0x1000001)
  t.is(nodes.buffer.length, 1)
})

test('node buffer - grow only if necessary', (t) => {
  const nodes = new NodeBuffer(0, 4)

  nodes.push(0)
  nodes.push(1)
  nodes.push(2)

  const b1 = nodes.buffer

  // flush buffer
  nodes.shift()
  nodes.shift()
  nodes.shift()

  nodes.push(3)
  nodes.push(4)
  nodes.push(5)
  nodes.push(6)

  t.is(nodes.buffer, b1)

  nodes.push(7)

  const b2 = nodes.buffer

  t.not(nodes.buffer, b1)
  t.is(nodes.buffer, b2)
  t.is(nodes.buffer.length, 8)

  t.is(nodes.get(1), null)
  t.is(nodes.get(8), null)

  t.is(nodes.get(3), 3)
  t.is(nodes.get(4), 4)
  t.is(nodes.get(5), 5)

  t.is(nodes.length, 8)
})

test('node buffer - push shift push', (t) => {
  const nodes = new NodeBuffer(0, 1)

  nodes.push(0)
  nodes.shift()

  nodes.push(0)
  nodes.shift()

  nodes.push(0)
  nodes.shift()

  nodes.push(0)

  t.is(nodes.length, 4)
  t.is(nodes.buffer.length, 1)
})

test('node buffer - offset', (t) => {
  const nodes = new NodeBuffer(5, 1)

  t.is(nodes.length, 5)

  t.is(nodes.push(5), 5)

  t.is(nodes.get(5), 5)
  t.is(nodes.length, 6)
  t.is(nodes.buffer.length, 1)
})

test('node buffer - shrink', (t) => {
  const nodes = new NodeBuffer(0, 1)

  t.is(nodes.buffer.length, 1)

  // fill buffer
  nodes.push('0')
  nodes.push('1')
  nodes.push('2')
  nodes.push('3')

  t.is(nodes.length, 4)
  t.is(nodes.buffer.length, 4)

  t.is(nodes.shift(), '0')
  t.is(nodes.shift(), '1')
  t.is(nodes.shift(), '2')
  t.is(nodes.shift(), '3')

  t.is(nodes.shift(), null)

  t.is(nodes.push('4'), 4)
  t.is(nodes.buffer.length, 1)
})
