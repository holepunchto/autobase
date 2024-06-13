const test = require('brittle')
const m = require('../lib/messages')
const c = require('compact-encoding')
const b4a = require('b4a')

test('wakeup', function (t) {
  const key = b4a.alloc(32)

  t.alike(c.decode(m.Wakeup, c.encode(m.Wakeup, { version: 1, type: 0 })), { version: 1, type: 0, writers: null })
  t.alike(c.decode(m.Wakeup, c.encode(m.Wakeup, { version: 0, type: 0 })), { version: 0, type: 0, writers: null })
  t.alike(c.decode(m.Wakeup, c.encode(m.Wakeup, { version: 0, type: 1, writers: [{ key, length: 1 }] })), { version: 0, type: 1, writers: [{ key, length: -1 }] })
  t.alike(c.decode(m.Wakeup, c.encode(m.Wakeup, { version: 1, type: 1, writers: [{ key, length: 1 }] })), { version: 1, type: 1, writers: [{ key, length: 1 }] })
})
