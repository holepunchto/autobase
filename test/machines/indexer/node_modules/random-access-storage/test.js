var tape = require('tape')
var ras = require('./')

tape('basic read', function (t) {
  t.plan(2 * 4 + 4)

  var expected = [Buffer.from('hi'), Buffer.from('ho')]
  var queued = expected.slice(0)
  var s = ras({
    read: function (req) {
      process.nextTick(function () {
        t.same(req.offset, 0)
        t.same(req.size, 2)
        req.callback(null, queued.shift())
      })
    }
  })

  t.ok(s.readable)
  t.notOk(s.writable)
  t.notOk(s.deletable)
  t.notOk(s.statable)
  s.read(0, 2, ondata)
  s.read(0, 2, ondata)

  function ondata (err, data) {
    t.error(err, 'no error')
    t.same(data, expected.shift())
  }
})

tape('basic write', function (t) {
  t.plan(2 * 2 + 4)

  var expected = [Buffer.from('hi'), Buffer.from('ho')]
  var s = ras({
    write: function (req) {
      t.same(req.data, expected.shift())
      req.callback(null)
    }
  })

  t.notOk(s.readable)
  t.ok(s.writable)
  t.notOk(s.deletable)
  t.notOk(s.statable)
  s.write(0, Buffer.from('hi'), onwrite)
  s.write(0, Buffer.from('ho'), onwrite)

  function onwrite (err, write) {
    t.error(err, 'no error')
  }
})

tape('basic del', function (t) {
  t.plan(2 + 2 * 3 + 4)

  var s = ras({
    del: function (req) {
      t.same(req.offset, 0)
      t.same(req.size, 2)
      req.callback(null)
    }
  })

  t.notOk(s.readable)
  t.notOk(s.writable)
  t.ok(s.deletable)
  t.notOk(s.statable)
  s.del(0, 2, ondelete)
  s.del(0, 2, ondelete)
  s.del(0, 2) // cb is optional

  function ondelete (err) {
    t.error(err, 'no error')
  }
})

tape('basic stat', function (t) {
  t.plan(2 * 2 + 4)

  var s = ras({
    stat: function (req) {
      req.callback(null, {size: 42})
    }
  })

  t.notOk(s.readable)
  t.notOk(s.writable)
  t.notOk(s.deletable)
  t.ok(s.statable)
  s.stat(onstat)
  s.stat(onstat)

  function onstat (err, st) {
    t.error(err, 'no error')
    t.same(st, {size: 42})
  }
})

tape('no opts', function (t) {
  var s = ras()

  t.notOk(s.readable)
  t.notOk(s.writable)
  t.notOk(s.deletable)
  t.notOk(s.statable)
  t.end()
})

tape('many open calls only trigger one _open', function (t) {
  t.plan(1)

  var s = ras({
    open: function (req) {
      process.nextTick(function () {
        t.pass('is opening')
        req.callback(null)
      })
    }
  })

  s.open()
  s.open()
  s.open()
  s.open()
  s.open()
  setImmediate(() => s.open())
})

tape('open errors', function (t) {
  t.plan(3 + 2)

  var s = ras({
    open: function (req) {
      t.pass('in open')
      setImmediate(() => req.callback(new Error('nope')))
    },
    write: function (req) {
      t.fail('should not get here')
      req.callback(null)
    }
  })

  s.write(0, Buffer.from('hi'), onwrite)
  s.write(0, Buffer.from('hi'), onwrite)
  s.write(0, Buffer.from('hi'), onwrite)
  s.open() // should try and open again

  function onwrite (err) {
    t.same(err, new Error('Not opened'))
  }
})

tape('open before read', function (t) {
  t.plan(5 * 2 + 1 + 1)

  var open = false
  var s = ras({
    open: function (req) {
      t.ok(!open, 'only open once')
      open = true
      req.callback(null)
    },
    read: function (req) {
      t.ok(open, 'is open')
      req.callback(null, Buffer.from('hi'))
    }
  })

  t.notOk(s.opened, 'opened property not set')
  s.read(0, 2, ondata)
  s.read(0, 2, ondata)

  function ondata (err, data) {
    t.error(err, 'no error')
    t.ok(open, 'is open')
    t.ok(s.opened, 'opened property set')
    t.same(data, Buffer.from('hi'))
  }
})

tape('close', function (t) {
  t.plan(7)

  var s = ras({
    close: function (req) {
      t.pass('closing')
      req.callback(null)
    }
  })

  s.on('close', () => t.pass('close emitted'))
  s.open()
  s.close()
  s.close()
  s.close(function () {
    t.pass('calls the callback')
  })

  s.read(0, 10, err => t.same(err, new Error('Closed')))
  s.stat(err => t.same(err, new Error('Closed')))
  s.write(0, Buffer.from('hi'), err => t.same(err, new Error('Closed')))
  s.del(0, 10, err => t.same(err, new Error('Closed')))
})

tape('close, no open', function (t) {
  t.plan(5)

  var s = ras({
    close: req => t.fail('only close if open')
  })

  s.close()
  s.close()
  s.close(function () {
    t.pass('calls the callback')
  })

  s.read(0, 10, err => t.same(err, new Error('Closed')))
  s.stat(err => t.same(err, new Error('Closed')))
  s.write(0, Buffer.from('hi'), err => t.same(err, new Error('Closed')))
  s.del(0, 10, err => t.same(err, new Error('Closed')))
})

tape('destroy', function (t) {
  t.plan(4)

  var s = ras({
    open: req => t.fail('no open'),
    destroy: function (req) {
      t.pass('destroying')
      req.callback(null)
    }
  })

  s.on('destroy', () => t.pass('destroy emitted'))
  s.destroy()
  s.destroy(function (err) {
    t.error(err, 'no error')
    t.pass('calls the callback')
  })
})

tape('destroy closes first', function (t) {
  t.plan(2)

  var s = ras({
    close: function (req) {
      t.pass('closing')
      req.callback(null)
    },
    destroy: function (req) {
      t.ok(s.closed, 'is closed')
      req.callback(null)
    }
  })

  s.open()
  s.destroy()
})

tape('destroy with explicit close first', function (t) {
  t.plan(2)

  var s = ras({
    close: function (req) {
      t.pass('closing')
      req.callback(null)
    },
    destroy: function (req) {
      t.ok(s.closed, 'is closed')
      req.callback(null)
    }
  })

  s.open()
  s.close()
  s.destroy()
})

tape('open and close', function (t) {
  t.plan(7)

  var s = ras({
    open: function (req) {
      t.pass('opening')
      req.callback(null)
    },
    close: function (req) {
      t.pass('closing')
      req.callback(null)
    }
  })

  s.open()
  s.close()
  s.close()
  s.close(function () {
    t.pass('calls the callback')
  })

  s.read(0, 10, err => t.same(err, new Error('Closed')))
  s.stat(err => t.same(err, new Error('Closed')))
  s.write(0, Buffer.from('hi'), err => t.same(err, new Error('Closed')))
  s.del(0, 10, err => t.same(err, new Error('Closed')))
})

tape('write and close', function (t) {
  t.plan(1 + 5 + 1 + 3)

  var closed = false
  var s = ras({
    open: function (req) {
      t.pass('opened')
      req.callback(null)
    },
    write: function (req) {
      t.pass('in write')
      process.nextTick(function () {
        req.callback(null)
      })
    },
    close: function (req) {
      t.notOk(closed, 'not closed yet')
      closed = true
      req.callback(null)
    }
  })

  s.write(0, Buffer.from('hi'))
  s.write(0, Buffer.from('hi'))
  s.write(0, Buffer.from('hi'))
  s.write(0, Buffer.from('hi'))
  s.write(0, Buffer.from('hi'))
  s.close(err => t.error(err, 'no error'))
  s.close(err => t.error(err, 'no error'))
  s.close(err => t.error(err, 'no error'))
})

tape('open readonly', function (t) {
  t.plan(2)

  var s = ras({
    open: () => t.fail('no open'),
    openReadonly: function (req) {
      t.pass('open readonly')
      req.callback(null)
    },
    read: req => req.callback(null, Buffer.from('hi'))
  })

  s.open()
  s.read(0, 10, err => t.error(err, 'no error'))
})

tape('open readonly and then write', function (t) {
  t.plan(4)

  var readonlyFirst = true

  var s = ras({
    open: function (req) {
      t.notOk(readonlyFirst, 'open readonly first')
      req.callback(null)
    },
    openReadonly: function (req) {
      t.ok(readonlyFirst, 'open readonly first')
      readonlyFirst = false
      req.callback(null)
    },
    read: req => req.callback(null, Buffer.from('hi')),
    write: req => req.callback(null)
  })

  s.open()
  s.read(0, 2, err => t.error(err, 'no error'))
  s.write(0, Buffer.from('hi'), err => t.error(err, 'no error'))
})

tape('open readonly ignored when first op is write', function (t) {
  t.plan(3)

  var s = ras({
    open: function (req) {
      t.pass('should open')
      req.callback(null)
    },
    openReadonly: req => t.fail('first op is a write'),
    read: req => req.callback(null, Buffer.from('hi')),
    write: req => req.callback(null)
  })

  s.write(0, Buffer.from('hi'), err => t.error(err, 'no error'))
  s.read(0, 2, err => t.error(err, 'no error'))
})

tape('always async', function (t) {
  var s = ras({
    read: req => req.callback(null, Buffer.from('hi'))
  })

  s.open(function () {
    var sync = true

    s.read(0, 2, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hi'))
      t.notOk(sync)
      t.end()
    })

    sync = false
  })
})

tape('open error forwarded to dependents', function (t) {
  var s = ras({
    open: req => req.callback(new Error('Nope')),
    read: req => req.callback(null, Buffer.from('hi')),
    write: req => req.callback(null, null)
  })

  s.write(0, Buffer.from('hi'), function (err) {
    t.ok(err)
    t.same(err.message, 'Nope')
  })

  s.read(0, 2, function (err) {
    t.ok(err)
    t.same(err.message, 'Nope')
  })

  s.close(function (err) {
    t.ok(!err)
    t.end()
  })
})

tape('close immediately', function (t) {
  t.plan(11)

  var closed = false
  var s = ras({
    read (req) {
      setImmediate(function () {
        t.ok(!closed)
        req.callback(null, Buffer.alloc(1))
      })
    },
    close (req) {
      closed = true
      t.pass('closed')
      req.callback()
    }
  })

  for (var i = 0; i < 10; i++) s.read(0, 1, () => {})
  s.close()
})
