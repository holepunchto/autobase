const tape = require('tape')
const nanoresource = require('../')

tape('basic usage', function (t) {
  t.plan(2 + 2 + 3 * 3 + 3 * 3)

  let opened = false
  let closed = false

  const r = nanoresource({
    open (cb) {
      t.notOk(closed, 'not closed in open')
      t.notOk(opened, 'only open once')
      opened = true
      cb(null)
    },
    close (cb) {
      t.ok(opened, 'opened when closing')
      t.notOk(closed, 'only close once')
      closed = true
      cb(null)
    }
  })

  r.open(onopen)
  r.open(onopen)
  r.open(onopen)

  r.close(onclose)
  r.close(onclose)
  r.close(onclose)

  function onopen (err) {
    t.error(err, 'no error')
    t.ok(r.opened, 'was opened')
    t.ok(opened, 'open ran')
  }

  function onclose (err) {
    t.error(err, 'no error')
    t.ok(r.closed, 'was closed')
    t.ok(closed, 'close ran')
  }
})

tape('open/close is never sync', function (t) {
  const r = nanoresource({
    open (cb) {
      cb(null)
    },
    close (cb) {
      cb(null)
    }
  })

  let syncOpen = true
  r.open(function () {
    t.notOk(syncOpen)
    let syncClose = true
    r.close(function () {
      t.notOk(syncClose)
      t.end()
    })
    syncClose = false
  })
  syncOpen = false
})

tape('active/inactive', function (t) {
  t.plan(2 + 10 * 1)

  let fails = 0
  let ran = 0

  const r = nanoresource({
    close (cb) {
      t.same(fails, 0)
      t.same(ran, 10)
      cb(null)
    }
  })

  for (let i = 0; i < 10; i++) {
    r.open(function () {
      t.ok(r.active())
      ran++
      fails++
      setImmediate(function () {
        fails--
        r.inactive()
      })
    })
  }

  r.open(() => r.close())
})

tape('active after close', function (t) {
  t.plan(5)

  const r = nanoresource({
    close (cb) {
      t.notOk(r.active(), 'cannot be active in close')
      setImmediate(cb)
    }
  })

  t.ok(r.active(), 'can be active')
  r.open()
  t.ok(r.active(), 'can be active')
  r.close(() => t.notOk(r.active(), 'still cannot be active'))
  t.notOk(r.active(), 'cannot be active cause close was call')

  r.inactive()
  r.inactive()
})
