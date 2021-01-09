const Nanoguard = require('./')
const tape = require('tape')

tape('basic', function (t) {
  const g = new Nanoguard()

  g.ready(function () {
    t.pass('is ready')
    t.end()
  })
})

tape('waiting', function (t) {
  const g = new Nanoguard()

  t.equals(g.waiting, false, 'not waiting')
  g.wait()
  t.equals(g.waiting, true, 'waiting')
  g.wait()
  t.equals(g.waiting, true, 'waiting')
  g.continueSync()
  t.equals(g.waiting, true, 'waiting')
  g.continueSync()
  t.equals(g.waiting, false, 'not waiting')

  t.end()
})

tape('resume next tick', function (t) {
  const g = new Nanoguard()

  g.wait()
  g.ready(function () {
    t.pass('is ready')
    t.ok(!sync)
    t.end()
  })
  let sync = true
  g.continue()
  sync = false
})

tape('wait and then resume', function (t) {
  const g = new Nanoguard()
  let ready = false

  g.wait()
  g.ready(function () {
    ready = true
    t.pass('is ready')
  })

  t.notOk(ready)
  g.continueSync()
  t.ok(ready)
  t.end()
})

tape('multiple ready and wait and then resume', function (t) {
  const g = new Nanoguard()
  let ready = 0

  g.ready(function () {
    t.same(++ready, 1)
    t.pass('is ready')
  })

  t.same(ready, 1)
  g.wait()
  g.wait()

  g.ready(function () {
    t.same(++ready, 2)
    t.pass('is ready again')
  })

  t.same(ready, 1)
  g.continueSync()
  t.same(ready, 1)
  g.continueSync()
  t.same(ready, 2)
  t.end()
})

tape('wait and continue', function (t) {
  const g = new Nanoguard()

  const continueOnce = g.waitAndContinue()

  g.ready(function () {
    t.pass('continued')
    t.end()
  })

  continueOnce()
  continueOnce()
  continueOnce()
  continueOnce()
})

tape('depend', function (t) {
  const g = new Nanoguard()
  const o = new Nanoguard()

  let ready = false

  g.depend(o)
  g.wait()

  g.ready(function () {
    t.ok(ready)

    ready = false
    o.wait()

    g.ready(function () {
      t.ok(ready)
      t.end()
    })

    setImmediate(function () {
      ready = true
      o.continueSync()
    })
  })

  ready = true
  g.continueSync()
})
