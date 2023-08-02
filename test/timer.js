const test = require('brittle')
const Timer = require('../lib/timer')

test('timer - simple', t => {
  t.plan(1)

  const timer = new Timer(handler, 100)

  const fail = setTimeout(() => { t.fail() }, 200)
  timer.bump()

  function handler () {
    clearTimeout(fail)
    t.pass()
    timer.stop()
  }
})

test('timer - bump', t => {
  t.plan(1)

  const timer = new Timer(handler, 100)

  timer.bump()
  const int = setInterval(() => timer.bump(), 50)

  const pass = setTimeout(() => {
    clearInterval(int)
    t.pass()
    timer.stop()
  }, 200)

  function handler () {
    clearInterval(int)
    clearTimeout(pass)

    t.fail()
    timer.stop()
  }
})

test('timer - bump max timeout', t => {
  t.plan(1)

  const timer = new Timer(handler, 100, { limit: 200 })

  timer.bump()
  const int = setInterval(timer.bump.bind(timer), 50)

  const fail = setTimeout(() => {
    clearInterval(int)
    t.fail()
    timer.stop()
  }, 400)

  function handler () {
    clearInterval(int)
    clearTimeout(fail)

    t.pass()
    timer.stop()
  }
})

test('timer - extend', t => {
  t.plan(2)

  const timer = new Timer(handler, 100)

  timer.bump()
  timer.extend()

  t.is(timer._interval, 200)

  const pass = setTimeout(() => {
    t.pass()
    timer.stop()
  }, 150)

  function handler () {
    clearTimeout(pass)

    t.fail()
    timer.stop()
  }
})

test('timer - reset', t => {
  t.plan(2)

  const timer = new Timer(handler, 50)

  timer.bump()
  timer.extend()
  timer.extend()
  timer.extend()

  t.is(timer._interval, 400)

  timer.reset()

  const fail = setTimeout(() => {
    t.fail()
    timer.stop()
  }, 200)

  function handler () {
    clearTimeout(fail)

    t.pass()
    timer.stop()
  }
})

test('timer - await execution', t => {
  t.plan(1)

  let running = false

  const timer = new Timer(promise, 100, { limit: 200 })

  timer.bump()

  const int = setInterval(() => timer.bump(), 100)

  t.teardown(() => {
    timer.stop()
    clearInterval(int)
  })

  function promise () {
    if (running) return t.fail()
    running = true

    return new Promise(resolve => {
      setTimeout(resolve, 500)
    }).then(() => {
      t.pass()
      running = false
    })
  }
})
