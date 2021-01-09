var tape = require('tape')
var siphash24 = require('./')

var KEY = Buffer.from('abcdefghijklmnop')

tape('basic "hello world"', function (t) {
  var out = siphash24(Buffer.from('hello world'), KEY)
  t.same(Buffer.from(out).toString('hex'), 'cc381e910d3720ce')
  out = siphash24(Buffer.from('hello world'), KEY)
  t.same(Buffer.from(out).toString('hex'), 'cc381e910d3720ce')
  t.end()
})

tape('basic "foo"', function (t) {
  var out = siphash24(Buffer.from('foo'), KEY)
  t.same(Buffer.from(out).toString('hex'), '3f969a1d4c0c0f35')
  out = siphash24(Buffer.from('foo'), KEY)
  t.same(Buffer.from(out).toString('hex'), '3f969a1d4c0c0f35')
  t.end()
})

tape('pass in output', function (t) {
  var out = Buffer.alloc(8)
  siphash24(Buffer.from('foo'), KEY, out)
  t.same(Buffer.from(out).toString('hex'), '3f969a1d4c0c0f35')
  out.fill(0)
  siphash24(Buffer.from('foo'), KEY, out)
  t.same(Buffer.from(out).toString('hex'), '3f969a1d4c0c0f35')
  t.end()
})

tape('big input', function (t) {
  var out = siphash24(Buffer.alloc(1024 * 1024), KEY)
  t.same(Buffer.from(out).toString('hex'), 'ef0fca94174ee536')
  t.end()
})

tape('asserts', function (t) {
  t.throws(function () {
    siphash24()
  })
  t.throws(function () {
    siphash24(Buffer.from('hi'), Buffer.alloc(0))
  })
  t.throws(function () {
    siphash24(Buffer.from('hi'), Buffer.alloc(8), Buffer.alloc(0))
  })
  t.end()
})
