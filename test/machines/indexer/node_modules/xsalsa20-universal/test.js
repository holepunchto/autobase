const tape = require('tape')
const XOR = require('./')
const XORBR = require('./browser')

tape('basic', function (t) {
  const a = new XOR(Buffer.alloc(24), Buffer.alloc(32))
  const b = new XOR(Buffer.alloc(24), Buffer.alloc(32))
  const aOut = Buffer.alloc(1000)
  const bOut = Buffer.alloc(1000)

  a.update(aOut, aOut)
  b.update(bOut.slice(0, 500), bOut.slice(0, 500))
  b.update(bOut.slice(500), bOut.slice(500))

  t.same(aOut, bOut)
  t.end()
})

tape('basic browser', function (t) {
  const a = new XORBR(Buffer.alloc(24), Buffer.alloc(32))
  const b = new XORBR(Buffer.alloc(24), Buffer.alloc(32))
  const aOut = Buffer.alloc(1000)
  const bOut = Buffer.alloc(1000)

  a.update(aOut, aOut)
  b.update(bOut.slice(0, 500), bOut.slice(0, 500))
  b.update(bOut.slice(500), bOut.slice(500))

  t.same(aOut, bOut)
  t.end()
})

tape('basic browser/node compat', function (t) {
  const a = new XOR(Buffer.alloc(24), Buffer.alloc(32))
  const b = new XORBR(Buffer.alloc(24), Buffer.alloc(32))
  const aOut = Buffer.alloc(1000)
  const bOut = Buffer.alloc(1000)

  a.update(aOut, aOut)
  b.update(bOut.slice(0, 500), bOut.slice(0, 500))
  b.update(bOut.slice(500), bOut.slice(500))

  t.same(aOut, bOut)
  t.end()
})
