const tape = require('tape')
const crypto = require('./')

tape('randomBytes', function (t) {
  const buffer = crypto.randomBytes(100)
  t.ok(Buffer.isBuffer(buffer))
  t.notSame(crypto.randomBytes(100), buffer)
  t.end()
})

tape('key pair', function (t) {
  const keyPair = crypto.keyPair()

  t.same(keyPair.publicKey.length, 32)
  t.same(keyPair.secretKey.length, 64)
  t.end()
})

tape('sign', function (t) {
  const keyPair = crypto.keyPair()
  const message = Buffer.from('hello world')

  const sig = crypto.sign(message, keyPair.secretKey)

  t.same(sig.length, 64)
  t.ok(crypto.verify(message, sig, keyPair.publicKey))
  t.notOk(crypto.verify(message, Buffer.alloc(64), keyPair.publicKey))
  t.end()
})

tape('hash leaf', function (t) {
  const data = Buffer.from('hello world')

  t.same(crypto.data(data), Buffer.from('ccfa4259ee7c41e411e5770973a49c5ceffb5272d6a37f2c6f2dac2190f7e2b7', 'hex'))
  t.end()
})

tape('hash parent', function (t) {
  const data = Buffer.from('hello world')

  const parent = crypto.parent({
    index: 0,
    size: 11,
    hash: crypto.data(data)
  }, {
    index: 2,
    size: 11,
    hash: crypto.data(data)
  })

  t.same(parent, Buffer.from('43563406adba8b34b133fdca32d0a458c5be769615e01df30e6535ccd3c075f0', 'hex'))
  t.end()
})

tape('capabilities', function (t) {
  const key = Buffer.alloc(32).fill('secret')
  const local = { rx: Buffer.alloc(32).fill('rx'), tx: Buffer.alloc(32).fill('tx') }
  const remote = { rx: local.tx, tx: local.rx }

  const cap = crypto.capability(key, local)
  const remoteCap = crypto.remoteCapability(key, remote)

  t.same(cap, remoteCap)
  t.end()
})

tape('tree', function (t) {
  const roots = [
    { index: 3, size: 11, hash: Buffer.alloc(32) },
    { index: 9, size: 2, hash: Buffer.alloc(32) }
  ]

  t.same(crypto.tree(roots), Buffer.from('334dd9d8f9a48c7b7e60affa8704a3597f87fe645fe83f1aada3a1216ea91e65', 'hex'))
  t.same(crypto.signable(Buffer.from('334dd9d8f9a48c7b7e60affa8704a3597f87fe645fe83f1aada3a1216ea91e65', 'hex'), 6), Buffer.from('334dd9d8f9a48c7b7e60affa8704a3597f87fe645fe83f1aada3a1216ea91e650000000000000006', 'hex'))
  t.same(crypto.signable(roots, 6), Buffer.from('334dd9d8f9a48c7b7e60affa8704a3597f87fe645fe83f1aada3a1216ea91e650000000000000006', 'hex'))
  t.end()
})
