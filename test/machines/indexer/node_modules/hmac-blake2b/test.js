var hmac = require('.')
var test = require('tape')

test('hmac 1', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b', 'hex')

  var data = Buffer.from('4869205468657265', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('358a6a184924894fc34bee5680eedf57d84a37bb38832f288e3b27dc63a98cc8c91e76da476b508bc6b2d408a248857452906e4a20b48c6b4b55d2df0fe1dd24', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 2', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('4a656665', 'hex')

  var data = Buffer.from('7768617420646f2079612077616e7420666f72206e6f7468696e673f', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('6ff884f8ddc2a6586b3c98a4cd6ebdf14ec10204b6710073eb5865ade37a2643b8807c1335d107ecdb9ffeaeb6828c4625ba172c66379efcd222c2de11727ab4', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 3', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex')

  var data = Buffer.from('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('f43bc62c7a99353c3b2c60e8ef24fbbd42e9547866dc9c5be4edc6f4a7d4bc0ac620c2c60034d040f0dbaf86f9e9cd7891a095595eed55e2a996215f0c15c018', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 4', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('0102030405060708090a0b0c0d0e0f10111213141516171819', 'hex')

  var data = Buffer.from('cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('e5dbb6de2fee42a1caa06e4e7b84ce408ffa5c4a9de2632eca769cde8875014c72d0720feaf53f76e6a180357f528d7bf484fa3a14e8cc1f0f3bada717b43491', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 5', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c', 'hex')

  var data = Buffer.from('546573742057697468205472756e636174696f6e', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('7d03e0d2ad83656e5ace6aa9ddf6407a', 'hex')
  assert.ok(expected.equals(mac.slice(0, 128 / 8)))
  assert.end()
})

test('hmac 6', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex')

  var data = Buffer.from('54657374205573696e67204c6172676572205468616e20426c6f636b2d53697a65204b6579202d2048617368204b6579204669727374', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('a54b2943b2a20227d41ca46c0945af09bc1faefb2f49894c23aebc557fb79c4889dca74408dc865086667aedee4a3185c53a49c80b814c4c5813ea0c8b38a8f8', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})


test('hmac 7', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex')

  var data = Buffer.from('5468697320697320612074657374207573696e672061206c6172676572207468616e20626c6f636b2d73697a65206b657920616e642061206c6172676572207468616e20626c6f636b2d73697a6520646174612e20546865206b6579206e6565647320746f20626520686173686564206265666f7265206265696e6720757365642062792074686520484d414320616c676f726974686d2e', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('ab347980a64b5e825dd10e7d32fd43a01a8e6dea267ab9ad7d913524526618925311afbcb0c49519cbebdd709540a8d725fb911ac2aee9b2a3aa43d796123393', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})
