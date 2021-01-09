var sodium = require('sodium-universal')
var hmac = require('.')
var test = require('tape')

test('hmac 1', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b', 'hex')

  var data = Buffer.from('4869205468657265', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('87aa7cdea5ef619d4ff0b4241a1d6cb02379f4e2ce4ec2787ad0b30545e17cdedaa833b7d6b8a702038b274eaea3f4e4be9d914eeb61f1702e696c203a126854', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 2', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('4a656665', 'hex')

  var data = Buffer.from('7768617420646f2079612077616e7420666f72206e6f7468696e673f', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea2505549758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 3', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex')

  var data = Buffer.from('dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('fa73b0089d56a284efb0f0756c890be9b1b5dbdd8ee81a3655f83e33b2279d39bf3e848279a722c806b485a47e67c807b946a337bee8942674278859e13292fb', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 4', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('0102030405060708090a0b0c0d0e0f10111213141516171819', 'hex')

  var data = Buffer.from('cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('b0ba465637458c6990e5a8c5f61d4af7e576d97ff94b872de76f8050361ee3dba91ca5c11aa25eb4d679275cc5788063a5f19741120c4f2de2adebeb10a298dd', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})

test('hmac 5', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c', 'hex')

  var data = Buffer.from('546573742057697468205472756e636174696f6e', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('415fad6271580a531d4179bc891d87a6', 'hex')
  assert.ok(expected.equals(mac.slice(0, 128 / 8)))
  assert.end()
})

test('hmac 6', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex')

  var data = Buffer.from('54657374205573696e67204c6172676572205468616e20426c6f636b2d53697a65204b6579202d2048617368204b6579204669727374', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('80b24263c7c1a3ebb71493c1dd7be8b49b46d1f41b4aeec1121b013783f8f3526b56d037e05f2598bd0fd2215d6a1e5295e64f73f63f0aec8b915a985d786598', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})


test('hmac 7', function (assert) {
  var mac = Buffer.alloc(hmac.BYTES)
  var key = Buffer.from('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'hex')

  var data = Buffer.from('5468697320697320612074657374207573696e672061206c6172676572207468616e20626c6f636b2d73697a65206b657920616e642061206c6172676572207468616e20626c6f636b2d73697a6520646174612e20546865206b6579206e6565647320746f20626520686173686564206265666f7265206265696e6720757365642062792074686520484d414320616c676f726974686d2e', 'hex')

  hmac(mac, data, key)

  var expected = Buffer.from('e37b6a775dc87dbaa4dfa9f96e5e3ffddebd71f8867289865df5a32d20cdc944b6022cac3c4982b10d5eeb55c3e4de15134676fb6de0446065c97440fa8c6a58', 'hex')
  assert.ok(expected.equals(mac))
  assert.end()
})
