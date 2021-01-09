var hash = require('../hash')
var test = require('tape')

test('constants', function (assert) {
  assert.ok(hash.HASHLEN === 32 || hash.HASHLEN === 64, 'HASHLEN conforms to Noise Protocol')
  assert.ok(hash.BLOCKLEN > 0, 'BLOCKLEN conforms to Noise Protocol')
  assert.end()
})

test.skip('hash')
test.skip('hkdf')
