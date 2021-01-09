var noise = require('..')
var test = require('tape')

test('Static key pattern without static keypair', function (assert) {
  assert.throws(_ => noise.initialize('XX', true, Buffer.alloc(0)))
  assert.end()
})
