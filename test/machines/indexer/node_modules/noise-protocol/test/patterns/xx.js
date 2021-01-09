var noise = require('../..')
var test = require('tape')

test('XX pattern', function (assert) {
  var client = noise.initialize('XX', true, Buffer.alloc(0), noise.keygen())
  var server = noise.initialize('XX', false, Buffer.alloc(0), noise.keygen())

  var clientTx = Buffer.alloc(512)
  var serverRx = Buffer.alloc(512)

  var serverTx = Buffer.alloc(512)
  var clientRx = Buffer.alloc(512)

  // ->
  assert.false(noise.writeMessage(client, Buffer.alloc(0), clientTx))
  assert.ok(noise.writeMessage.bytes > 0)
  assert.false(noise.readMessage(server, clientTx.subarray(0, noise.writeMessage.bytes), serverRx))
  assert.equal(noise.readMessage.bytes, 0)

  // <-
  assert.false(noise.writeMessage(server, Buffer.alloc(0), serverTx))
  assert.ok(noise.writeMessage.bytes > 0)
  assert.false(noise.readMessage(client, serverTx.subarray(0, noise.writeMessage.bytes), clientRx))
  assert.equal(noise.readMessage.bytes, 0)

  // ->
  var splitClient = noise.writeMessage(client, Buffer.alloc(0), clientTx)
  assert.ok(noise.writeMessage.bytes > 0)
  var splitServer = noise.readMessage(server, clientTx.subarray(0, noise.writeMessage.bytes), serverRx)
  assert.equal(noise.readMessage.bytes, 0)

  assert.same(splitClient.tx, splitServer.rx)
  assert.same(splitClient.rx, splitServer.tx)

  assert.end()
})
