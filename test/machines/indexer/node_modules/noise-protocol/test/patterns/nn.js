var noise = require('../..')
var test = require('tape')

test('NN pattern', function (assert) {
  var client = noise.initialize('NN', true, Buffer.alloc(0))
  var server = noise.initialize('NN', false, Buffer.alloc(0))

  var clientTx = Buffer.alloc(512)
  var serverRx = Buffer.alloc(512)

  var serverTx = Buffer.alloc(512)
  var clientRx = Buffer.alloc(512)

  assert.false(noise.writeMessage(client, Buffer.alloc(0), clientTx))
  assert.ok(noise.writeMessage.bytes > 0)
  assert.false(noise.readMessage(server, clientTx.subarray(0, noise.writeMessage.bytes), serverRx))
  assert.equal(noise.readMessage.bytes, 0)

  var splitServer = noise.writeMessage(server, Buffer.alloc(0), serverTx)
  assert.ok(noise.writeMessage.bytes > 0)
  var splitClient = noise.readMessage(client, serverTx.subarray(0, noise.writeMessage.bytes), clientRx)
  assert.equal(noise.readMessage.bytes, 0)

  assert.same(splitClient.tx, splitServer.rx)
  assert.same(splitClient.rx, splitServer.tx)

  assert.end()
})
