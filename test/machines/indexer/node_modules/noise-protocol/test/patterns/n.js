var noise = require('../..')
var test = require('tape')

test('N pattern', function (assert) {
  var serverKeys = noise.keygen()

  var client = noise.initialize('N', true, Buffer.alloc(0), null, null, serverKeys.publicKey)
  var server = noise.initialize('N', false, Buffer.alloc(0), serverKeys)

  var clientTx = Buffer.alloc(512)
  var serverRx = Buffer.alloc(512)

  var splitClient = noise.writeMessage(client, Buffer.from('Hello world'), clientTx)
  assert.ok(noise.writeMessage.bytes > 11)
  assert.false(Buffer.from(clientTx).includes(Buffer.from('Hello world')))
  assert.false(Buffer.from(clientTx).includes(Buffer.from(client.rs)))
  assert.false(Buffer.from(clientTx).includes(Buffer.from(client.esk)))
  var splitServer = noise.readMessage(server, clientTx.subarray(0, noise.writeMessage.bytes), serverRx)
  assert.equal(noise.readMessage.bytes, 11)

  assert.same(splitClient.tx, splitServer.rx)
  assert.same(splitClient.rx, splitServer.tx)
  assert.notSame(splitServer.rx, splitServer.tx)
  assert.notSame(splitClient.rx, splitClient.tx)

  assert.end()
})
