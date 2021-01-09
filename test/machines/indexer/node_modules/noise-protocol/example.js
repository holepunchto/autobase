var noise = require('.')

var sClient = noise.keygen()
var sServer = noise.keygen()

var client = noise.initialize('KK', true, Buffer.alloc(0), sClient, null, sServer.publicKey)
var server = noise.initialize('KK', false, Buffer.alloc(0), sServer, null, sClient.publicKey)

var clientTx = Buffer.alloc(128)
var serverTx = Buffer.alloc(128)

var clientRx = Buffer.alloc(128)
var serverRx = Buffer.alloc(128)

// -> e, es, ss
noise.writeMessage(client, Buffer.alloc(0), clientTx)
noise.readMessage(server, clientTx.subarray(0, noise.writeMessage.bytes), serverRx)

// <- e, ee, se
var serverSplit = noise.writeMessage(server, Buffer.alloc(0), serverTx)
var clientSplit = noise.readMessage(client, serverTx.subarray(0, noise.writeMessage.bytes), clientRx)

noise.destroy(client)
noise.destroy(server)

// Can now do transport encryption with splits
console.log(serverSplit)
console.log(clientSplit)
