var simpleHandshake = require('.')

// Generate static keys. Ideally these are saved as the represent either party's
// identity
var clientKeys = simpleHandshake.keygen()
var serverKeys = simpleHandshake.keygen()

// Make a client/server pair. These are also known as initiator/responder
var client = simpleHandshake(true, {
  pattern: 'XX',
  staticKeyPair: clientKeys,
  onstatickey: (key, ondone) => {
    console.log('static key', key)
    ondone(null)
  }
})

var server = simpleHandshake(false, {
  pattern: 'XX',
  staticKeyPair: serverKeys,
  onstatickey: (key, ondone) => {
    console.log('static key', key)
    ondone(null)
  }
})

// Use a simple Round trip function to do the back and forth.
// In practise this would go over a network
rtt(client, server, function (err) {
  if (err) throw err

  // Now both parties have arrived at the same shared secrets
  // client.tx === server.rx and server.tx === client.rx
  console.log(client.split, server.split)
})

function rtt (from, to, cb) {
  // waiting === true means waiting to receive data, hence it should be false
  // if we're ready to send data!
  if (from.waiting !== false) return cb(new Error('Not ready to send data'))

  from.send(null, function (err, buf) {
    if (err) return cb(err)

    to.recv(buf, function (err) {
      if (err) return cb(err)

      // Keep going until from is finished
      if (from.finished === true) return cb()

      // recurse until finished
      return rtt(to, from, cb)
    })
  })
}
