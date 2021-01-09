var MerkleTreeStream = require('./')
var crypto = require('crypto')

var stream = new MerkleTreeStream({
  leaf: function (leaf, roots) {
    return crypto.createHash('sha256').update(leaf.data).digest()
  },
  parent: function (a, b) {
    return crypto.createHash('sha256').update(a.hash).update(b.hash).digest()
  }
})

stream.write('hello')
stream.write('hashed')
stream.write('world')

stream.on('data', function (data) {
  console.log(data)
})
