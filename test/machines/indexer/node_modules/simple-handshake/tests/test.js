const test = require('tape')
const sh = require('..')

test('keygen', function (assert) {
  const keys = sh.keygen()
  assert.ok(keys.publicKey, 'should have publicKey')
  assert.ok(keys.secretKey, 'should have secretKey')

  const seedKeys = sh.seedKeygen(Buffer.alloc(32, 'Hello world'))
  assert.ok(seedKeys.publicKey, 'should have publicKey')
  assert.ok(seedKeys.secretKey, 'should have secretKey')
  assert.same(seedKeys.publicKey, Buffer.from('0f2d1af1fd8e59ef017c475a9ceca9bb445e63834d5eb54c1190fa800681f152', 'hex'), 'should be stable publicKey')
  assert.same(seedKeys.secretKey, Buffer.from('f5228ae72dd36e8a2be120e8b70261bf4c3c3d1e09a176dab1c9e1831e4c6e62', 'hex'), 'should be stable secretKey')
  assert.end()
})

test('simple case', function (assert) {
  const initiator = sh(true)
  const responder = sh(false)

  rtt(initiator, responder)

  function rtt (from, to) {
    // waiting === true means waiting to receive data, hence it should be false
    // if we're ready to send data!
    if (from.waiting !== false) return assert.end(new Error('Not ready to send data'))

    from.send(null, function (err, buf) {
      if (err) return assert.end(err)

      to.recv(buf, function (err, msg) {
        if (err) return assert.end(err)

        // Keep going until from is finished
        if (from.finished === true) {
          assert.ok(from.finished, 'initiator should be finished')
          assert.ok(to.finished, 'responder should be finished')
          assert.ok(from.handshakeHash != null, 'initiator handshakeHash should be set')
          assert.ok(to.handshakeHash != null, 'responder handshakeHash should be set')
          assert.same(from.handshakeHash, to.handshakeHash, 'handshakeHashes should be equal')
          assert.same(from.split.tx, to.split.rx, 'splits should be symmetric')
          assert.same(from.split.rx, to.split.tx, 'splits should be symmetric')
          return assert.end()
        }

        // recurse until finished
        return rtt(to, from)
      })
    })
  }
})

test('handlers', function (assert) {
  var step = 0
  const initiator = sh(true, {
    onephemeralkey (key, cb) {
      assert.equal(step++, 2, 'initiator gets ephemeral key after responder has finished handshake')
      cb()
    },
    onhandshake (state, cb) {
      assert.equal(step++, 3, 'initiator finishes their handshake last')
      cb()
    },
    onstatickey (key, cb) {
      assert.error(new Error('Default handshake has no static keys'))
    }
  })

  const responder = sh(false, {
    onephemeralkey (key, cb) {
      assert.equal(step++, 0, 'responder gets ephemeral key first')
      cb()
    },
    onhandshake (state, cb) {
      assert.equal(step++, 1, 'responder can finish handshake immediately')
      cb()
    },
    onstatickey (key, cb) {
      assert.error(new Error('Default handshake has no static keys'))
    }
  })

  rtt(initiator, responder)

  function rtt (from, to) {
    // waiting === true means waiting to receive data, hence it should be false
    // if we're ready to send data!
    if (from.waiting !== false) return assert.end(new Error('Not ready to send data'))

    from.send(null, function (err, buf) {
      if (err) return assert.end(err)

      to.recv(buf, function (err, msg) {
        if (err) return assert.end(err)

        // Keep going until from is finished
        if (from.finished === true) {
          assert.ok(from.finished, 'initiator should be finished')
          assert.ok(to.finished, 'responder should be finished')
          assert.ok(from.handshakeHash != null, 'initiator handshakeHash should be set')
          assert.ok(to.handshakeHash != null, 'responder handshakeHash should be set')
          assert.same(from.handshakeHash, to.handshakeHash, 'handshakeHashes should be equal')
          assert.same(from.split.tx, to.split.rx, 'splits should be symmetric')
          assert.same(from.split.rx, to.split.tx, 'splits should be symmetric')
          return assert.end()
        }

        // recurse until finished
        return rtt(to, from)
      })
    })
  }
})

test.only('handlers with static keys', function (assert) {
  var step = 0

  var iKey = sh.keygen()
  var rKey = sh.keygen()
  const initiator = sh(true, {
    pattern: 'XX',
    staticKeyPair: iKey,
    onephemeralkey (key, cb) {
      assert.equal(step++, 1, 'initiator gets ephemeral key after first message')
      cb()
    },
    onstatickey (key, cb) {
      assert.equal(step++, 2, 'initiator gets static key with ephemeral key')
      assert.same(key, rKey.publicKey)
      cb()
    },
    onhandshake (state, cb) {
      assert.equal(step++, 3, 'initiator finishes their handshake first')
      cb()
    }
  })

  const responder = sh(false, {
    pattern: 'XX',
    staticKeyPair: rKey,
    onephemeralkey (key, cb) {
      assert.equal(step++, 0, 'responder gets ephemeral key first')
      cb()
    },
    onstatickey (key, cb) {
      assert.equal(step++, 4, 'responder gets static key last')
      assert.same(key, iKey.publicKey)
      cb()
    },
    onhandshake (state, cb) {
      assert.equal(step++, 5, 'responder can finish handshake finally')
      cb()
    }
  })

  rtt(initiator, responder)

  function rtt (from, to) {
    // waiting === true means waiting to receive data, hence it should be false
    // if we're ready to send data!
    if (from.waiting !== false) return assert.end(new Error('Not ready to send data'))

    from.send(null, function (err, buf) {
      if (err) return assert.end(err)

      to.recv(buf, function (err, msg) {
        if (err) return assert.end(err)

        // Keep going until from is finished
        if (from.finished === true) {
          assert.ok(from.finished, 'initiator should be finished')
          assert.ok(to.finished, 'responder should be finished')
          assert.ok(from.handshakeHash != null, 'initiator handshakeHash should be set')
          assert.ok(to.handshakeHash != null, 'responder handshakeHash should be set')
          assert.same(from.handshakeHash, to.handshakeHash, 'handshakeHashes should be equal')
          assert.same(from.split.tx, to.split.rx, 'splits should be symmetric')
          assert.same(from.split.rx, to.split.tx, 'splits should be symmetric')
          return assert.end()
        }

        // recurse until finished
        return rtt(to, from)
      })
    })
  }
})
