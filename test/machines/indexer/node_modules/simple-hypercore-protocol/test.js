const tape = require('tape')
const SHP = require('./')

const key = Buffer.alloc(32).fill('key')
const discoveryKey = Buffer.alloc(32).fill('discovery')

tape('single open', function (t) {
  const a = new SHP(true, {
    send (data) {
      b.recv(data)
    }
  })

  const b = new SHP(false, {
    onopen (ch, message) {
      t.same(ch, 0)
      t.same(message.discoveryKey, discoveryKey)
      t.notOk(message.key)
      t.end()
    },
    send (data) {
      a.recv(data)
    }
  })

  a.open(0, { key, discoveryKey })
})

tape('single close', function (t) {
  const a = new SHP(true, {
    send (data) {
      b.recv(data)
    }
  })

  const b = new SHP(false, {
    onclose (ch, message) {
      t.same(ch, 0)
      t.same(message.discoveryKey, discoveryKey)
      t.end()
    },
    send (data) {
      a.recv(data)
    }
  })

  a.close(0, { discoveryKey })
})

tape('back and fourth', function (t) {
  const a = new SHP(true, {
    ondata (ch, message) {
      t.same(ch, 0)
      t.same(message, {
        index: 42,
        value: Buffer.from('data'),
        nodes: [],
        signature: null
      })
      a.close(ch)
    },
    send (data) {
      process.nextTick(() => b.recv(data))
    }
  })

  const b = new SHP(false, {
    onopen (ch, message) {
      t.same(ch, 0)
      t.same(message.discoveryKey, discoveryKey)
      t.notOk(message.key)
    },
    onrequest (ch, message) {
      t.same(ch, 0)
      t.same(message, { index: 42, bytes: 0, hash: false, nodes: 0 })
      b.data(0, {
        index: 42,
        value: Buffer.from('data')
      })
    },
    onclose (ch) {
      t.same(ch, 0)
      t.end()
    },
    send (data) {
      process.nextTick(() => a.recv(data))
    }
  })

  a.open(0, { key, discoveryKey })
  a.request(0, {
    index: 42
  })
})

tape('various messages', function (t) {
  t.plan(9)

  const a = new SHP(true, {
    send (data) {
      process.nextTick(() => b.recv(data))
    }
  })

  const b = new SHP(false, {
    onhandshake () {
      t.pass('handshook')
    },
    onextension (ch, id, data) {
      t.same(ch, 0)
      t.same(id, 42)
      t.same(data, Buffer.from('binary!'))
    },
    onhave (ch, message) {
      t.same(ch, 0)
      t.same(message.start, 42)
      t.same(message.length, 10)
    },
    onoptions (ch, message) {
      t.same(ch, 0)
      t.same(message.ack, true)
    },
    send (data) {
      process.nextTick(() => a.recv(data))
    }
  })

  a.extension(0, 42, Buffer.from('binary!'))
  a.options(0, { ack: true })
  a.have(0, { start: 42, length: 10 })
})

tape('auth', function (t) {
  t.plan(4)

  const a = new SHP(true, {
    onauthenticate (remotePublicKey, done) {
      t.pass('authenticated b')
      if (remotePublicKey.equals(b.publicKey)) return done(null)
      t.fail('bad public key')
      done(new Error('Nope'))
    },
    onhandshake () {
      t.pass('handshook b')
    },
    send (data) {
      process.nextTick(() => b.recv(data))
    }
  })

  const b = new SHP(false, {
    onauthenticate (remotePublicKey, done) {
      t.pass('authenticated a')
      if (remotePublicKey.equals(a.publicKey)) return done(null)
      t.fail('bad public key')
      done(new Error('Nope'))
    },
    onhandshake () {
      t.pass('handshook a')
    },
    send (data) {
      process.nextTick(() => a.recv(data))
    }
  })
})

tape('send ping', function (t) {
  let pinging = false
  let sent = 0

  const a = new SHP(true, {
    send (data) {
      if (pinging) sent++
      b.recv(data)
    },
    onhandshake () {
      process.nextTick(function () {
        pinging = true
        for (let i = 0; i < 100; i++) a.ping()
        t.same(sent, 100, 'sent a hundred pings')
        t.end()
      })
    }
  })

  const b = new SHP(false, {
    send (data) {
      a.recv(data)
    }
  })

  a.ping() // should not fail
})

tape('set key pair later', function (t) {
  let later = null

  const a = new SHP(false, {
    keyPair (done) {
      setImmediate(function () {
        later = SHP.keyPair()
        done(null, later)
      })
    },
    send (data) {
      b.recv(data)
    }
  })

  const b = new SHP(true, {
    send (data) {
      a.recv(data)
    },
    onauthenticate (remotePublicKey, done) {
      t.same(remotePublicKey, later.publicKey)
      t.same(a.publicKey, later.publicKey)
      done()
    },
    onhandshake () {
      t.end()
    }
  })
})

tape('disable noise', function (t) {
  const a = new SHP(true, {
    noise: false,
    encrypted: false,
    onhandshake () {
      t.fail('onhandshake may not be called with noise: false')
    },
    send (data) {
      b.recv(data)
    }
  })

  const b = new SHP(false, {
    noise: false,
    encrypted: false,
    onhandshake () {
      t.fail('onhandshake may not be called with noise: false')
    },
    onopen (ch, message) {
      t.same(ch, 0)
      t.same(message.discoveryKey, discoveryKey)
      t.notOk(message.key)
      t.end()
    },
    send (data) {
      a.recv(data)
    }
  })

  a.open(0, { key, discoveryKey })
})

tape('handshakeHash', function (t) {
  t.plan(3)

  var pending = 3
  const a = new SHP(true, {
    onhandshake () {
      if (--pending === 0) process.nextTick(check)
    },
    send (data) {
      process.nextTick(() => b.recv(data))
    }
  })

  const b = new SHP(false, {
    onhandshake () {
      if (--pending === 0) process.nextTick(check)
    },
    send (data) {
      process.nextTick(() => a.recv(data))
    }
  })

  if (--pending === 0) check()
  function check () {
    t.ok(a.handshakeHash)
    t.ok(b.handshakeHash)
    t.deepEqual(a.handshakeHash, b.handshakeHash)
  }
})
