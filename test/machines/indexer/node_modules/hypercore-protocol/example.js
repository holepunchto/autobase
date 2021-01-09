const Protocol = require('./')

const a = new Protocol(true, {
  onauthenticate (remotePublicKey, done) {
    console.log('verifying the public key of b', remotePublicKey)
    done(null)
  },
  onhandshake () {
    console.log('onhandshake()')
  }
})

const b = new Protocol(false, {
  onauthenticate (remotePublicKey, done) {
    console.log('verifying the public key of a', remotePublicKey)
    done(null)
  }
})

a.pipe(b).pipe(a)

a.on('close', () => console.log('a closed', a))
b.on('close', () => console.log('b closed', b))

const key = Buffer.from('This is a 32 byte key, 012345678')
let missing = 5

const channel = a.open(key, {
  onhave (have) {
    console.log('channel.onhave()', have)

    for (var i = 0; i < 5; i++) {
      channel.request({
        index: i
      })
    }
  },
  ondata (data) {
    console.log('channel.ondata()', data)

    if (!--missing) {
      channel.status({
        uploading: false,
        download: false
      })
    }
  }
})

const remoteChannel = b.open(key, {
  onrequest (request) {
    console.log('remoteChannel.onrequest()', request)
    remoteChannel.data({
      index: request.index,
      value: 'sup'
    })
  },
  onwant (want) {
    console.log('remoteChannel.onwant()', want)
    remoteChannel.have({
      start: 0,
      length: 1000
    })
  },
  onstatus (status) {
    console.log('remoteChannel.onstatus', status)
    remoteChannel.close()
  }
})

channel.want({
  start: 0,
  length: 1000
})

console.log('a:')
console.log(a)
console.log('b:')
console.log(b)
