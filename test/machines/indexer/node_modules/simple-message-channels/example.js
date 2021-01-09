const SMC = require('./')

const a = new SMC({
  onmessage (channel, type, message) {
    console.log(channel, type, message)
  }
})

const b = new SMC()

a.recv(b.sendBatch([
  { channel: 0, type: 1, message: Buffer.from('a') },
  { channel: 0, type: 1, message: Buffer.from('b') },
  { channel: 0, type: 1, message: Buffer.from('c') }
]))
