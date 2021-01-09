const nanoresource = require('./')
const fs = require('fs')

class FileSize extends nanoresource {
  constructor (name) {
    super()
    this.filename = name
    this.fd = 0
  }

  _open (cb) {
    console.log('Now opening file ...')
    fs.open(this.filename, 'r', (err, fd) => {
      if (err) return cb(err)
      this.fd = fd
      cb(null)
    })
  }

  _close (cb) {
    console.log('Now closing file ...')
    fs.close(this.fd, cb)
  }

  size (cb) {
    this.open((err) => {
      if (err) return cb(err)
      if (!this.active(cb)) return
      fs.fstat(this.fd, (err, st) => {
        if (err) return this.inactive(cb, err)
        this.inactive(cb, null, st.size)
      })
    })
  }
}

const f = new FileSize('index.js')

f.size((err, size) => {
  if (err) throw err
  console.log('size is:', size)
})

// size a couple of times
f.size((err, size) => {
  if (err) throw err
  console.log('size is:', size)
})

// after a bit when we are done with the resource we close it ...
setTimeout(() => f.close(), 1000)
