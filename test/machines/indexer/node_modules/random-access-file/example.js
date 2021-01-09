var raf = require('random-access-file')
var file = raf('hello.txt')

var max = 500 * 1024 * 1024
var buf = Buffer.alloc(1024)
buf.fill('lo')

var offset = 0
write()

function write () {
  file.write(offset, buf, afterWrite)
}

function afterWrite (err) {
  if (err) throw err
  if (offset >= max) return done()
  offset += buf.length
  write()
}

function done () {
  console.log('wrote hello.txt')
}
