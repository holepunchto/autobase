var raf = require('./')
var tape = require('tape')
var os = require('os')
var path = require('path')
var fs = require('fs')
var mkdirp = require('mkdirp')
var isWin = process.platform === 'win32'

var tmp = path.join(os.tmpdir(), 'random-access-file-' + process.pid + '-' + Date.now())
var i = 0

mkdirp.sync(tmp)

tape('write and read', function (t) {
  var file = raf(gen())

  file.write(0, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    file.read(0, 5, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hello'))
      file.destroy(() => t.end())
    })
  })
})

tape('read empty', function (t) {
  var file = raf(gen(), {writable: true})

  file.read(0, 0, function (err, buf) {
    t.error(err, 'no error')
    t.same(buf, Buffer.alloc(0), 'empty buffer')
    file.destroy(() => t.end())
  })
})

tape('read range > file', function (t) {
  var file = raf(gen())

  file.read(0, 5, function (err, buf) {
    t.ok(err, 'not satisfiable')
    file.destroy(() => t.end())
  })
})

tape('read range > file with data', function (t) {
  var file = raf(gen())

  file.write(0, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    file.read(0, 10, function (err, buf) {
      t.ok(err, 'not satisfiable')
      file.destroy(() => t.end())
    })
  })
})

tape('random access write and read', function (t) {
  var file = raf(gen())

  file.write(10, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    file.write(0, Buffer.from('hello'), function (err) {
      t.error(err, 'no error')
      file.read(10, 2, function (err, buf) {
        t.error(err, 'no error')
        t.same(buf, Buffer.from('hi'))
        file.read(0, 5, function (err, buf) {
          t.error(err, 'no error')
          t.same(buf, Buffer.from('hello'))
          file.read(5, 5, function (err, buf) {
            t.error(err, 'no error')
            t.same(buf, Buffer.from([0, 0, 0, 0, 0]))
            t.end()
          })
        })
      })
    })
  })
})

tape('re-open', function (t) {
  var name = gen()
  var file = raf(name)

  file.write(10, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    var file2 = raf(name)
    file2.read(10, 5, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hello'))
      t.end()
    })
  })
})

tape('re-open and truncate', function (t) {
  var name = gen()
  var file = raf(name)

  file.write(10, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    var file2 = raf(name, {truncate: true})
    file2.read(10, 5, function (err, buf) {
      t.ok(err, 'file should be truncated')
      t.end()
    })
  })
})

tape('truncate with size', function (t) {
  var file = raf(gen(), {size: 100, writable: true})

  file.stat(function (err, st) {
    t.error(err, 'no error')
    t.same(st.size, 100)
    file.destroy(() => t.end())
  })
})

tape('bad open', function (t) {
  if (isWin) return t.end() // windows apparently allow you to open dirs :/

  var file = raf(tmp, {writable: true})

  file.open(function (err) {
    t.ok(err)
    file.close(() => t.end())
  })
})

tape('mkdir path', function (t) {
  var name = path.join(tmp, ++i + '-folder', 'test.txt')
  var file = raf(name)

  file.write(0, Buffer.from('hello'), function (err) {
    t.error(err, 'no error')
    file.read(0, 5, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hello'))
      t.end()
      file.destroy()
    })
  })
})

tape('write/read big chunks', function (t) {
  var file = raf(gen())
  var bigBuffer = Buffer.alloc(10 * 1024 * 1024)
  var missing = 2

  bigBuffer.fill('hey. hey. how are you doing?. i am good thanks how about you? i am good')

  file.write(0, bigBuffer, function (err) {
    t.error(err, 'no error')
    file.read(0, bigBuffer.length, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, bigBuffer)
      done()
    })
  })
  file.write(bigBuffer.length * 2, bigBuffer, function (err) {
    t.error(err, 'no error')
    file.read(bigBuffer.length * 2, bigBuffer.length, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, bigBuffer)
      done()
    })
  })

  function done () {
    if (!--missing) file.destroy(() => t.end())
  }
})

tape('rmdir option', function (t) {
  var name = path.join('rmdir', ++i + '', 'folder', 'test.txt')
  var file = raf(name, {rmdir: true, directory: tmp})

  file.write(0, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    file.read(0, 2, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hi'))
      file.destroy(ondestroy)
    })
  })

  function ondestroy (err) {
    t.error(err, 'no error')
    fs.stat(path.join(tmp, 'rmdir'), function (err) {
      t.same(err && err.code, 'ENOENT', 'should be removed')
      t.end()
    })
  }
})

tape('rmdir option with non empty parent', function (t) {
  var name = path.join('rmdir', ++i + '', 'folder', 'test.txt')
  var nonEmpty = path.join(tmp, name, '../..')
  var file = raf(name, {rmdir: true, directory: tmp})

  file.write(0, Buffer.from('hi'), function (err) {
    t.error(err, 'no error')
    fs.writeFileSync(path.join(nonEmpty, 'thing'), '')
    file.read(0, 2, function (err, buf) {
      t.error(err, 'no error')
      t.same(buf, Buffer.from('hi'))
      file.destroy(ondestroy)
    })
  })

  function ondestroy (err) {
    t.error(err, 'no error')
    fs.stat(path.join(tmp, 'rmdir'), function (err) {
      t.error(err, 'should not be removed')
      fs.readdir(nonEmpty, function (err, list) {
        t.error(err, 'no error')
        t.same(list, ['thing'], 'should only be one entry')
        t.end()
      })
    })
  }
})

tape('del', function (t) {
  var file = raf(gen())

  file.write(0, Buffer.alloc(100), function (err) {
    t.error(err, 'no error')
    file.stat(function (err, st) {
      t.error(err, 'no error')
      t.same(st.size, 100)
      file.del(0, 40, function (err) {
        t.error(err, 'no error')
        file.stat(function (err, st) {
          t.error(err, 'no error')
          t.same(st.size, 100, 'inplace del, same size')
          file.del(50, 50, function (err) {
            t.error(err, 'no error')
            file.stat(function (err, st) {
              t.error(err, 'no error')
              t.same(st.size, 50)
              file.destroy(() => t.end())
            })
          })
        })
      })
    })
  })
})

tape('open and close many times', function (t) {
  var name = gen()
  var file = raf(name)
  var buf = Buffer.alloc(4)

  file.write(0, buf, function (err) {
    t.error(err, 'no error')
    loop(5000, function (err) {
      t.error(err, 'no error')
      file.destroy(() => t.end())
    })
  })

  function loop (n, cb) {
    var file = raf(name)
    file.read(0, 4, function (err, buffer) {
      if (err) return cb(err)
      if (!buf.equals(buffer)) {
        t.same(buffer, buf)
        return cb()
      }
      buf.writeUInt32BE(n)
      file.write(0, buf, function (err) {
        if (err) return cb(err)
        file.close(function (err) {
          if (!n || err) return cb(err)
          loop(n - 1, cb)
        })
      })
    })
  }
})

tape('trigger bad open', function (t) {
  var file = raf(gen(), {writable: true})

  file.fd = 10000
  file.open(function (err) {
    t.ok(err, 'should error trying to close old fd')
    file.open(function (err) {
      t.error(err, 'no error')
      file.destroy(() => t.end())
    })
  })
})

tape('trigger lock', function (t) {
  var p = gen()
  var file = raf(p, {writable: true, lock: () => false})

  file.open(function (err) {
    t.ok(err, 'should error because it is locked')
    t.same(err.message, 'ELOCKED: File is locked')
    t.same(err.path, p)
    t.same(err.code, 'ELOCKED')
    t.end()
  })
})

tape('cannot escape directory', function (t) {
  var name = '../../../../../../../../../../../../../tmp'
  var file = raf(name, {writable: true, directory: tmp})

  file.open(function (err) {
    t.error(err, 'no error')
    t.same(file.filename, path.join(tmp, 'tmp'))
    t.end()
  })
})

tape('directory filename resolves correctly', function (t) {
  var name = 'test.txt'
  var file = raf(name, {writable: true, directory: tmp})
  t.same(file.filename, path.join(tmp, name))
  t.end()
})

function gen () {
  return path.join(tmp, ++i + '.txt')
}
