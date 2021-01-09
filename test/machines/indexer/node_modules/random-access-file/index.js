var inherits = require('util').inherits
var RandomAccess = require('random-access-storage')
var fs = require('fs')
var mkdirp = require('mkdirp-classic')
var path = require('path')
var constants = fs.constants || require('constants')

var READONLY = constants.O_RDONLY
var READWRITE = constants.O_RDWR | constants.O_CREAT

module.exports = RandomAccessFile

function RandomAccessFile (filename, opts) {
  if (!(this instanceof RandomAccessFile)) return new RandomAccessFile(filename, opts)
  RandomAccess.call(this)

  if (!opts) opts = {}
  if (opts.directory) filename = path.join(opts.directory, path.resolve('/', filename).replace(/^\w+:\\/, ''))

  this.directory = opts.directory || null
  this.filename = filename
  this.fd = 0

  // makes random-access-storage open in writable mode first
  if (opts.writable || opts.truncate) this.preferReadonly = false

  this._size = opts.size || opts.length || 0
  this._truncate = !!opts.truncate || this._size > 0
  this._rmdir = !!opts.rmdir
  this._lock = opts.lock || noLock
}

inherits(RandomAccessFile, RandomAccess)

RandomAccessFile.prototype._open = function (req) {
  var self = this

  mkdirp(path.dirname(this.filename), ondir)

  function ondir (err) {
    if (err) return req.callback(err)
    open(self, READWRITE, req)
  }
}

RandomAccessFile.prototype._openReadonly = function (req) {
  open(this, READONLY, req)
}

RandomAccessFile.prototype._write = function (req) {
  var data = req.data
  var fd = this.fd

  fs.write(fd, data, 0, req.size, req.offset, onwrite)

  function onwrite (err, wrote) {
    if (err) return req.callback(err)

    req.size -= wrote
    req.offset += wrote

    if (!req.size) return req.callback(null)
    fs.write(fd, data, data.length - req.size, req.size, req.offset, onwrite)
  }
}

RandomAccessFile.prototype._read = function (req) {
  var data = req.data || Buffer.allocUnsafe(req.size)
  var fd = this.fd

  if (!req.size) return process.nextTick(readEmpty, req)
  fs.read(fd, data, 0, req.size, req.offset, onread)

  function onread (err, read) {
    if (err) return req.callback(err)
    if (!read) return req.callback(new Error('Could not satisfy length'))

    req.size -= read
    req.offset += read

    if (!req.size) return req.callback(null, data)
    fs.read(fd, data, data.length - req.size, req.size, req.offset, onread)
  }
}

RandomAccessFile.prototype._del = function (req) {
  var fd = this.fd

  fs.fstat(fd, onstat)

  function onstat (err, st) {
    if (err) return req.callback(err)
    if (req.offset + req.size < st.size) return req.callback(null)
    fs.ftruncate(fd, req.offset, ontruncate)
  }

  function ontruncate (err) {
    req.callback(err)
  }
}

RandomAccessFile.prototype._stat = function (req) {
  fs.fstat(this.fd, onstat)

  function onstat (err, st) {
    req.callback(err, st)
  }
}

RandomAccessFile.prototype._close = function (req) {
  var self = this

  fs.close(this.fd, onclose)

  function onclose (err) {
    if (err) return req.callback(err)
    self.fd = 0
    req.callback(null)
  }
}

RandomAccessFile.prototype._destroy = function (req) {
  var self = this

  var root = this.directory && path.resolve(path.join(this.directory, '.'))
  var dir = path.resolve(path.dirname(this.filename))

  fs.unlink(this.filename, onunlink)

  function onunlink (err) {
    if (!self._rmdir || !root || dir === root) return req.callback(err)
    fs.rmdir(dir, onrmdir)
  }

  function onrmdir (err) {
    dir = path.join(dir, '..')
    if (err || dir === root) return req.callback(null)
    fs.rmdir(dir, onrmdir)
  }
}

function open (self, mode, req) {
  if (self.fd) fs.close(self.fd, oncloseold)
  else fs.open(self.filename, mode, onopen)

  function onopen (err, fd) {
    if (err) return req.callback(err)
    self.fd = fd
    if (!self._lock(self.fd)) return req.callback(createLockError(self.filename)) // TODO: fix fd leak here
    if (!self._truncate || mode === READONLY) return req.callback(null)
    fs.ftruncate(self.fd, self._size, ontruncate)
  }

  function oncloseold (err) {
    if (err) return onerrorafteropen(err)
    self.fd = 0
    fs.open(self.filename, mode, onopen)
  }

  function ontruncate (err) {
    if (err) return onerrorafteropen(err)
    req.callback(null)
  }

  function onerrorafteropen (err) {
    fs.close(self.fd, function () {
      self.fd = 0
      req.callback(err)
    })
  }
}

function readEmpty (req) {
  req.callback(null, Buffer.alloc(0))
}

function noLock (fd) {
  return true
}

function createLockError (path) {
  var err = new Error('ELOCKED: File is locked')
  err.code = 'ELOCKED'
  err.path = path
  return err
}
