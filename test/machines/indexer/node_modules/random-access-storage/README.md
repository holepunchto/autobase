# random-access-storage

Easily make random-access-storage instances

```
npm install random-access-storage
```

[![build status](https://travis-ci.org/random-access-storage/random-access-storage.svg?branch=master)](https://travis-ci.org/random-access-storage/random-access-storage)

A random-access-storage instance is a common interface for a storage abstraction, that provides the following core api.

* `read(offset, size)` - Read a buffer at a custom offset.
* `write(offset, data)` - Write a buffer to a custom offset.
* `del(offset, size)` - Delete data at a custom offset.

This module exposes a base class that implements most of the plumbing and flow you'd usually want to implement when making one.

## Usage

``` js
var randomAccess = require('random-access-storage')
var fs = require('fs')

var file = fileReader('index.js')

file.read(0, 10, (err, buf) => console.log('0-10: ' + buf.toString()))
file.read(40, 15, (err, buf) => console.log('40-55: ' + buf.toString()))
file.close()

function fileReader (name) {
  var fd = 0
  return randomAccess({
    open: function (req) {
      // called once automatically before the first read call
      fs.open(name, 'r', function (err, res) {
        if (err) return req.callback(err)
        fd = res
        req.callback(null)
      })
    },
    read: function (req) {
      var buf = Buffer.allocUnsafe(req.size)
      fs.read(fd, buf, 0, buf.length, req.offset, function (err, read) {
        if (err) return req.callback(err)
        if (read < buf.length) return req.callback(new Error('Could not read'))
        req.callback(null, buf)
      })
    },
    close: function (req) {
      if (!fd) return req.callback(null)
      fs.close(fd, err => req.callback(err))
    }
  })
}
```

## API

#### `var storage = randomAccessStorage([options])`

Make a new instance. Options include:

``` js
{
  open: fn, // sets ._open
  openReadonly: fn, // sets ._openReadonly
  read: fn, // sets ._read
  write: fn, // sets ._write
  del: fn, // sets ._del
  stat: fn, // sets ._stat
  close: fn, // sets ._close
  destroy: fn // sets ._destroy
}
```

#### `storage.readable`

True if the storage implements `._read`.

#### `storage.writable`

True if the storage implements `._write`.

#### `storage.deletable`

True if the storage implements `._del`.

#### `storage.statable`

True if the storage implements `._stat`.

#### `storage.opened`

True if the storage has been fully opened.

#### `storage.closed`

True if the storage has been fully closed.

#### `storage.destroyed`

True is the destorage has been fully destroyed.

#### `storage.on('open')`

Emitted when the storage is fully open.

#### `storage.on('close')`

Emitted when the storage is fully closed.

#### `storage.on('destroy')`

Emitted when the storage is fully destroyed.

#### `storage.open(cb)`

Explicitly open the storage. If you do not call this yourself,
it will automatically called before any read/write/del/stat operation.

It is safe to call this more than once.

Triggers *one* call to `_open` if you implement that.

If you implement `_openReadonly` and the operation that triggers the open
is not a write/del then `_openReadonly` will be called instead.

If a write is later performed a `_open` call will be triggered as well,
expecting you to open the storage in read/write mode.

#### `storage._open(req)`

Implement storage open.

Call `req.callback` when it is fully opened.

#### `storage._openReadonly(req)`

Implement storage readonly open.

Useful if you prefer to open the storage in readonly mode unless a write is performed. Call `req.callback` when it is fully opened.

#### `storage.read(offset, size, callback)`

Read the specified bytes from the storage at the specified byte offset.
Calls the callback with a `(err, buffer)`.

#### `storage._read(req)`

Implement storage read.

* `req.offset` contains the byte offset you should read at.
* `req.size` contains the amount of bytes you should read.

Call `req.callback(err, buffer)` when the read is completed.

Note that this is guaranteed to run after the storage has been opened and not after it has been closed.

#### `storage.write(offset, buffer, [callback])`

Write the specified buffer to the specified byte offset. Optionally pass a callback that is called with `(err)` when the write has completed.

#### `storage._write(req)`

Implement storage write.

* `req.offset` contains the byte offset you should write at.
* `req.data` contains the buffer you should write.

Call `req.callback(err)` when the write is completed.

Note that this is guaranteed to run after the storage has been opened and not after it has been closed.

#### `storage.del(offset, size, [callback])`

Delete the specified amount of bytes as the specified offset. Optionally pass a callback that is called with `(err)` when the delete has completed.

#### `storage._del(req)`

Implement storage delete.

* `req.offset` contains the byte offset to delete at.
* `req.size` contains the amount of bytes to delete.

Call `req.callback(err)` when the delete has completed.

Note that this is guaranteed to run after the storage has been opened and not after it has been closed.

#### `storage.stat(callback)`

Stat the storage. Should return an object with useful information about the underlying storage, including:

```
{
  size: number // how many bytes of data is stored?
}
```

#### `storage._stat(req)`

Implement storage stat.

Call `req.callback(err, statObject)` when the stat has completed.

Note that this is guaranteed to run after the storage has been opened and not after it has been closed.

#### `storage.close([callback])`

Close the storage instance.

#### `storage._close(req)`

Implement storage close.

Call `req.callback(err)` when the storage is fully closed.

Note this is guaranteed to run after all pending read/write/stat/del operations has finished and no methods will run after.

#### `storage.destroy([callback])`

Destroy the storage instance, removing all underlying data.

#### `storage._destroy(req)`

Implement storage destroy.

Call `req.callback(err)` when the storage has been fully destroyed.

Note this is guaranteed to run after `.close()` has been called and no methods will be run after.

## License

MIT
