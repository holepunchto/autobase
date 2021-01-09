# random-access-file

Continuous reading or writing to a file using random offsets and lengths

```
npm install random-access-file
```

[![build status](https://travis-ci.org/random-access-storage/random-access-file.svg?branch=master)](https://travis-ci.org/random-access-storage/random-access-file)

## Why?

If you are receiving a file in multiple pieces in a distributed system it can be useful to write these pieces to disk one by one in various places throughout the file without having to open and close a file descriptor all the time.

random-access-file allows you to do just this.

## Usage

``` js
var randomAccessFile = require('random-access-file')

var file = randomAccessFile('my-file.txt')

file.write(10, Buffer.from('hello'), function(err) {
  // write a buffer to offset 10
  file.read(10, 5, function(err, buffer) {
    console.log(buffer) // read 5 bytes from offset 10
    file.close(function() {
      console.log('file is closed')
    })
  })
})
```

file will use an open file descriptor. When you are done with the file you should call `file.close()`.

## API

#### `var file = randomAccessFile(filename, [options])`

Create a new file. Options include:

``` js
{
  truncate: false, // truncate the file before reading / writing
  size: someSize, // truncate the file to this size first
  readable: true, // should the file be opened as readable?
  writable: true,  // should the file be opened as writable?
  lock (fd) => bool // optional function that informs if the file could be locked
}
```

#### `file.write(offset, buffer, [callback])`

Write a buffer at a specific offset.

#### `file.read(offset, length, callback)`

Read a buffer at a specific offset. Callback is called with the buffer read.

#### `file.del(offset, length, callback)`

Will truncate the file if offset + length is larger than the current file length.
Is otherwise a noop.

#### `file.stat(callback)`

Stat the storage. Should return an object with useful information about the underlying storage, including:

```js
{
  size: number // how many bytes of data is stored?
}
```

#### `file.close([callback])`

Close the underlying file descriptor.

#### `file.destroy([callback])`

Unlink the underlying file.

#### `file.on('open')`

Emitted when the file descriptor has been opened. You can access the fd using `file.fd`.
You do not need to wait for this event before doing any reads/writes.

#### `file.on('close')`

Emitted when the file has been closed.

## License

MIT
