# nanoresource

Small module that helps you maintain state around resources

```sh
npm install nanoresource
```

Allows you to easily implement open/close functionality for a resource
and having a way to mark the resource as active/inactive to avoid it being closed
while it is in middle of something.

## Usage

We can use this module to implement a simple resource that keep a file descriptor
around behind the scene to keep stating the same file.

```js
const nanoresource = require('nanoresource')
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
```

When running it you should see that the file is only being opened and closed
once.

## API

#### `const r = nanoresource(options)`

Create a new nanoresource. You can also extend from this prototype if you prefer.

Options include:

```js
{
  open: function (cb) { ... },
  close: function (cb) { ... }
}
```

If you specify open or close they are used to populate `r._open` and `r._close` for you.

The open should open the resource and the close one should close it.

The close method is guaranteed to run *after* open. If no open has been called and close is called the close method is *not* called.

#### `r.open(cb)`

Open the resource. Will call `r._open` behind the scenes once. If multiple calls to `r.open(cb)` the callbacks will be pushed to an internal queue and executed after the one call to `_open` has completed. If the resource was opened in the past the callback will be called on the next tick.

* Check `r.opened` to see if the resource is fully opened.
* Check `r.opening` to see if the resource is in the process of being opened.

If the `_open` method fails and calls it callback with an error this error is forwarded to the pending callbacks and if `r.open` is called again `_open` will be re-run.

#### `r.close(cb)`

Same semantics as `r.open`, except it only runs `_close` if the resource has been opened. If the resouce is in the middle of opening, `r.close` will wait for the open to finish and then try to close it.

If the resource is active (see the `r.active()` docs) then close will wait for the the resource to become inactive before closing it. However is a call `r.active()` happens after `r.close()` has been called it will fail immediately.

* Check `r.closed` to see if the resource is fully closed.
* Check `r.closing` to see if the resource is in the process of being closed.

Once a resource has been closed it can not be re-opened.

#### `const valid = r.active()`

Mark the resource as active. By marking a resource as active you have to call `r.inactive()` once at a later stage to indicate that it is no longer active from your point of view.

If the resource is not in a valid active state (for example if it is being closed), the `r.active` method will return falls and you should return an error to the caller.

As a conveinience you can pass in a callback to `r.active(callback)` and the active method will call that callback immediately with an error if it's not in a valid active state in addition to returning false.

It is same to have multiple methods call this method in parallel.

#### `r.inactive()`

The counter-part to `r.active()`. You must call this if you called `r.active` previously. It is a good idea to call this as the last thing you do in your "action" method of your resource.

As a conveinience you can pass in a callback, error, and value to `r.inactive(callback, error, value)` to call a callback after marking your view of the resource as inactive.

It is same to have multiple methods call this method in parallel.

## EventEmitter

If you need a nanoresource that is also an EventEmitter do `const Nanoresource = require('./emitter')` which returns an implementation that inherits from Node.js's EventEmitter prototype.

## License

MIT
