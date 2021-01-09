# fd-lock

Advisory cross-platform lock on a file using a file descriptor to it.

```
npm install fd-lock
```

## Usage

``` js
const lock = require('fd-lock')

// Can we lock the file using the fd?
console.log(lock(fd))
```

## API

#### `bool = lock(fd)`

Try to lock access to a file using a file descriptor.
Returns true if the file could be locked, false if not.

Note that the lock is only advisory and there is nothing stopping someone from accessing the file by simply ignoring the lock.

Works across processes as well.

#### `bool = lock.unlock(fd)`

Unlocks a file if you have the lock.

## License

MIT
