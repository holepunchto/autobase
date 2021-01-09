# nanoguard

Small module that allows you to guard a call to a function.

```
npm install nanoguard
```

## Usage

``` js
const Nanoguard = require('nanoguard')
const guard = new Nanoguard()

guard.wait()

// When the amount of wait() calls reflect continue() calls ready is called
guard.ready(function () {
  console.log('Ready to continue!')
})

guard.continue()
```

## API

#### `const guard = new Nanoguard()`

Make a new guard instance

#### `guard.wait()`

Increment the wait counter.
Non-owners of the guard can use this to defer the ready function of the guard owner.

#### `guard.continue()`

Decrement the wait counter on the next tick. If the counter is `0` it calls all pending
ready functions.
If you called wait() you have to call continue() at some point.

#### `guard.continueSync()`

Same as `guard.continue()` but decrements in the same tick.

#### `const cont = guard.waitAndContinue()`

Calls wait and returns a function that when called calls continue() once no matter how many times it is called.

#### `guard.ready(fn)`

Pass a function that is called when the wait counter is `0`.

#### `guard.destroy()`

Force sets the wait counter to `0` forever.
Should only be called by the owner of the guard.

#### `const bool = guard.waiting`

Boolean indicating if the wait counter is `> 0`.

## License

MIT
