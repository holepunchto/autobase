var custom = require('./')

var foo = {
  [custom]: () => 'totally foo'
}

console.log(foo) // prints totally foo
