# inspect-custom-symbol

Use [util.inspect.custom](https://nodejs.org/api/util.html#util_custom_inspection_functions_on_objects) without having to browserify util in the browser

```
npm install inspect-custom-symbol
```

## Usage

``` js
var custom = require('inspect-custom-symbol')

var foo = {
  [custom]: () => 'totally foo'
}

console.log(foo) // prints totally foo
```

## License

MIT
