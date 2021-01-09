# xsalsa20-universal

xsalsa20 instance that works in node and in the browser

```
npm install xsalsa20-universal
```

## Usage

``` js
const XSalsa20 = require('xsalsa20-universal')

const x = new XSalsa20(nonce, key)

x.update(output1, input1)
x.update(output2, input2)
x.update(output3, input3)

x.final()
```

## License

MIT
