# hypercore-streams

External implementation of a WriteStream and ReadStream for Hypercore

```
npm install hypercore-streams
```

## Usage

``` js
const { WriteStream, ReadStream } = require('hypercore-streams')

const ws = new WriteStream(feed)
const rs = new ReadStream(feed, {
  start: 0,
  live: true,
  valueEncoding: 'json'
})
```

## License

MIT
