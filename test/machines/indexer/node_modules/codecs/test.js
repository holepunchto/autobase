var tape = require('tape')
var codecs = require('./')

tape('json', function (t) {
  var enc = codecs('json')
  t.same(enc.name, 'json')
  t.same(enc.encode({}), Buffer.from('{}'))
  t.same(enc.decode(Buffer.from('{}')), {})
  t.end()
})

tape('utf-8', function (t) {
  var enc = codecs('utf-8')
  t.same(enc.name, 'utf-8')
  t.same(enc.encode('hello world'), Buffer.from('hello world'))
  t.same(enc.decode(Buffer.from('hello world')), 'hello world')
  t.end()
})

tape('hex', function (t) {
  var enc = codecs('hex')
  t.same(enc.name, 'hex')
  t.same(enc.encode('abcd'), Buffer.from([0xab, 0xcd]))
  t.same(enc.decode(Buffer.from([0xab, 0xcd])), 'abcd')
  t.end()
})

tape('binary', function (t) {
  var enc = codecs()
  t.same(enc.name, 'binary')
  const input = Buffer.from('hello world')
  t.same(enc.encode('hello world'), input)
  t.equals(enc.encode(input), input)
  t.equals(enc.decode(input), input)
  const uint8 = new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
  t.ok(Buffer.isBuffer(enc.encode(uint8)))
  t.equals(enc.encode(uint8).compare(input), 0)
  t.ok(Buffer.isBuffer(enc.decode(uint8)))
  t.equals(enc.decode(uint8).compare(input), 0)
  t.end()
})

tape('custom', function (t) {
  var enc = codecs({
    name: 'custom',
    encode: function () {
      return Buffer.from('lol')
    },
    decode: function () {
      return 42
    }
  })

  t.same(enc.name, 'custom')
  t.same(enc.encode('hello'), Buffer.from('lol'))
  t.same(enc.encode(42), Buffer.from('lol'))
  t.same(enc.decode(Buffer.from('lol')), 42)
  t.end()
})

tape('uint8arrays in binary', function (t) {
  var enc = codecs('binary')

  var buf = enc.encode(new Uint8Array([1, 2, 3]))
  t.same(buf, Buffer.from([1, 2, 3]))
  t.end()
})

tape('custom fallback', function (t) {
  t.equals(codecs('baseless', null), null)
  const custom = { decode: function () {}, encode: function () {} }
  t.equals(codecs('baseless', custom), custom)
  t.end()
})
