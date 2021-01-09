module.exports = codecs

codecs.ascii = createString('ascii')
codecs.utf8 = createString('utf-8')
codecs.hex = createString('hex')
codecs.base64 = createString('base64')
codecs.ucs2 = createString('ucs2')
codecs.utf16le = createString('utf16le')
codecs.ndjson = createJSON(true)
codecs.json = createJSON(false)
codecs.binary = {
  name: 'binary',
  encode: function encodeBinary (obj) {
    return typeof obj === 'string'
      ? Buffer.from(obj, 'utf-8')
      : Buffer.isBuffer(obj)
        ? obj
        : Buffer.from(obj.buffer, obj.byteOffset, obj.byteLength)
  },
  decode: function decodeBinary (buf) {
    return Buffer.isBuffer(buf)
      ? buf
      : Buffer.from(buf.buffer, buf.byteOffset, buf.byteLength)
  }
}

function codecs (fmt, fallback) {
  if (typeof fmt === 'object' && fmt && fmt.encode && fmt.decode) return fmt

  switch (fmt) {
    case 'ndjson': return codecs.ndjson
    case 'json': return codecs.json
    case 'ascii': return codecs.ascii
    case 'utf-8':
    case 'utf8': return codecs.utf8
    case 'hex': return codecs.hex
    case 'base64': return codecs.base64
    case 'ucs-2':
    case 'ucs2': return codecs.ucs2
    case 'utf16-le':
    case 'utf16le': return codecs.utf16le
  }

  return fallback !== undefined ? fallback : codecs.binary
}

function createJSON (newline) {
  return {
    name: newline ? 'ndjson' : 'json',
    encode: newline ? encodeNDJSON : encodeJSON,
    decode: function decodeJSON (buf) {
      return JSON.parse(buf.toString())
    }
  }

  function encodeJSON (val) {
    return Buffer.from(JSON.stringify(val))
  }

  function encodeNDJSON (val) {
    return Buffer.from(JSON.stringify(val) + '\n')
  }
}

function createString (type) {
  return {
    name: type,
    encode: function encodeString (val) {
      if (typeof val !== 'string') val = val.toString()
      return Buffer.from(val, type)
    },
    decode: function decodeString (buf) {
      return buf.toString(type)
    }
  }
}
