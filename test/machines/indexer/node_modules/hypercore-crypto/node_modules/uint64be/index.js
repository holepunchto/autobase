const UINT_32_MAX = Math.pow(2, 32)

exports.encodingLength = function () {
  return 8
}

exports.encode = function (num, buf, offset) {
  if (!buf) buf = Buffer.allocUnsafe(8)
  if (!offset) offset = 0

  const top = Math.floor(num / UINT_32_MAX)
  const rem = num - top * UINT_32_MAX

  buf.writeUInt32BE(top, offset)
  buf.writeUInt32BE(rem, offset + 4)
  return buf
}

exports.decode = function (buf, offset) {
  if (!offset) offset = 0

  const top = buf.readUInt32BE(offset)
  const rem = buf.readUInt32BE(offset + 4)

  return top * UINT_32_MAX + rem
}

exports.encode.bytes = 8
exports.decode.bytes = 8
