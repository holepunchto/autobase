function uppercase (indexNode) {
  const val = indexNode.node.value
  const valBuf = Buffer.from(val.buffer, val.byteOffset, val.byteLength)
  return valBuf.toString('utf-8').toUpperCase()
}

module.exports = {
  uppercase
}
