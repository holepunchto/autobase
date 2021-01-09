function double (indexNode) {
  const val = indexNode.node.value
  const valStr = Buffer.from(val.buffer, val.byteOffset, val.byteLength).toString('utf-8')
  return [valStr + ':first', valStr + ':second']
}

module.exports = {
  double
}
