async function causalValues (base) {
  const buf = []
  for await (const indexNode of base.createCausalStream()) {
    buf.push(indexNode)
  }
  return buf
}

async function indexedValues (index) {
  const buf = []
  await index.update()
  for (let i = index.length - 1; i > 0; i--) {
    const indexNode = await index.get(i)
    buf.push(indexNode)
  }
  return buf
}

function debugInputNode (inputNode) {
  if (!inputNode) return null
  return {
    ...inputNode,
    key: inputNode.id,
    value: inputNode.value.toString()
  }
}

function bufferize (arr) {
  return arr.map(b => Buffer.from(b))
}

module.exports = {
  bufferize,
  causalValues,
  indexedValues,
  debugInputNode
}
