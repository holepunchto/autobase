async function causalValues (base) {
  return collect(base.createCausalStream())
}

async function collect (stream, map) {
  const buf = []
  for await (const node of stream) {
    buf.push(map ? map(node) : node)
  }
  return buf
}

async function linearizedValues (index) {
  const buf = []
  await index.update()
  for (let i = index.length - 1; i >= 0; i--) {
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
  collect,
  causalValues,
  linearizedValues,
  debugInputNode
}
