async function causalValues (base) {
  const buf = []
  for await (const indexNode of base.createCausalStream()) {
    buf.push(debugIndexNode(indexNode))
  }
  return buf
}

async function indexedValues (base, output, opts = {}) {
  const buf = []
  const index = base.decodeIndex(output, {
    includeInputNodes: true,
    ...opts
  })
  for (let i = index.length - 1; i > 0; i--) {
    const indexNode = await index.get(i)
    buf.push(debugIndexNode(indexNode))
  }
  return buf
}

function debugIndexNode (indexNode) {
  return {
    value: (indexNode.value ?? indexNode.node.value).toString('utf8'),
    key: indexNode.node.key,
    seq: indexNode.node.seq,
    links: indexNode.node.links,
    clock: indexNode.clock
  }
}

module.exports = {
  causalValues,
  indexedValues,
  debugIndexNode
}
