const { IndexNode } = require('../../lib/nodes')

async function causalValues (base) {
  const buf = []
  for await (const indexNode of base.createCausalStream()) {
    buf.push(debugIndexNode(indexNode))
  }
  return buf
}

async function indexedValues (output) {
  const buf = []
  for (let i = output.length - 1; i >= 0; i--) {
    const indexNode = IndexNode.decode(await output.get(i))
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
