const Corestore = require('corestore')
const b4a = require('b4a')
const Hyperswarm = require('hyperswarm')

const replayLinearizer = require('./')

const discoveryKey = b4a.from(process.argv[2], 'hex')
const encryptionKey = b4a.from(process.argv[3], 'hex')
const heads = parseHeads(process.argv[4])
const indexers = JSON.parse(process.argv[5])

replay(discoveryKey, encryptionKey, heads, indexers)

async function replay (discoveryKey, encryptionKey, heads, indexers) {
  const swarm = new Hyperswarm()

  const store = new Corestore('./replay-example')

  swarm.on('connection', conn => {
    console.log('connection!')
    store.replicate(conn)
  })

  await swarm.join(discoveryKey).flushed()

  const linearizer = await replayLinearizer(store, indexers, heads, encryptionKey)

  for (const idx of linearizer.indexers) {
    console.log(idx.core.key.toString('hex'), linearizer.shouldAck(idx))
  }
}

function parseHeads (str) {
  const heads = JSON.parse(str)
  for (const head of heads) {
    head.key = b4a.from(head.key.data)
  }

  return heads
}
