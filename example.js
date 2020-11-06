const Autobase = require('./')
const AutoIndex = require('./lib/auto-index')
const Corestore = require('corestore')
const Rebaser = require('./lib/rebaser')

main().catch(console.error)

async function main () {
  // const l = new AutoIndex('index2.json')
  const all = []

  for (let i = 0; i < 4; i++) {
    const store = new Corestore('./store/' + i)
    const a = new Autobase(store)
    all.push(a)
    await a.ready()
  }

  const writers = all.map(a => a.local.key)

  for (const a of all) {
    a.setWriters(writers)
    await a.ready()
  }

  const [a, b, c, d] = all
  const h = new Rebaser(d.store.namespace('index').default({ valueEncoding: 'json' }))

  // await d.append('d#1 meeeeerge')

  replicate(a, b)
  replicate(b, c)
  replicate(a, c)
  replicate(d, b)

  // console.log('a:')

  console.log(await h.rebase(d))

  await print(d)

  // console.log(map)

  // for await (const data of a.createCausalStream()) {
  //   console.log(data)
  // }

  // console.log('b:')
  // for await (const data of b.createCausalStream()) {
  //   console.log(data.node.value)
  // }

  // console.log('c:')
  // for await (const data of c.createCausalStream()) {
  //   console.log(data.node.value)
  // }

  // console.log('d:')
  // for await (const data of d.createCausalStream()) {
  //   console.log(data.node.value)
  // }
}

function replicate (a, b) {
  const s = a.replicate(true, { live: true })
  s.pipe(b.replicate(false, { live: true })).pipe(s)
}

async function print (auto) {
  const nodes = []

  for await (const data of auto.createCausalStream()) {
    nodes.push(data)
  }

  nodes.reverse()

  const seqs = new Map()
  const heads = new Set()

  for (const data of nodes) {
    for (const head of heads) {
      if (gt(data, head)) {
        heads.delete(head)
      }
    }
    const seq = seqs.size

    heads.add(data)
    seqs.set(data, seq)

    const d = { seq, heads: [ ...heads ].map(h => seqs.get(h)), value: data.node.value }
    console.log(d)
  }

  function pad (n) {
    return n.toString().padStart((nodes.length - 1).toString().length, '0')
  }

  function gt (a, b) {
    for (const c of a.node.links()) {
      if (c[0].toString('hex') === b.node.feed.toString('hex')) {
        return c[1] > b.node.seq
      }
    }
    return false
  }
}
