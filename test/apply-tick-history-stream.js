const test = require('brittle')
const b4a = require('b4a')

const {
  create,
  replicateAndSync,
  addWriterAndSync
} = require('./helpers')

function collect (stream) {
  return new Promise((resolve, reject) => {
    const entries = []
    let ended = false
    stream.on('data', d => entries.push(d))
    stream.on('error', err => reject(err))
    stream.on('end', () => { ended = true })
    stream.on('close', () => {
      if (ended) resolve(entries)
      else reject(new Error('Premature close'))
    })
  })
}

const totalAppendsFromHistory = (h) => {
  let totalAppends = 0
  for (const update of h) {
    let totalViewAppends = 0
    for (const view of update.views) {
      totalViewAppends += view.appends
    }
    totalAppends += totalViewAppends
  }

  return totalAppends
}

// a - b - a

test('apply history - simple 2', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await addWriterAndSync(a, b)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await replicateAndSync(bases)

  const h = await collect(a.system.createApplyTickHistoryStream())

  t.is(h.length, 6)
  t.alike(h, [
    { index: 4, views: [], writer: { key: a.local.key, length: 1 } }, // adding b as indexer
    { index: 6, views: [], writer: { key: b.local.key, length: 1 } }, // b no longer pending via ack
    { index: 8, views: [], writer: { key: a.local.key, length: 2 } }, // a acking b for consensus
    { index: 10, views: [{ key: a.view.key, appends: 1 }], writer: { key: a.local.key, length: 3 } }, // a0
    { index: 12, views: [{ key: a.view.key, appends: 1 }], writer: { key: b.local.key, length: 2 } }, // b0
    { index: 14, views: [{ key: a.view.key, appends: 1 }], writer: { key: a.local.key, length: 4 } } // a1
  ])
  t.is(totalAppendsFromHistory(h), a.view.length)
})

// [a, a, a] - [b, b, b, b] - a

test('apply history - batch writer append', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await addWriterAndSync(a, b)

  await a.append(['a' + ai++, 'a' + ai++, 'a' + ai++])
  await replicateAndSync(bases)

  await b.append(['b' + bi++, 'b' + bi++, 'b' + bi++, 'b' + bi++])
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await replicateAndSync(bases)

  const h = await collect(a.system.createApplyTickHistoryStream())

  t.is(h.length, 6)
  t.alike(h, [
    { index: 4, views: [], writer: { key: a.local.key, length: 1 } },
    { index: 6, views: [], writer: { key: b.local.key, length: 1 } }, // b no longer pending via ack
    { index: 8, views: [], writer: { key: a.local.key, length: 2 } }, // a acking b for consensus
    { index: 10, views: [{ key: a.view.key, appends: 3 }], writer: { key: a.local.key, length: 5 } },
    { index: 12, views: [{ key: a.view.key, appends: 4 }], writer: { key: b.local.key, length: 5 } },
    { index: 14, views: [{ key: a.view.key, appends: 1 }], writer: { key: a.local.key, length: 6 } }
  ])
  t.is(totalAppendsFromHistory(h), a.view.length)
})

test('apply history - add writer w/ existing view', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await a.append(['a' + ai++, 'a' + ai++, 'a' + ai++])
  await replicateAndSync(bases)

  const viewV0Key = a.view.key

  await addWriterAndSync(a, b)

  const viewV1Key = a.view.key

  await b.append(['b' + bi++, 'b' + bi++, 'b' + bi++, 'b' + bi++])
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await replicateAndSync(bases)

  const h = await collect(a.system.createApplyTickHistoryStream())

  t.is(h.length, 6)
  t.alike(h, [
    { index: 3, views: [{ key: viewV0Key, appends: 3 }], writer: { key: a.local.key, length: 3 } },
    { index: 6, views: [], writer: { key: a.local.key, length: 4 } },
    { index: 8, views: [], writer: { key: b.local.key, length: 1 } }, // b no longer pending via ack
    { index: 10, views: [], writer: { key: a.local.key, length: 5 } }, // a acking b for consensus
    { index: 12, views: [{ key: viewV1Key, appends: 4 }], writer: { key: b.local.key, length: 5 } },
    { index: 14, views: [{ key: viewV1Key, appends: 1 }], writer: { key: a.local.key, length: 6 } }
  ])
  t.is(totalAppendsFromHistory(h), a.view.length)
})

test('apply history - add writer w/ multiple views', async t => {
  const { bases } = await create(2, t, { open, apply })

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await a.append(['a' + ai++, 'a' + ai++, 'a' + ai++])
  await replicateAndSync(bases)

  const view0V0Key = a.view[0].key
  const view1V0Key = a.view[1].key

  await addWriterAndSync(a, b)

  const view0V1Key = a.view[0].key
  const view1V1Key = a.view[1].key

  await b.append(['b' + bi++, 'b' + bi++, 'b' + bi++, 'b' + bi++])
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await replicateAndSync(bases)

  const h = await collect(a.system.createApplyTickHistoryStream())

  t.is(h.length, 6)
  t.alike(h, [
    {
      index: 3,
      views: [
        {
          key: view0V0Key,
          appends: 3
        },
        {
          key: view1V0Key,
          appends: 3
        }
      ],
      writer: { key: a.local.key, length: 3 }
    },
    { index: 6, views: [], writer: { key: a.local.key, length: 4 } },
    { index: 8, views: [], writer: { key: b.local.key, length: 1 } }, // b no longer pending via ack
    { index: 10, views: [], writer: { key: a.local.key, length: 5 } }, // a acking b for consensus
    {
      index: 12,
      views: [
        {
          key: view0V1Key,
          appends: 4
        },
        {
          key: view1V1Key,
          appends: 4
        }
      ],
      writer: { key: b.local.key, length: 5 }
    },
    {
      index: 14,
      views: [
        {
          key: view0V1Key,
          appends: 1
        },
        {
          key: view1V1Key,
          appends: 1
        }
      ],
      writer: { key: a.local.key, length: 6 }
    }
  ])
  t.is(totalAppendsFromHistory(h), a.view[0].length + a.view[1].length)

  // `open` & `apply` with support for 2 views
  function open (store) {
    return [
      store.get('view', { valueEncoding: 'json' }),
      store.get('view-upper', { valueEncoding: 'json' })
    ]
  }

  async function apply (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        const key = Buffer.from(value.add, 'hex')
        await base.addWriter(key, { indexer: value.indexer })
        continue
      }

      if (view) {
        await view[0].append(value)
        await view[1].append(value.toUpperCase())
      }
    }
  }
})

test('apply history - writer removed', async t => {
  const { bases } = await create(2, t, { apply })

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await addWriterAndSync(a, b)

  await a.append(['a' + ai++, 'a' + ai++, 'a' + ai++])
  await replicateAndSync(bases)

  await b.append(['b' + bi++, 'b' + bi++, 'b' + bi++, 'b' + bi++])
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await a.append({ remove: b4a.toString(b.local.key, 'hex') })

  await replicateAndSync(bases)
  await replicateAndSync(bases)

  const h = await collect(a.system.createApplyTickHistoryStream())

  t.is(h.length, 7)
  t.alike(h, [
    { index: 4, views: [], writer: { key: a.local.key, length: 1 } },
    { index: 6, views: [], writer: { key: b.local.key, length: 1 } }, // b no longer pending via ack
    { index: 8, views: [], writer: { key: a.local.key, length: 2 } }, // a acking b for consensus
    { index: 10, views: [{ key: a.view.key, appends: 3 }], writer: { key: a.local.key, length: 5 } },
    { index: 12, views: [{ key: a.view.key, appends: 4 }], writer: { key: b.local.key, length: 5 } },
    { index: 14, views: [{ key: a.view.key, appends: 1 }], writer: { key: a.local.key, length: 6 } },
    { index: 17, views: [], writer: { key: a.local.key, length: 7 } } // Removal
  ])
  t.is(totalAppendsFromHistory(h), a.view.length)

  async function apply (batch, view, base) {
    for (const { value } of batch) {
      if (value.add) {
        await base.addWriter(b4a.from(value.add, 'hex'), { indexer: value.indexer })
        continue
      }

      if (value.remove) {
        await base.removeWriter(b4a.from(value.remove, 'hex'))
        continue
      }

      await view.append(value)
    }
  }
})

test('apply history - ignores reverse option', async t => {
  const { bases } = await create(2, t)

  const [a, b] = bases

  let ai = 0
  let bi = 0

  await addWriterAndSync(a, b)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await b.append('b' + bi++)
  await replicateAndSync(bases)

  await a.append('a' + ai++)
  await replicateAndSync(bases)

  await replicateAndSync(bases)

  const h = await collect(a.system.createApplyTickHistoryStream({ reverse: true }))

  t.is(h.length, 6)
  t.alike(h, [
    { index: 4, views: [], writer: { key: a.local.key, length: 1 } }, // adding b as indexer
    { index: 6, views: [], writer: { key: b.local.key, length: 1 } }, // b no longer pending via ack
    { index: 8, views: [], writer: { key: a.local.key, length: 2 } }, // a acking b for consensus
    { index: 10, views: [{ key: a.view.key, appends: 1 }], writer: { key: a.local.key, length: 3 } }, // a0
    { index: 12, views: [{ key: a.view.key, appends: 1 }], writer: { key: b.local.key, length: 2 } }, // b0
    { index: 14, views: [{ key: a.view.key, appends: 1 }], writer: { key: a.local.key, length: 4 } } // a1
  ])
  t.is(totalAppendsFromHistory(h), a.view.length)
})
