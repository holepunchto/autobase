const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const EncryptionView = require('../lib/encryption')
const { OplogMessage } = require('../lib/messages.js')

const { create, replicateAndSync } = require('./helpers')

test('anchor - simple', async (t) => {
  const { bases } = await create(1, t, { apply })
  const [a] = bases

  let anchor = null

  await a.append({ data: 'value', anchor: true })

  t.is(a.system.members, 2) // anchor adds a member
  t.is(a.view.length, 1)
  t.is(a.view.signedLength, 1)

  await a.update()

  const heads = a.heads()

  t.is(heads.length, 1)
  t.alike(anchor, heads[0])

  async function apply(nodes, view, base) {
    for (const node of nodes) {
      await view.append(node.value.data)
      if (node.value.anchor) {
        anchor = await base.createAnchor()
      }
    }
  }
})

test('anchor - encryption', async (t) => {
  const encryptionKey = b4a.alloc(32).fill('secret')
  const { bases } = await create(1, t, { apply, encryptionKey })
  const [a] = bases

  let anchor = null

  await a.append({ data: 'value', anchor: true })

  t.is(a.system.members, 2) // anchor adds a member
  t.is(a.view.length, 1)
  t.is(a.view.signedLength, 1)

  await a.update()

  const heads = a.heads()

  t.is(heads.length, 1)
  t.alike(anchor, heads[0])
  const anchorCore = a.store.get({ key: anchor.key })
  const anchorBlock = await anchorCore.get(0)

  let value = anchorBlock
  const index = 0
  const key = anchor.key
  const manifest = anchorCore.manifest

  // The following is all of decodeValue() but not selecting only for the op's value
  const e = new EncryptionView(a, a.encryptionCore)
  const w = e.getWriterEncryption()

  await w.decrypt(index, value, { key, manifest })
  value = value.subarray(8)
  const op = c.decode(OplogMessage, value)

  const { heads: nodeHeads, value: nodeValue } = op.node
  t.is(nodeValue, null, 'value is null')
  t.alike(nodeHeads, [{ key: a.local.key, length: 1 }], 'defined local key & length as head')

  async function apply(nodes, view, base) {
    for (const node of nodes) {
      await view.append(node.value.data)
      if (node.value.anchor) {
        anchor = await base.createAnchor()
      }
    }
  }
})

test('anchor - same anchor', async (t) => {
  const { bases } = await create(2, t, { apply })
  const [a, b] = bases

  let existing = null

  await a.append({ add: b4a.toString(b.local.key, 'hex'), indexer: true })
  await replicateAndSync([a, b])

  await b.append({ data: 'hello', anchor: false })
  await a.append({ data: 'value', anchor: true })

  await replicateAndSync([a, b])

  t.is(a.system.members, 3) // anchor adds a member
  t.is(a.view.length, 2)
  t.is(a.view.signedLength, 0)

  const heads = a.heads()

  t.is(heads.length, 2)
  t.alike(existing, heads[1])

  async function apply(nodes, view, base) {
    for (const node of nodes) {
      if (node.value.add) {
        await base.addWriter(b4a.from(node.value.add, 'hex'), { indexer: node.value.indexer })
        continue
      }

      await view.append(node.value.data)

      if (node.value.anchor) {
        const anchor = await base.createAnchor()

        if (existing) {
          t.alike(existing, anchor)
        } else {
          existing = anchor
        }
      }
    }
  }
})
