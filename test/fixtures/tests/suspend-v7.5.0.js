const fs = require('fs/promises')
const path = require('path')
const Corestore = require('corestore')
const test = require('brittle')
const tmpDir = require('test-tmp')
const b4a = require('b4a')

const { createBase, replicateAndSync } = require('../../helpers')

test('suspend - restart from v7.5.0 fixture', async t => {
  const fixturePath = path.join(__dirname, '../data/suspend/corestore-v7.5.0')

  const bdir = await tmpDir(t)
  const cdir = await tmpDir(t)

  await fs.cp(path.join(fixturePath, 'b'), bdir, { recursive: true })
  await fs.cp(path.join(fixturePath, 'c'), cdir, { recursive: true })

  const bstore = new Corestore(bdir, { allowBackup: true })
  const cstore = new Corestore(cdir, { allowBackup: true })

  const b = await createBase(bstore.session(), null, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  const c = await createBase(cstore.session(), null, t, {
    apply: applyMultiple,
    open: openMultiple
  })

  await b.ready()
  await c.ready()

  // invariant
  const exp = {
    key: b4a.from('0f5016881fa0b6801e59f842fe10d2d2acb10b531dc590e783655c9140adfdea', 'hex'),
    length: 83
  }

  await c.append({ index: 1, data: 'c' + 300 })

  const last = await c.local.get(c.local.length - 1)
  t.alike(last.node.heads, [exp])

  await replicateAndSync([b, c])

  t.is(await c.view.first.get(c.view.first.length - 1), 'c' + 300)
  t.is(await c.view.second.get(c.view.second.length - 1), 'b' + 299)
})

function openMultiple (store) {
  return {
    first: store.get('first', { valueEncoding: 'json' }),
    second: store.get('second', { valueEncoding: 'json' })
  }
}

async function applyMultiple (batch, view, base) {
  for (const { value } of batch) {
    if (value.add) {
      await base.addWriter(Buffer.from(value.add, 'hex'))
      continue
    }

    if (value.index === 1) {
      await view.first.append(value.data)
    } else if (value.index === 2) {
      await view.second.append(value.data)
    }
  }
}
