const test = require('brittle')
const Corestore = require('corestore')
const tmpDir = require('test-tmp')

const { createBase } = require('./helpers')

test.solo('repair borked batches', async (t) => {
  const tmp = await t.tmp()
  const store = new Corestore(tmp)
  const base = createBase(store, null, t)
  await base.ready()

  await base.append('hello')
  t.is(base.view.length, 1, 'appended')
  const batchOnlyState = base.view.core.sessionStates.find((s) => s.name === 'batch' && !s.atomized)

  t.comment('before borking')
  // {
  //   await batchOnlyState.mutex.lock()
  //   const batch1 = await base.view.core.storage.createSession('batch', null)

  //   const tx = batch1.write()
  //   // Set a nonsense dependency to force all tree nodes (from the parent) to fail
  //   tx.setDependency({
  //     dataPointer: 1337,
  //     length: 3
  //   })
  //   const flushed = await tx.flush()
  //   batchOnlyState._unlock()
  //   t.ok(flushed)
  // }

  t.comment('verify its borked')
  // {
  //   await batchOnlyState.mutex.lock()
  //   const rx = batchOnlyState.storage.read()
  //   const tree = rx.getTreeNode(1)
  //   rx.tryFlush()
  //   t.is(await tree, null, 'tree node gone')
  //   batchOnlyState._unlock()
  // }
  await base.close()
  await store.close()

  const store2 = new Corestore(tmp)
  await store2.ready()

  const base2 = createBase(store2, null, t)
  await t.execution(base2.ready())
  await base2.append('boop')

  await base2.close()
  await store2.close()

  const store3 = new Corestore(tmp)
  await store3.ready()

  const base3 = createBase(store3, null, t)
  await t.execution(base3.ready(), 'reloading from storage works')
  await base3.append('beep')

  t.is(base3.view.length, 3, 'all appends on view')
  await base3.close()
})

test('repair system core borked batch', async (t) => {
  const tmp = await tmpDir(t)
  const store = new Corestore(tmp)
  const base = createBase(store, null, t)

  await base.append('hello')
  t.is(base.view.length, 1, 'appended')
  const batchCore = base.system.core.core
  const batchOnlyState = batchCore.sessionStates.find((s) => s.name === 'batch' && !s.atomized)

  t.comment('before borking')
  {
    await batchOnlyState.mutex.lock()
    const batch1 = await batchCore.storage.createSession('batch', null)

    const tx = batch1.write()
    // Set a nonsense dependency to force all tree nodes (from the parent) to fail
    const dep = {
      dataPointer: 1337,
      length: 3
    }
    tx.setDependency(dep)
    batchOnlyState.storage.setDependencyHead(dep) // update memory
    const flushed = await tx.flush()
    batchOnlyState._unlock()
    t.ok(flushed)
  }

  t.comment('verify its borked')
  {
    await batchOnlyState.mutex.lock()
    const rx = batchOnlyState.storage.read()
    const tree = rx.getTreeNode(1)
    rx.tryFlush()
    t.is(await tree, null, 'tree node gone')
    batchOnlyState._unlock()
  }
  await base.close()
  await store.close()

  const store2 = new Corestore(tmp)
  await store2.ready()

  const base2 = createBase(store2, null, t)
  await t.execution(base2.ready())

  // does waiting mean its fine?
  await base2.close()
  await store2.close()

  const store3 = new Corestore(tmp)
  await store3.ready()

  const base3 = createBase(store3, null, t)
  await t.execution(base3.ready())
  await base3.close()
})
