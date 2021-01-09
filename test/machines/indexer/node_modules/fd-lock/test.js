const tape = require('tape')
const lock = require('./')
const { openSync, closeSync, unlinkSync } = require('fs')
const { spawnSync } = require('child_process')

tape('two in one process', function (assert) {
  const fd = openSync(__filename, 'r')
  assert.ok(lock(fd), 'could lock')

  const fd1 = openSync(__filename, 'r')
  assert.notOk(lock(fd1), 'could not lock again')

  assert.ok(lock.unlock(fd), 'could unlock')

  assert.ok(lock(fd1), 'could lock the other one now')
  assert.notOk(lock(fd), 'could not lock first one')
  closeSync(fd1)

  assert.ok(lock(fd), 'could lock first one after closing second one')
  closeSync(fd)

  assert.end()
})

tape('two in one process (new file)', function (assert) {
  const fd = openSync(__filename + '.tmp', 'w')
  assert.ok(lock(fd), 'could lock')

  const fd1 = openSync(__filename + '.tmp', 'r')
  assert.notOk(lock(fd1), 'could not lock again')

  assert.ok(lock.unlock(fd), 'could unlock')

  assert.ok(lock(fd1), 'could lock the other one now')
  assert.notOk(lock(fd), 'could not lock first one')
  closeSync(fd1)

  assert.ok(lock(fd), 'could lock first one after closing second one')
  closeSync(fd)
  unlinkSync(__filename +  '.tmp')

  assert.end()
})


tape('two in different processes', function (assert) {
  const fd = openSync(__filename, 'r')

  assert.ok(lock(fd))

  {
    const { stdout, stderr } = spawnSync(process.execPath, [ '-e', `
      const lock = require(${JSON.stringify(__dirname)})
      const { openSync } = require('fs')
      console.log(lock(openSync(${JSON.stringify(__filename)}, 'r')))
    ` ])

    assert.same(stderr.toString(), '')
    assert.same(stdout.toString().trim(), 'false', 'other process could not lock')
  }

  closeSync(fd)

  {
    const { stdout, stderr } = spawnSync(process.execPath, [ '-e', `
      const lock = require(${JSON.stringify(__dirname)})
      const { openSync } = require('fs')
      console.log(lock(openSync(${JSON.stringify(__filename)}, 'r')))
    ` ])

    assert.same(stderr.toString(), '')
    assert.same(stdout.toString().trim(), 'true', 'other process could lock')
  }

  assert.end()
})
