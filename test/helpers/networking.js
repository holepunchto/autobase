const DebuggingStream = require('debugging-stream')
const { sync } = require('autobase-test-helpers')

function replicateDebugStream(a, b, t, opts = {}) {
  const { latency, speed, jitter } = opts

  const s1 = a.replicate(true, { keepAlive: false, ...opts })
  const s2Base = b.replicate(false, { keepAlive: false, ...opts })
  const s2 = new DebuggingStream(s2Base, { latency, speed, jitter })

  s1.on('error', (err) => t.comment(`replication stream error (initiator): ${err}`))
  s2.on('error', (err) => t.comment(`replication stream error (responder): ${err}`))

  if (opts.teardown !== false) {
    t.teardown(
      async function () {
        let missing = 2
        await new Promise((resolve) => {
          s1.on('close', onclose)
          s1.destroy()

          s2.on('close', onclose)
          s2.destroy()

          function onclose() {
            console.log('[onclose]')
            if (--missing === 0) resolve()
            console.log('onclose missing', missing)
          }
        })
      },
      { order: 0 }
    )
  }

  s1.pipe(s2).pipe(s1)

  return [s1, s2]
}

function replicateBasesDebugStream(bases, t, opts) {
  const streams = []
  const missing = bases.slice()

  while (missing.length) {
    const a = missing.pop()

    for (const b of missing) {
      streams.push(...replicateDebugStream(a, b, t, { teardown: false, ...opts }))
    }
  }

  return close

  function close() {
    return Promise.all(
      streams.map((s) => {
        s.destroy()
        return new Promise((resolve) => s.on('close', resolve))
      })
    )
  }
}

async function replicateAndSyncDebugStream(bases, t, opts) {
  const done = replicateBasesDebugStream(bases, t, opts)
  await sync(bases, opts)
  await done()
}

module.exports = {
  replicateDebugStream,
  replicateBasesDebugStream,
  replicateAndSyncDebugStream
}
