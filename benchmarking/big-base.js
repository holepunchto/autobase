const p = require('path')
const Hypercore = require('hypercore')
const Autobase = require('.')

const ROOT = p.join(__dirname, 'storage')
const NUM_CORES = 2000
const APPENDS = [2 * 1e5]
const APPEND_SIZES = [64]
const COMPRESSION_TAG = process.env['COMPRESS'] ? 'compressed' : 'uncompressed'

start()

async function start () {
  for (const numAppends of APPENDS) {
    for (const appendSize of APPEND_SIZES) {
      console.log(`testing with ${numAppends} and append size ${appendSize}`)
      await test(numAppends, appendSize)
      console.log(' * done')
    }
  }
}

async function test (numAppends, appendSize) {
  const dir = p.join(ROOT, `${COMPRESSION_TAG}-${NUM_CORES}-${numAppends}-${appendSize}`)
  const inputs = []
  for (let i = 0; i < NUM_CORES; i++) {
    inputs.push(new Hypercore(p.join(dir, '' + i)))
  }
  const base = new Autobase({
    inputs,
    autostart: true 
  })
  for (let i = 0; i < numAppends; i++) {
    if ((i % 1000) === 0) console.log(i)
    const input = inputs[Math.floor(Math.random() * inputs.length)]
    const buf = Buffer.allocUnsafe(appendSize).fill(Math.floor(Math.random() * 10))
    await base.append(buf, null, input)
  }
}
