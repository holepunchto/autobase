const p = require('path')
const { tmpdir } = require('os')
const ram = require('random-access-memory')
const Corestore = require('corestore')

const Autobase = require('..')

const VALUE_CHARACTERS = 'abcdefghijklmnopqrstuvwxyz'
let setups = 0

async function setup () {
  const dir = p.join(tmpdir(), `autobase-fuzzing-${process.pid}`)
  const subdir = p.join(dir, '' + setups++)

  const store1 = new Corestore(p.join(subdir, 'store-1'))
  const store2 = new Corestore(p.join(subdir, 'store-2'))

  const s1 = store1.replicate(true, { live: true })
  const s2 = store2.replicate(false, { live: true })
  s1.pipe(s2).pipe(s1)

  const input1 = store1.get({ name: 'input' })
  const input2 = store2.get({ name: 'input' })
  const output1 = store1.get({ name: 'output' })
  const output2 = store2.get({ name: 'output' })

  const base1 = new Autobase([input1, input2], { input: input1 })
  const base2 = new Autobase([input1, input2], { input: input2 })

  const view1 = base1.linearize(output1)
  const view2 = base2.linearize([output1], { autocommit: false })

  return {
    reference: {
      autobases: [base1, base2],
      views: {
        local: view1,
        remote: view2
      }
    },
    actual: null,
    state: null
  }
}

function operations ({ autobases, views }, _, rng, { operations: opts } = {}) {
  const append = {
    inputs: () => [rng(autobases.length), randomString(rng, VALUE_CHARACTERS, opts.append.valueLength || 5)],
    operation: async (inputIdx, value) => {
      await autobases[inputIdx].append(Buffer.from(value))
    }
  }

  const appendForked = {
    inputs: () => [rng(autobases.length), randomString(rng, VALUE_CHARACTERS, opts.append.valueLength || 5)],
    operation: async (inputIdx, value) => {
      await autobases[inputIdx].append(Buffer.from(value), [])
    }
  }

  const updateLocalView = {
    inputs: () => [],
    operation: async () => {
      await views.local.update()
    }
  }

  const updateRemoteView = {
    inputs: () => [],
    operation: async () => {
      await views.remote.update()
      console.log('remote update status:', views.remote.status)
    }
  }

  return {
    append,
    appendForked,
    updateLocalView,
    updateRemoteView
  }
}

function validation ({ autobases, views }, _, rng, opts = {}) {
  const compareViewAndCausalStream = async () => {
    const memView = await processCausalStream(autobases[0].createCausalStream())
    await views.remote.update()

    const memLength = memView.length
    const viewLength = views.remote.length
    if (viewLength > memLength) {
      throw new Error(`view is longer than causal stream: ${memLength} !== ${viewLength}`)
    }

    for (let i = 0; i < viewLength; i++) {
      const memValue = memView[i]?.value?.toString()
      const viewValue = (await views.remote.get(i))?.value?.toString()
      if (memValue !== viewValue) {
        throw new Error(`mismatched nodes at index ${i}: ${memValue} !== ${viewValue}`)
      }
    }
  }
  return {
    tests: {
      compareViewAndCausalStream
    },
    validators: {
      causalOrdering: {
        operation: test => test(),
        test: 'compareViewAndCausalStream'
      }
    }
  }
}

module.exports = {
  setup,
  operations,
  validation
}

async function processCausalStream (stream) {
  const buf = []
  for await (const node of stream) {
    buf.push(node)
  }
  buf.reverse()
  return buf
}

function randomString (rng, alphabet, length) {
  let s = ''
  for (let i = 0; i < length; i++) {
    s += alphabet[rng(alphabet.length)]
  }
  return s
}
