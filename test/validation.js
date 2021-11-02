const test = require('tape')
const Hypercore = require('hypercore')
const ram = require('random-access-memory')

const Autobase = require('../')

test('should throw if hypercore valueEncoding is utf-8', async t => {
  const coreWithUtf8 = new Hypercore(ram, { valueEncoding: 'utf-8' })

  const base = new Autobase([coreWithUtf8])

  try {
    await base.ready()
    t.fail('Should not be ready')
  } catch (error) {
    t.equal(error.message, 'Hypercore input must be binary ones.')
  }
})

test('should throw if hypercore valueEncoding is json', async t => {
  const coreWithJson = new Hypercore(ram, { valueEncoding: 'json' })

  const base = new Autobase([coreWithJson])

  try {
    await base.ready()
    t.fail('Should not be ready')
  } catch (error) {
    t.equal(error.message, 'Hypercore input must be binary ones.')
  }
})

test('should not throw if hypercore valueEncoding is binary', async t => {
  const coreWithBinary = new Hypercore(ram)

  const base = new Autobase([coreWithBinary])

  try {
    await base.ready()
    t.pass('Should be ready')
  } catch {
    t.fail('Should not throw')
  }
})
