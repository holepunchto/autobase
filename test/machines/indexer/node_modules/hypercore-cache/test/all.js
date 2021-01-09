const test = require('tape')

const HypercoreCache = require('..')

test('set/get without namespaces, no swapping or eviction', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 * 10 })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  t.same(cache._freshByteSize, 1024 * 2)
  t.same(cache.get('a'), aVal)
  t.same(cache.get('b'), bVal)
  t.end()
})

test('set/get without namespaces, stale swap ', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  t.same(cache._freshByteSize, 1024)
  t.same(cache._staleByteSize, 1024)
  t.same(cache.get('a'), aVal)
  t.same(cache.get('b'), bVal)
  t.end()
})

test('set/get without namespaces, eviction', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  debugSet(cache, 'c', cVal)
  debugSet(cache, 'd', dVal)

  t.same(cache.byteSize, 1024 * 2)
  t.false(cache.get('a'), aVal)
  t.false(cache.get('b'), aVal)
  t.same(cache.get('c'), cVal)
  t.same(cache.get('d'), dVal)
  t.end()
})

test('set/get without namespaces, full cache', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 * 2 })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  debugSet(cache, 'c', cVal)
  debugSet(cache, 'd', dVal)

  t.same(cache.byteSize, 1024 * 4)
  t.same(cache.get('a'), aVal)
  t.same(cache.get('b'), bVal)
  t.same(cache.get('c'), cVal)
  t.same(cache.get('d'), dVal)
  t.end()
})

test('set/get without namespaces, lots of sets', t => {
  const NUM_SETS = 1024
  // Must be a power of two for this test.
  const CACHE_SIZE = 64

  const cache = new HypercoreCache({ maxByteSize: 1024 * CACHE_SIZE })
  const bufs = []

  for (let i = 0; i < NUM_SETS; i++) {
    const val = Buffer.from('' + i)
    cache.set(i, val)
    bufs.push(val)
  }

  t.same(cache.byteSize, 1024 * CACHE_SIZE * 2)
  let failed = false
  for (let i = 0; i < CACHE_SIZE * 2; i++) {
    if (cache.get('' + (NUM_SETS - i)) !== bufs[NUM_SETS - i]) {
      failed = true
      t.fail('cache entry incorrect:', i)
    }
  }
  for (let i = 0; i < NUM_SETS - CACHE_SIZE * 2; i++) {
    if (cache.get('' + i)) {
      failed = true
      t.fail('cache entry incorrectly set:', i)
    }
  }
  if (!failed) t.pass('cache is correct')
  t.end()
})

test('removal without namespaces, entry in fresh', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 * 2 })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  debugSet(cache, 'c', cVal)
  debugSet(cache, 'd', dVal)

  t.same(cache.byteSize, 1024 * 4)
  t.same(cache.get('d'), dVal)

  cache.del('d')
  t.same(cache.byteSize, 1024 * 3)
  t.same(cache._freshByteSize, 1024 * 1)
  t.same(cache._staleByteSize, 1024 * 2)

  t.false(cache.get('d'))
  t.same(cache.get('a'), aVal)
  t.same(cache.get('b'), bVal)
  t.same(cache.get('c'), cVal)

  t.end()
})

test('removal without namespaces, entry in stale', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 * 2 })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  debugSet(cache, 'c', cVal)
  debugSet(cache, 'd', dVal)

  t.same(cache.byteSize, 1024 * 4)
  t.same(cache.get('d'), dVal)

  cache.del('a')
  t.same(cache.byteSize, 1024 * 3)
  t.same(cache._freshByteSize, 1024 * 2)
  t.same(cache._staleByteSize, 1024 * 1)

  t.false(cache.get('a'))
  t.same(cache.get('b'), bVal)
  t.same(cache.get('c'), cVal)
  t.same(cache.get('d'), dVal)

  t.end()
})

test('removal without namespaces, entry in both stale and fresh', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 * 2 })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  debugSet(cache, 'c', cVal)
  debugSet(cache, 'a', dVal)

  t.same(cache.byteSize, 1024 * 4)
  t.same(cache.get('a'), dVal)

  cache.del('a')
  t.same(cache.byteSize, 1024 * 2)
  t.same(cache._freshByteSize, 1024 * 1)
  t.same(cache._staleByteSize, 1024 * 1)

  t.false(cache.get('a'))
  t.same(cache.get('b'), bVal)
  t.same(cache.get('c'), cVal)

  t.end()
})

test('custom size estimator', t => {
  const cache = new HypercoreCache({ maxByteSize: 1, estimateSize: val => val.length })
  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  debugSet(cache, 'c', cVal)
  debugSet(cache, 'd', dVal)

  t.same(cache.byteSize, 2)
  t.false(cache.get('a'))
  t.false(cache.get('b'))
  t.same(cache.get('c'), cVal)
  t.same(cache.get('d'), dVal)
  t.end()
})

test('onevict is triggered', t => {
  t.plan(8)
  let called = 0

  const cache = new HypercoreCache({
    maxByteSize: 1,
    estimateSize: val => val.length,
    onEvict: evicted => {
      called++
      t.same(evicted.size, 1)
    }
  })

  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(cache, 'a', aVal)
  debugSet(cache, 'b', bVal)
  debugSet(cache, 'c', cVal)
  debugSet(cache, 'd', dVal)

  t.same(cache.byteSize, 2)
  t.false(cache.get('a'))
  t.false(cache.get('b'))
  t.same(cache.get('c'), cVal)
  t.same(cache.get('d'), dVal)
  t.same(called, 2)
  t.end()
})

test('set/get with namespaces', t => {
  const cache = new HypercoreCache({ maxByteSize: 1024 * 2 })
  const ns1 = cache.namespace('1')
  const ns2 = cache.namespace('2')

  const aVal = Buffer.from('a')
  const bVal = Buffer.from('b')
  const cVal = Buffer.from('c')
  const dVal = Buffer.from('c')

  debugSet(ns1, 'a', aVal)
  debugSet(ns1, 'b', bVal)
  debugSet(ns2, 'c', cVal)
  debugSet(ns2, 'd', dVal)

  t.same(cache.byteSize, 1024 * 4)
  t.same(ns1.get('a'), aVal)
  t.same(ns1.get('b'), bVal)
  t.same(ns2.get('c'), cVal)
  t.same(ns2.get('d'), dVal)
  t.end()
})

function debugSet (cache, key, val) {
  // console.log('before setting', key, cache._info)
  cache.set(key, val)
  // console.log('after setting', key, cache._info)
}
