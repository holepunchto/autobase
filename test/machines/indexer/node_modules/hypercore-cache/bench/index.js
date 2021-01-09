const hashlru = require('hashlru')
const alru = require('array-lru')
const HypercoreCache = require('..')

const NUM_VALUES = 1024 * 1024

function bench (name, cache) {
  console.time(name)
  for (let i = 0; i < NUM_VALUES; i++) {
    cache.set(i, Math.random())
  }
  for (let i = 0; i < NUM_VALUES; i++) {
    cache.set(i, Math.random())
  }
  for (let i = NUM_VALUES; i < NUM_VALUES * 2; i++) {
    cache.set(i, Math.random())
  }
  console.timeEnd(name)
}

const coreCache = new HypercoreCache({
  maxByteSize: NUM_VALUES * 8,
  estimateSize: val => 8
})
const hlruCache = hashlru(NUM_VALUES)
const alruCache = alru(NUM_VALUES)

bench('hashlru', hlruCache)
bench('hashlru', hlruCache)
bench('hashlru', hlruCache)

bench('alru', alruCache)
bench('alru', alruCache)
bench('alru', alruCache)

bench('hypercore-cache', coreCache)
bench('hypercore-cache', coreCache)
bench('hypercore-cache', coreCache)
