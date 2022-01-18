const b = require('b4a')

function indexOf (clock, key) {
  if (!clock.length) return -1
  let start = 0
  let end = clock.length
  while (start < end) {
    const mid = Math.floor((end + start) / 2)
    const comp = b.compare(clock[mid][0], key)
    if (comp === -1) {
      start = mid + 1
    } else if (comp === 1) {
      end = mid
    } else {
      return mid
    }
  }
  return -1
}

function get (clock, key) {
  const idx = indexOf(clock, key)
  return idx === -1 ? null : clock[idx]
}

function lt (clock1, clock2) {
  if (!clock1.length || !clock2.length || clock1 === clock2) return false
  for (const [key, length] of clock1) {
    const o = get(clock2, key)
    if (!o || (length >= o[1])) return false
  }
  return true
}

function lte (clock1, clock2) {
  if (!clock1.length|| !clock2.length) return false
  for (const [key, length] of clock1) {
    const o = get(clock2, key)
    if (!o || (length > o[1])) return false
  }
  return true
}

function gte (clock1, clock2) {
  if (!clock1.length || !clock2.length) return false
  for (const [key, length] of clock1) {
    const o = get(clock2, key)
    if (!o || (length < o[1])) return false
  }
  return true
}

function eq (clock1, clock2) {
  return lte(clock1, clock2) && gte(clock2, clock1)
}

module.exports = {
  indexOf,
  get,
  lt,
  lte,
  gte,
  eq
}
