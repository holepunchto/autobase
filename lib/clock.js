function lt (clock1, clock2) {
  if (!clock1.size || !clock2.size || clock1 === clock2) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length >= clock2.get(key)) return false
  }
  return true
}

function lte (clock1, clock2) {
  if (clock1.size !== clock2.size) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length > clock2.get(key)) return false
  }
  return true
}

function gte (clock1, clock2) {
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length < clock2.get(key)) return false
  }
  return true
}

function eq (clock1, clock2) {
  if (clock1.size !== clock2.size) return false
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) return false
    if (length !== clock2.get(key)) return false
  }
  return true
}

function length (clock) {
  let length = 0
  for (const l of clock.values()) {
    length += l + 1
  }
  return length
}

function greatestCommonAncestor (clock1, clock2) {
  const ancestor = new Map()
  for (const [key, length] of clock1) {
    if (!clock2.has(key)) continue
    ancestor.set(key, Math.min(length, clock2.get(key)))
  }
  return ancestor
}

module.exports = {
  lt,
  lte,
  gte,
  eq,
  length,
  greatestCommonAncestor
}
