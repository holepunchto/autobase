const b4a = require('b4a')
const assert = require('nanoassert')

module.exports = class TopoList {
  constructor () {
    this.tip = []
    this.undo = 0
    this.shared = 0
  }

  static compare (a, b) {
    return cmp(a, b)
  }

  static add (node, indexed, offset) {
    addSortedOptimistic(node, indexed, offset)
  }

  mark () {
    this.shared = this.tip.length
    this.undo = 0
  }

  // todo: bump to new api that just tracks undo
  flush (indexed = []) {
    if (indexed.length) this._applyIndexed(indexed)

    const u = {
      shared: this.shared,
      undo: this.undo,
      length: indexed.length + this.tip.length,
      indexed,
      tip: this.tip
    }

    this.mark()

    return u
  }

  print () {
    return this.tip.map(n => n.writer.core.key.toString() + n.length)
  }

  _applyIndexed (nodes) {
    assert(nodes.length <= this.tip.length, 'Indexed batch cannot exceed tip')

    let shared = 0

    for (; shared < nodes.length; shared++) {
      if (this.tip[shared] !== nodes[shared]) break
    }

    // reordering
    if (shared < nodes.length) this._track(shared)

    const tip = []

    for (let i = shared; i < this.tip.length; i++) {
      const node = this.tip[i]
      if (node.yielded) continue
      const s = addSortedOptimistic(node, tip, 0)
      if (s === tip.length - 1) continue
      this._track(shared + s)
    }

    this.tip = tip
  }

  add (node) {
    const shared = addSortedOptimistic(node, this.tip, 0)
    this._track(shared)
  }

  _track (shared) {
    if (shared < this.shared) {
      this.undo += this.shared - shared
      this.shared = shared
    }
  }
}

function hasOptimisticNodes (node) {
  if (node.optimistic) return true

  for (const d of node.dependencies) {
    if (d.optimistic) return true
  }

  return false
}

function getOptimisticDeps (node) {
  const deps = new Set()
  const stack = [node]

  while (stack.length > 0) {
    const next = stack.pop()
    if (deps.has(next)) continue
    if (next.optimistic) deps.add(next)
    for (const d of next.dependencies) {
      if (d.optimistic) stack.push(d)
    }
  }

  return deps
}

function sortOptimisticUntilSettled (list, offset) {
  let n = 0
  while (true) {
    n++
    if (n > 1e3) {
      console.log('no2')
      process.exit()
    }
    let settled = true
    for (let i = 0; i < list.length; i++) {
      const n = list[i]
      // if (deps.has(n)) continue
      if (!n.optimistic) continue
      const shared = sortOptimistic(n, list, i, offset)
      if (shared === i) continue
      settled = false
    }
    if (settled) break
  }
  // if (n > 2) console.log('opt', n)
}

function sortNonOptimisticUntilSettled (list, offset) {
  let n = 0
  while (true) {
    n++
    if (n > 1e3) {
      console.log('no')
      process.exit()
    }
    let settled = true
    for (let i = list.length - 1; i >= offset; i--) {
      const n = list[i]
      const shared = sortNonOptimistic(n, list, i, offset)
      if (shared === i) continue
      settled = false
    }
    if (settled) break
  }
  // if (n > 2) console.log('non-opt', n)
}

function addSortedOptimistic (node, list, offset) {
  // return addSorted(node, list, offset)
  // if (!hasOptimisticNodes(node)) return addSorted(node, list, offset)

  if (node.optimistic) {
    list.push(node)
    sortOptimistic(node, list, list.length - 1, offset)
    // if (!node.dependencies.length) return
    // if (node.dependencies.every(n => !n.optimistic)) return

    // for (const n of node.dependencies) {
    //   if (n.optimistic) sortOptimistic(n, list, list.indexOf(n), offset)
    // }

    // console.log('--')

    // const d = getOptimisticDeps(node)
    // for (let i = 0; i < list.length; i++) {
    //   const n = list[i]
    //   if (d.has(n)) sortOptimistic(n, list, i, offset)
    // }
    sortOptimisticUntilSettled(list, offset)
    return
  }

  // if (getOptimisticDeps(node).size === 0) {
  //   // sortNonOptimisticUntilSettled(list, offset)
  //   list.push(node)
  //   sortNonOptimistic(node, list, list.length - 1, offset) // TODO: track
  //   // sortOptimisticUntilSettled(list, offset)
  //   return
  // }

  sortNonOptimisticUntilSettled(list, offset)

  list.push(node)
  sortNonOptimistic(node, list, list.length - 1, offset) // TODO: track

  sortOptimisticUntilSettled(list, offset)

  let shared = offset
  return shared
}

function addSorted (node, list, offset) {
  list.push(node)
  return sortNonOptimistic(node, list, list.length - 1, offset)
}

// function sortNonOptimistic (node, list, i, offset) {
//   const stack = [{ i, node }]

//   while (stack.length) {
//     const { i, node } = stack.pop()
//     let continuePhase = true

//     while (i > offset) {
//       const prev = list[i - 1]
//       if (links(node, prev)) {
//         if (prev.optimistic) {
//           stack.push({ i, node })
//           stack.push({ i: i - 1, prev })
//           continuePhase = false
//         }
//         break
//       }
//       list[i] = prev
//       list[--i] = node
//     }

//     if (!continuePhase) {
//       continue
//     }

//     while (i < list.length - 1) {
//       const next = list[i + 1]
//       const c = cmp(node, next)
//       if (c <= 0) break
//       list[i] = next
//       list[++i] = node
//     }
//   }
// }

global.sortNonOptimistic = sortNonOptimistic
global.sortOptimistic = sortOptimistic

function sortNonOptimistic (node, list, i, offset) {
  while (i > offset) {
    const prev = list[i - 1]
    if (links(node, prev)) {
      break
    }
    list[i] = prev
    list[--i] = node
  }

  while (i < list.length - 1) {
    const next = list[i + 1]
    const c = cmp(node, next)
    if (c <= 0) break
    list[i] = next
    list[++i] = node
  }

  return i
}

function sortOptimistic (node, list, i, offset, debug) {
  while (i < list.length - 1) {
    const next = list[i + 1]
    if (links(next, node)) {
      break
    }
    list[i] = next
    list[++i] = node
  }


  while (i > offset) {
    const prev = list[i - 1]
    const c = cmp(prev, node)
    if (c <= 0) break
    list[i] = prev
    list[--i] = node
  }

  return i
}

function links (a, b) {
  if (b.dependents.has(a)) return true
  return a.length > 0 && b.length === a.length - 1 && a.writer === b.writer
}

function cmp (a, b) {
  return links(b, a) ? -1 : links(a, b) ? 1 : cmpUnlinked(a, b)
}

function cmpUnlinked (a, b) {
  const c = b4a.compare(a.writer.core.key, b.writer.core.key)

  if (c !== 0) {
    // a node marked for optimistic execution always sorts AFTER one who didnt for security
    if (a.optimistic !== b.optimistic) return a.optimistic ? 1 : -1
  }

  return c === 0 ? (a.length < b.length ? -1 : 1) : c
}
