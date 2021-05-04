const c = require('compact-encoding')

// Common Structs
const Clock = {
  preencode (state, req) {
    c.fixed32.preencode(state, req.key)
    c.uint.preencode(state, req.length)
  },
  encode (state, req) {
    c.fixed32.encode(state, req.key)
    c.uint.encode(state, req.length)
  },
  decode (state) {
    return {
      key: c.fixed32.decode(state),
      length: c.uint.decode(state)
    }
  }
}

const ClockArray = c.array(Clock)

const Clocks = {
  preencode (state, req) {
    if (!req) {
      c.uint.preencode(state, 0)
      return
    }
    c.uint.preencode(state, req.size)
    state.end += req.size * 32
    for (const length of req.values()) {
      c.uint.preencode(state, length)
    }
  },
  encode (state, req) {
    const arr = keyMapToArray(req)
    ClockArray.encode(state, arr)
  },
  decode (state) {
    const arr = ClockArray.decode(state)
    return keyArrayToMap(arr)
  }
}

const Batch = {
  preencode (state, req) {
    c.uint.preencode(state, req[0])
    c.uint.preencode(state, req[1])
  },
  encode (state, req) {
    c.uint.encode(state, req[0])
    c.uint.encode(state, req[1])
  },
  decode (state) {
    return [
      c.uint.decode(state),
      c.uint.decode(state)
    ]
  }
}

const CausalNode = {
  preencode (state, req) {
    Clocks.preencode(state, req.clock)
    c.buffer.preencode(state, req.value)
    Batch.preencode(state, req.batch)
  },
  encode (state, req) {
    Clocks.encode(state, req.clock)
    c.buffer.encode(state, req.value)
    Batch.encode(state, req.batch)
  },
  decode (state) {
    return {
      clock: Clocks.decode(state),
      value: c.buffer.decode(state),
      batch: Batch.decode(state)
    }
  }
}

// Output Structs

const IndexNode = {
  preencode (state, req) {
    c.fixed32.preencode(state, req.change)
    CausalNode.preencode(state, req)
  },
  encode (state, req) {
    c.fixed32.encode(state, req.change)
    CausalNode.encode(state, req)
  },
  decode (state) {
    return {
      change: c.fixed32.decode(state),
      ...CausalNode.decode(state)
    }
  }
}

function keyMapToArray (m) {
  const arr = []
  if (!m) return arr
  for (const [k, v] of m) {
    arr.push({ key: Buffer.from(k, 'hex'), length: v })
  }
  arr.sort(compareKeys)
  return arr
}

function keyArrayToMap (arr) {
  const m = new Map()
  if (!arr) return m
  for (const { key, length } of arr) {
    m.set(key.toString('hex'), length)
  }
  return m
}

function compareKeys (a, b) {
  return Buffer.compare(a.key, b.key)
}

module.exports = {
  CausalNode,
  IndexNode
}
