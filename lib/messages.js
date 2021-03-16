const c = require('compact-encoding')

// Header Structs

const HeaderMetadata = {
  preencode (state, req) {
    c.buffer.preencode(state, req && req.userData)
  },
  encode (state, req) {
    c.buffer.encode(state, req && req.userData)
  },
  decode (state) {
    return {
      userData: c.buffer.decode(state)
    }
  }
}

const Header = {
  preencode (state, req) {
    c.string.preencode(state, req.protocol)
    HeaderMetadata.preencode(state, req.metadata)
  },
  encode (state, req) {
    c.string.encode(state, req.protocol)
    HeaderMetadata.encode(state, req.metadata)
  },
  fullEncode (req) {
    return fullEncode(Header, req)
  },
  decode (state) {
    return {
      protocol: c.string.decode(state),
      metadata: HeaderMetadata.decode(state)
    }
  }
}

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
    c.uint.preencode(state, req.size)
    for (const seq of req.values()) {
      state.end += 32
      c.uint.preencode(state, seq)
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

// Input Structs

const InputNode = {
  preencode (state, req) {
    c.buffer.preencode(state, req.value)
    c.bool.preencode(state, req.partial)
    if (!req.partial) Clocks.preencode(state, req.links)
  },
  encode (state, req) {
    c.buffer.encode(state, req.value)
    c.bool.encode(state, req.partial)
    if (!req.partial) Clocks.encode(state, req.links)
  },
  fullEncode (req) {
    return fullEncode(InputNode, req)
  },
  decode (state) {
    const value = c.buffer.decode(state)
    const partial = c.bool.decode(state)
    return {
      value,
      partial,
      links: partial ? new Map() : Clocks.decode(state)
    }
  }
}

// Output Structs

const IndexNode = {
  preencode (state, req) {
    Clocks.preencode(state, req.clock)
    c.uint.preencode(state, req.batch || 1)
    c.buffer.preencode(state, req.value)
    c.fixed32.preencode(state, req.node.key)
    c.uint.preencode(state, req.node.seq)
  },
  encode (state, req) {
    Clocks.encode(state, req.clock)
    c.uint.encode(state, req.batch || 1)
    c.buffer.encode(state, req.value)
    c.fixed32.encode(state, req.node.key)
    c.uint.encode(state, req.node.seq)
  },
  fullEncode (req) {
    return fullEncode(IndexNode, req)
  },
  decode (state) {
    return {
      clock: Clocks.decode(state),
      batch: c.uint.decode(state),
      value: c.buffer.decode(state),
      key: c.fixed32.decode(state),
      seq: c.uint.decode(state),
    }
  }
}

function fullEncode (enc, req) {
  const state = c.state()
  enc.preencode(state, req)
  state.buffer = Buffer.allocUnsafe(state.end)
  enc.encode(state, req)
  return state.buffer
}

function keyMapToArray (m) {
  const arr = []
  for (const [k, v] of m) {
    arr.push({ key: Buffer.from(k, 'hex'), length: v })
  }
  arr.sort(compareKeys)
  return arr
}

function keyArrayToMap (arr) {
  const m = new Map()
  for (const { key, length } of arr) {
    m.set(key.toString('hex'), length)
  }
  return m
}

function compareKeys (a, b) {
  return Buffer.compare(a.key, b.key)
}

module.exports = {
  Header,
  InputNode,
  IndexNode
}
