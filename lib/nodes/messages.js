const c = require('compact-encoding')

const Header = {
  preencode (state, req) {
    state.end++ // flags
    if (!req) return
    c.string.preencode(state, req.protocol)
  },
  encode (state, req) {
    if (!req) {
      c.uint.encode(state, 0)
      return
    }
    c.uint.encode(state, 1)
    c.string.encode(state, req.protocol)
  },
  decode (state) {
    const flags = c.uint.decode(state)
    if (!flags) return null
    return {
      protocol: c.string.decode(state)
    }
  }
}

// Common Structs

const Keys = c.array(c.fixed32)
const KeyPointer = {
  preencode (state, o) {
    c.uint.preencode(state, o.seq)
    c.uint.preencode(state, o.offset)
  },
  encode (state, o) {
    c.uint.encode(state, o.seq)
    c.uint.encode(state, o.offset)
  },
  decode (state) {
    return {
      seq: c.uint.decode(state),
      offset: c.uint.decode(state)
    }
  }
}

const Clock = {
  preencode (state, req) {
    KeyPointer.preencode(state, req.key)
    c.uint.preencode(state, req.length)
  },
  encode (state, req) {
    KeyPointer.encode(state, req.key)
    c.uint.encode(state, req.length)
  },
  decode (state) {
    return {
      key: KeyPointer.decode(state),
      length: c.uint.decode(state)
    }
  }
}
const ClockArray = c.array(Clock)

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

const Node = {
  preencode (state, req) {
    state.end++ // bitfield
    if (req.keys) Keys.preencode(state, req.keys)
    if (req.header) Header.preencode(state, req.header)
    if (req.change) KeyPointer.preencode(state, req.change)
    if (req.batch) Batch.preencode(state, req.batch)
    if (req.clock) ClockArray.preencode(state, req.clock)
    if (req.operations) c.uint.preencode(state, req.operations)
    c.buffer.preencode(state, req.value)
  },
  encode (state, req) {
    const s = state.start++
    let bitfield = 0

    if (req.keys && req.keys.length > 0) {
      bitfield |= 1
      Keys.encode(state, req.keys)
    }
    if (req.header) {
      bitfield |= 2
      Header.encode(state, req.header)
    }
    if (req.change) {
      bitfield |= 4
      KeyPointer.encode(state, req.change)
    }
    if (req.batch) {
      bitfield |= 8
      Batch.encode(state, req.batch)
    }
    if (req.clock) {
      bitfield |= 16
      ClockArray.encode(state, req.clock)
    }
    if (req.operations) {
      bitfield |= 32
      c.uint.encode(state, req.operations)
    }

    c.buffer.encode(state, req.value)

    state.buffer[s] = bitfield
  },
  decode (state) {
    const bitfield = c.uint.decode(state)
    return {
      keys: (bitfield & 1) !== 0 ? Keys.decode(state) : null,
      header: (bitfield & 2) !== 0 ? Header.decode(state) : null,
      change: (bitfield & 4) !== 0 ? KeyPointer.decode(state) : null,
      batch: (bitfield & 8) !== 0 ? Batch.decode(state) : null,
      clock: (bitfield & 16) !== 0 ? ClockArray.decode(state) : null,
      operations: (bitfield & 32) !== 0 ? c.uint.decode(state) : null,
      value: c.buffer.decode(state)
    }
  }
}

function decodeHeader (buffer) {
  const state = { start: 0, end: buffer.length, buffer }
  const bitfield = c.uint.decode(state)
  if ((bitfield & 2) === 0) return null
  if ((bitfield & 1) !== 0) {
    const keysLength = c.uint.decode(state)
    state.start += keysLength * 32 // skip over keys
  }
  return Header.decode(state)
}

function decodeKeys (buffer) {
  const state = { start: 0, end: buffer.length, buffer }
  return ((c.uint.decode(state) & 1) !== 0) ? Keys.decode(state) : null
}

module.exports = {
  Header,
  Node,
  decodeHeader,
  decodeKeys
}
