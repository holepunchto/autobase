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

// Input Structs

const InputNode = {
  preencode (state, req) {
    ClockArray.preencode(state, req.links)
    c.buffer.preencode(state, req.value)
    c.bool.preencode(state, req.partial)
  },
  encode (state, req) {
    ClockArray.encode(state, req.links)
    c.buffer.encode(state, req.value)
    c.bool.encode(state, req.partial)
  },
  fullEncode (req) {
    return fullEncode(InputNode, req)
  },
  decode (state) {
    return {
      links: ClockArray.decode(state),
      value: c.buffer.decode(state),
      partial: c.bool.decode(state)
    }
  }
}

// Output Structs

const IndexNode = {
  preencode (state, req) {
    ClockArray.preencode(state, req.clock)
    c.fixed32.preencode(state, req.key)
    c.uint.preencode(state, req.seq)
    c.uint.preencode(state, req.batch || 1)
    c.buffer.preencode(state, req.value)
  },
  encode (state, req) {
    ClockArray.encode(state, req.clock)
    c.fixed32.encode(state, req.key)
    c.uint.encode(state, req.seq)
    c.uint.encode(state, req.batch || 1)
    c.buffer.encode(state, req.value)
  },
  fullEncode (req) {
    return fullEncode(IndexNode, req)
  },
  decode (state) {
    return {
      clock: ClockArray.decode(state),
      key: c.fixed32.decode(state),
      seq: c.uint.decode(state),
      batch: c.uint.decode(state),
      value: c.buffer.decode(state)
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

module.exports = {
  Header,
  InputNode,
  IndexNode
}
