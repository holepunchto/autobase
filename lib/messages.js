const c = require('compact-encoding')

const Checkout = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.key)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    return {
      key: c.fixed32.decode(state),
      length: c.uint.decode(state)
    }
  }
}
const Clock = c.array(Checkout)

const Checkpoint = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.treeHash)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.treeHash)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    return {
      treeHash: c.fixed32.decode(state),
      length: c.uint.decode(state)
    }
  }
}

const SystemDigest = {
  preencode (state, d) {
    Clock.preencode(state, d.indexers)
    Clock.preencode(state, d.heads)
  },
  encode (state, d) {
    Clock.encode(state, d.indexers)
    Clock.encode(state, d.heads)
  },
  decode (state) {
    return {
      indexers: Clock.decode(state),
      heads: Clock.decode(state)
    }
  }
}

const OplogMessage = {
  preencode (state, m) {
    c.uint.preencode(state, 0) // future flags
    Clock.preencode(state, m.heads)
    c.uint.preencode(state, m.batch)
    c.buffer.preencode(state, m.value)
  },
  encode (state, m) {
    c.uint.encode(state, 0) // future flags
    Clock.encode(state, m.heads)
    c.uint.encode(state, m.batch)
    c.buffer.encode(state, m.value)
  },
  decode (state) {
    c.uint.decode(state) // future flags
    return {
      heads: Clock.decode(state),
      batch: c.uint.decode(state),
      value: c.buffer.decode(state)
    }
  }
}

module.exports = {
  SystemDigest,
  OplogMessage,
  Checkpoint
}
