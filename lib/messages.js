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
    Clock.preencode(state, d.writers)
    Clock.preencode(state, d.heads)
  },
  encode (state, d) {
    Clock.encode(state, d.writers)
    Clock.encode(state, d.heads)
  },
  decode (state) {
    return {
      writers: Clock.decode(state),
      heads: Clock.decode(state)
    }
  }
}

const Node = {
  preencode (state, m) {
    Clock.preencode(state, m.heads)
    c.uint.preencode(state, m.abi)
    c.uint.preencode(state, m.batch)
    c.buffer.preencode(state, m.value)
  },
  encode (state, m) {
    Clock.encode(state, m.heads)
    c.uint.encode(state, m.abi)
    c.uint.encode(state, m.batch)
    c.buffer.encode(state, m.value)
  },
  decode (state, m) {
    return {
      heads: Clock.decode(state),
      abi: c.uint.decode(state),
      batch: c.uint.decode(state),
      value: c.buffer.decode(state)
    }
  }
}

const OplogMessage = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    c.uint.preencode(state, m.checkpointer)
    if (m.checkpoint !== null) Checkpoint.preencode(state, m.checkpoint)
    Node.preencode(state, m.node)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)
    c.uint.encode(state, m.checkpointer)
    if (m.checkpoint !== null) Checkpoint.encode(state, m.checkpoint)
    Node.encode(state, m.node)
  },
  decode (state) {
    const version = c.uint.decode(state)
    const checkpointer = c.uint.decode(state)
    const checkpoint = checkpointer ? null : Checkpoint.decode(state)
    const node = Node.decode(state)

    return {
      version,
      checkpointer,
      checkpoint,
      node
    }
  }
}

module.exports = {
  SystemDigest,
  OplogMessage,
  Checkpoint
}
