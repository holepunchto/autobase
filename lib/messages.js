const c = require('compact-encoding')

const FLAG_OPLOG_IS_CHECKPOINTER = 0b1

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

const IndexCheckpoint = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.publicKey)
    c.fixed32.preencode(state, m.treeHash)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.publicKey)
    c.fixed32.encode(state, m.treeHash)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    return {
      publicKey: c.fixed32.decode(state),
      treeHash: c.fixed32.decode(state),
      length: c.uint.decode(state)
    }
  }
}

const Checkpoint = c.array({
  preencode (state, idx) {
    c.uint.preencode(state, idx.checkpointer)
    if (idx.checkpoint !== null) IndexCheckpoint.preencode(state, idx.checkpoint)
  },
  encode (state, idx) {
    c.uint.encode(state, idx.checkpointer)
    if (idx.checkpoint !== null) IndexCheckpoint.encode(state, idx.checkpoint)
  },
  decode (state) {
    const checkpointer = c.uint.decode(state)
    const checkpoint = checkpointer ? null : IndexCheckpoint.decode(state)

    return {
      checkpointer,
      checkpoint
    }
  }
})

const SystemIndex = c.array(c.string)

const Writer = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
    c.uint.preencode(state, m.length)
    c.bool.preencode(state, m.indexer)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.key)
    c.uint.encode(state, m.length)
    c.bool.encode(state, m.indexer)
  },
  decode (state) {
    return {
      key: c.fixed32.decode(state),
      length: c.uint.decode(state),
      indexer: c.bool.decode(state)
    }
  }
}
const Writers = c.array(Writer)

const SystemDigest = {
  preencode (state, d) {
    Writers.preencode(state, d.writers)
    Clock.preencode(state, d.heads)
    SystemIndex.preencode(state, d.indexes)
    Clock.preencode(state, d.indexerHeads)
  },
  encode (state, d) {
    Writers.encode(state, d.writers)
    Clock.encode(state, d.heads)
    SystemIndex.encode(state, d.indexes)
    Clock.encode(state, d.indexerHeads)
  },
  decode (state) {
    return {
      writers: Writers.decode(state),
      heads: Clock.decode(state),
      indexes: SystemIndex.decode(state),
      indexerHeads: Clock.decode(state)
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
    const isCheckpointer = m.flags & FLAG_OPLOG_IS_CHECKPOINTER
    c.uint.preencode(state, m.flags)
    c.uint.preencode(state, m.version)
    if (isCheckpointer) Checkpoint.preencode(state, m.checkpoint)
    Node.preencode(state, m.node)
  },
  encode (state, m) {
    const isCheckpointer = m.flags & FLAG_OPLOG_IS_CHECKPOINTER
    c.uint.encode(state, m.flags)
    c.uint.encode(state, m.version)
    if (isCheckpointer) Checkpoint.encode(state, m.checkpoint)
    Node.encode(state, m.node)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    const isCheckpointer = flags & FLAG_OPLOG_IS_CHECKPOINTER

    const version = c.uint.decode(state)
    const checkpoint = isCheckpointer ? Checkpoint.decode(state) : []
    const node = Node.decode(state)

    return {
      flags,
      version,
      checkpoint,
      node
    }
  }
}

module.exports = {
  FLAG_OPLOG_IS_CHECKPOINTER,
  Clock,
  SystemIndex,
  SystemDigest,
  OplogMessage,
  Checkpoint
}
