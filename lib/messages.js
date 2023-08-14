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
  },
  encode (state, d) {
    Writers.encode(state, d.writers)
    Clock.encode(state, d.heads)
    SystemIndex.encode(state, d.indexes)
  },
  decode (state) {
    return {
      writers: Writers.decode(state),
      heads: Clock.decode(state),
      indexes: SystemIndex.decode(state)
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

const View = {
  preencode (state, m) {
    c.string.preencode(state, m.name)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.string.encode(state, m.name)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    return {
      name: c.string.decode(state),
      length: c.uint.decode(state)
    }
  }
}

const Views = c.array(View)

const MembersDigest = {
  preencode (state, m) {
    c.uint.preencode(state, m.active)
    c.uint.preencode(state, m.total)
  },
  encode (state, m) {
    c.uint.encode(state, m.active)
    c.uint.encode(state, m.total)
  },
  decode (state) {
    return {
      active: c.uint.decode(state),
      total: c.uint.decode(state)
    }
  }
}

const Info = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    MembersDigest.preencode(state, m.members)
    Clock.preencode(state, m.heads)
    Clock.preencode(state, m.indexers)
    Views.preencode(state, m.views)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)
    MembersDigest.encode(state, m.members)
    Clock.encode(state, m.heads)
    Clock.encode(state, m.indexers)
    Views.encode(state, m.views)
  },
  decode (state) {
    return {
      version: c.uint.decode(state),
      members: MembersDigest.decode(state),
      heads: Clock.decode(state),
      indexers: Clock.decode(state),
      views: Views.decode(state)
    }
  }
}

const Member = {
  preencode (state, m) {
    state.end++ // flags
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.uint.encode(state, (m.isWriter ? 1 : 0) | (m.isIndexer ? 2 : 0))
    c.uint.encode(state, m.length)
  },
  decode (state) {
    const flags = c.uint.decode(state)
    return {
      isWriter: (flags & 1) !== 0,
      isIndexer: (flags & 2) !== 0,
      length: c.uint.decode(state)
    }
  }
}

module.exports = {
  FLAG_OPLOG_IS_CHECKPOINTER,
  Clock,
  SystemIndex,
  SystemDigest,
  OplogMessage,
  Checkpoint,
  Info,
  Member
}
