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

const SystemPointer = {
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
    c.fixed64.preencode(state, m.signature)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.fixed64.encode(state, m.signature)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    return {
      signature: c.fixed64.decode(state),
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

const Indexer = {
  preencode (state, m) {
    c.uint.preencode(state, m.signature)
    c.fixed32.preencode(state, m.namespace)
    c.fixed32.preencode(state, m.publicKey)
  },
  encode (state, m) {
    c.uint.encode(state, m.signature)
    c.fixed32.encode(state, m.namespace)
    c.fixed32.encode(state, m.publicKey)
  },
  decode (state) {
    return {
      signature: c.uint.decode(state),
      namespace: c.fixed32.decode(state),
      publicKey: c.fixed32.decode(state)
    }
  }
}
const Indexers = c.array(Indexer)

const Digest = {
  preencode (state, m) {
    c.uint.preencode(state, m.pointer)
    if (m.pointer === 0) {
      Indexers.preencode(state, m.indexers)
    }
  },
  encode (state, m) {
    c.uint.encode(state, m.pointer)
    if (m.pointer === 0) {
      Indexers.encode(state, m.indexers)
    }
  },
  decode (state) {
    const pointer = c.uint.decode(state)
    return {
      pointer,
      indexers: pointer === 0 ? Indexers.decode(state) : null
    }
  }
}

const OplogMessage = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)

    const isCheckpointer = m.digest !== null && m.checkpoint !== null
    c.uint.preencode(state, isCheckpointer ? 1 : 0)

    if (isCheckpointer) {
      Digest.preencode(state, m.digest)
      Checkpoint.preencode(state, m.checkpoint)
    }

    Node.preencode(state, m.node)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)

    const isCheckpointer = m.digest !== null && m.checkpoint !== null
    c.uint.encode(state, isCheckpointer ? 1 : 0)

    if (isCheckpointer) {
      Digest.encode(state, m.digest)
      Checkpoint.encode(state, m.checkpoint)
    }

    Node.encode(state, m.node)
  },
  decode (state) {
    const version = c.uint.decode(state)
    const flags = c.uint.decode(state)

    const isCheckpointer = (flags & 1) !== 0

    const digest = isCheckpointer ? Digest.decode(state) : null
    const checkpoint = isCheckpointer ? Checkpoint.decode(state) : null
    const node = Node.decode(state)

    return {
      version,
      digest,
      checkpoint,
      node
    }
  }
}

const View = {
  preencode (state, m) {
    c.uint.preencode(state, m.length)
    c.fixed32.preencode(state, m.key)
  },
  encode (state, m) {
    c.uint.encode(state, m.length)
    c.fixed32.encode(state, m.key)
  },
  decode (state) {
    return {
      length: c.uint.decode(state),
      key: c.fixed32.decode(state)
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

const PendingIndexers = c.array(c.fixed32)

const Info = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    MembersDigest.preencode(state, m.members)
    PendingIndexers.preencode(state, m.pendingIndexers)
    Clock.preencode(state, m.indexers)
    Clock.preencode(state, m.heads)
    Views.preencode(state, m.views)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)
    MembersDigest.encode(state, m.members)
    PendingIndexers.encode(state, m.pendingIndexers)
    Clock.encode(state, m.indexers)
    Clock.encode(state, m.heads)
    Views.encode(state, m.views)
  },
  decode (state) {
    return {
      version: c.uint.decode(state),
      members: MembersDigest.decode(state),
      pendingIndexers: PendingIndexers.decode(state),
      indexers: Clock.decode(state),
      heads: Clock.decode(state),
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
  Clock,
  Checkout,
  SystemPointer,
  OplogMessage,
  Checkpoint,
  Info,
  Member
}
