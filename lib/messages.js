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
    c.uint.preencode(state, m.batch)
    c.buffer.preencode(state, m.value)
  },
  encode (state, m) {
    Clock.encode(state, m.heads)
    c.uint.encode(state, m.batch)
    c.buffer.encode(state, m.value)
  },
  decode (state, m) {
    return {
      heads: Clock.decode(state),
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

const Additional = {
  preencode (state, m) {
    c.uint.preencode(state, m.pointer)
    if (m.pointer === 0) {
      AdditionalData.preencode(state, m.data)
    }
  },
  encode (state, m) {
    c.uint.encode(state, m.pointer)
    if (m.pointer === 0) {
      AdditionalData.encode(state, m.data)
    }
  },
  decode (state) {
    const pointer = c.uint.decode(state)
    return {
      pointer,
      data: pointer === 0 ? AdditionalData.decode(state) : null
    }
  }
}

const AdditionalData = {
  preencode (state, m) {
    c.uint.preencode(state, 0)
  },
  encode (state, m) {
    c.uint.encode(state, 0) // empty for now, for the future
  },
  decode (state) {
    const flags = c.uint.decode(state)
    return {
      encryptionId: flags & 1 ? c.fixed32.decode(state) : null, // to help validate the encryption key used
      abi: flags & 2 ? c.uint.decode(state) : 0
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

    Additional.preencode(state, m.additional) // at the btm so it can be edited
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

    Additional.encode(state, m.additional)
  },
  decode (state) {
    const version = c.uint.decode(state)
    const flags = c.uint.decode(state)

    const isCheckpointer = (flags & 1) !== 0

    const digest = isCheckpointer ? Digest.decode(state) : null
    const checkpoint = isCheckpointer ? Checkpoint.decode(state) : null
    const node = Node.decode(state)
    const additional = Additional.decode(state)

    return {
      version,
      digest,
      checkpoint,
      node,
      additional
    }
  }
}

const PendingIndexers = c.array(c.fixed32)

const Info = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    c.uint.preencode(state, m.members)
    PendingIndexers.preencode(state, m.pendingIndexers)
    Clock.preencode(state, m.indexers)
    Clock.preencode(state, m.heads)
    Clock.preencode(state, m.views)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)
    c.uint.encode(state, m.members)
    PendingIndexers.encode(state, m.pendingIndexers)
    Clock.encode(state, m.indexers)
    Clock.encode(state, m.heads)
    Clock.encode(state, m.views)
  },
  decode (state) {
    return {
      version: c.uint.decode(state),
      members: c.uint.decode(state),
      pendingIndexers: PendingIndexers.decode(state),
      indexers: Clock.decode(state),
      heads: Clock.decode(state),
      views: Clock.decode(state)
    }
  }
}

const Member = {
  preencode (state, m) {
    state.end++ // flags
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.uint.encode(state, (m.isIndexer ? 1 : 0) | (m.isRemoved ? 2 : 0))
    c.uint.encode(state, m.length)
  },
  decode (state) {
    const flags = c.uint.decode(state)
    return {
      isIndexer: (flags & 1) !== 0,
      isRemoved: (flags & 2) !== 0,
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
