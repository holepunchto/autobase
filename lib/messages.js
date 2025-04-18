const c = require('compact-encoding')
const assert = require('nanoassert')
const IndexEncoder = require('index-encoder')

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

const KeyV0 = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.key)
  },
  decode (state) {
    return {
      key: c.fixed32.decode(state),
      length: -1
    }
  }
}

const KeysV0 = c.array(KeyV0)

const WakeupV0 = {
  preencode (state, m) {
    c.uint.preencode(state, 0) // version
    c.uint.preencode(state, m.type)

    if (m.type === 1) {
      KeysV0.preencode(state, m.writers)
    }
  },
  encode (state, m) {
    c.uint.encode(state, 0) // version
    c.uint.encode(state, m.type)

    if (m.type === 1) {
      KeysV0.encode(state, m.writers)
    }
  },
  decode (state) {
    const v = c.uint.decode(state)
    if (v !== 0) throw new Error('Unsupported version: ' + v)

    const type = c.uint.decode(state)
    const m = { version: 0, type, writers: null }

    if (m.type === 1) {
      m.writers = KeysV0.decode(state)
    }

    return m
  }
}

const Wakeup = {
  preencode (state, m) {
    if (m.version === 0) return WakeupV0.preencode(state, m)

    c.uint.preencode(state, 1) // version
    c.uint.preencode(state, m.type)

    if (m.type === 1) {
      Clock.preencode(state, m.writers)
    }
  },
  encode (state, m) {
    if (m.version === 0) return WakeupV0.encode(state, m)

    c.uint.encode(state, 1) // version
    c.uint.encode(state, m.type)

    if (m.type === 1) {
      Clock.encode(state, m.writers)
    }
  },
  decode (state) {
    const start = state.start
    const v = c.uint.decode(state)

    if (v > 1) throw new Error('Unsupported version: ' + v)

    if (v === 0) {
      state.start = start
      return WakeupV0.decode(state)
    }

    const type = c.uint.decode(state)
    const m = { version: 1, type, writers: null }

    if (m.type === 1) {
      m.writers = Clock.decode(state)
    }

    return m
  }
}

const InternalViews = c.array(c.uint)

const V0BootRecord = {
  preencode () {
    throw new Error('version 0 records cannot be encoded')
  },
  encode () {
    throw new Error('version 0 records cannot be encoded')
  },
  decode (state) {
    const indexed = Checkout.decode(state)
    const heads = Clock.decode(state)

    // one cause initial recover is not ff recovery
    return { version: 0, key: indexed.key, indexedLength: indexed.length, indexersUpdated: false, fastForwarding: false, recoveries: 1, heads }
  }
}

const BootRecord = {
  preencode (state, m) {
    c.uint.preencode(state, 2) // version
    c.fixed32.preencode(state, m.key)
    InternalViews.preencode(state, m.internalViews)
    c.uint.preencode(state, 1) // always 1b
    if (m.recoveries) c.uint.preencode(state, m.recoveries)
  },
  encode (state, m) {
    c.uint.encode(state, 2) // version
    c.fixed32.encode(state, m.key)
    InternalViews.encode(state, m.internalViews)
    c.uint.encode(state, (m.indexersUpdated ? 1 : 0) | (m.fastForwarding ? 2 : 0) | (m.recoveries ? 4 : 0))
    if (m.recoveries) c.uint.encode(state, m.recoveries)
  },
  decode (state) {
    const v = c.uint.decode(state)
    if (v === 0) return V0BootRecord.decode(state)

    assert(v <= 2, 'Unsupported version: ' + v)

    const key = c.fixed32.decode(state)
    const internalViews = v === 1 ? [c.uint.decode(state)] : InternalViews.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: v,
      key,
      internalViews,
      indexersUpdated: (flags & 1) !== 0,
      fastForwarding: (flags & 2) !== 0,
      recoveries: (flags & 4) !== 0 ? c.uint.decode(state) : 0,
      heads: null // only used for compat
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

const DigestV0 = {
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

const Digest = {
  preencode (state, m) {
    c.uint.preencode(state, m.pointer)
    if (m.pointer === 0) {
      c.fixed32.preencode(state, m.key)
    }
  },
  encode (state, m) {
    c.uint.encode(state, m.pointer)
    if (m.pointer === 0) {
      c.fixed32.encode(state, m.key)
    }
  },
  decode (state) {
    const pointer = c.uint.decode(state)
    return {
      pointer,
      key: pointer === 0 ? c.fixed32.decode(state) : null
    }
  }
}

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
    c.uint.preencode(state, m.maxSupportedVersion)

    const isCheckpointer = m.digest !== null && m.checkpoint !== null

    let flags = 0
    if (isCheckpointer) flags |= 1
    if (m.optimistic) flags |= 2

    c.uint.preencode(state, flags)

    if (isCheckpointer) {
      Checkpoint.preencode(state, m.checkpoint.internal)
      Checkpoint.preencode(state, m.checkpoint.user)
      Digest.preencode(state, m.digest)
    }

    Node.preencode(state, m.node)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)
    c.uint.encode(state, m.maxSupportedVersion)

    const isCheckpointer = m.digest !== null && m.checkpoint !== null

    let flags = 0
    if (isCheckpointer) flags |= 1
    if (m.optimistic) flags |= 2

    c.uint.encode(state, flags)

    if (isCheckpointer) {
      Checkpoint.encode(state, m.checkpoint.internal)
      Checkpoint.encode(state, m.checkpoint.user)
      Digest.encode(state, m.digest)
    }

    Node.encode(state, m.node)
  },
  decode (state) {
    const version = c.uint.decode(state)

    if (version < 2) {
      const m = version === 1
        ? OplogMessageV1.decode(state)
        : OplogMessageV0.decode(state)

      const chk = m.checkpoint

      return {
        version,
        maxSupportedVersion: m.maxSupportedVersion,
        digest: version === 1 ? m.digest : null,
        checkpoint: chk ? { internal: chk.splice(0, 1), user: chk } : null,
        optimistic: version === 1 ? m.optimistic : false,
        node: m.node
      }
    }

    const maxSupportedVersion = c.uint.decode(state)

    const flags = c.uint.decode(state)

    const isCheckpointer = (flags & 1) !== 0

    const checkpoint = isCheckpointer ? {} : null
    if (isCheckpointer) {
      checkpoint.internal = Checkpoint.decode(state)
      checkpoint.user = Checkpoint.decode(state)
    }

    const digest = isCheckpointer ? Digest.decode(state) : null

    const node = Node.decode(state)

    return {
      version,
      maxSupportedVersion,
      digest,
      checkpoint,
      optimistic: (flags & 2) !== 0,
      node
    }
  }
}

const OplogMessageV1 = {
  preencode (state, m) {
    c.uint.preencode(state, m.version)
    c.uint.preencode(state, m.maxSupportedVersion)

    const isCheckpointer = m.digest !== null && m.checkpoint !== null

    let flags = 0
    if (isCheckpointer) flags |= 1
    if (m.optimistic) flags |= 2

    c.uint.preencode(state, flags)

    if (isCheckpointer) {
      Checkpoint.preencode(state, m.checkpoint)
      Digest.preencode(state, m.digest)
    }

    Node.preencode(state, m.node)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)
    c.uint.encode(state, m.maxSupportedVersion)

    assert(m.version <= 1, 'Expected version <= 1')

    const isCheckpointer = m.digest !== null && m.checkpoint !== null

    let flags = 0
    if (isCheckpointer) flags |= 1
    if (m.optimistic) flags |= 2

    c.uint.encode(state, flags)

    if (isCheckpointer) {
      Checkpoint.encode(state, m.checkpoint)
      Digest.encode(state, m.digest)
    }

    Node.encode(state, m.node)
  },
  decode (state) {
    const maxSupportedVersion = c.uint.decode(state)

    const flags = c.uint.decode(state)

    const isCheckpointer = (flags & 1) !== 0

    const checkpoint = isCheckpointer ? Checkpoint.decode(state) : null
    const digest = isCheckpointer ? Digest.decode(state) : null

    const node = Node.decode(state)

    return {
      version: 1,
      maxSupportedVersion,
      digest,
      checkpoint,
      optimistic: (flags & 2) !== 0,
      node
    }
  }
}

const OplogMessageV0 = {
  preencode (state, m) {
    const isCheckpointer = m.digest !== null && m.checkpoint !== null
    c.uint.preencode(state, isCheckpointer ? 1 : 0)

    if (isCheckpointer) {
      DigestV0.preencode(state, m.digest)
      Checkpoint.preencode(state, m.checkpoint)
    }

    Node.preencode(state, m.node)

    Additional.preencode(state, m.additional) // at the btm so it can be edited
    c.uint.preencode(state, m.maxSupportedVersion)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)

    const isCheckpointer = m.digest !== null && m.checkpoint !== null
    c.uint.encode(state, isCheckpointer ? 1 : 0)

    if (isCheckpointer) {
      DigestV0.encode(state, m.digest)
      Checkpoint.encode(state, m.checkpoint)
    }

    Node.encode(state, m.node)

    Additional.encode(state, m.additional)
    c.uint.encode(state, m.maxSupportedVersion)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    const isCheckpointer = (flags & 1) !== 0

    const digest = isCheckpointer ? DigestV0.decode(state) : null
    const checkpoint = isCheckpointer ? Checkpoint.decode(state) : null
    const node = Node.decode(state)
    const additional = Additional.decode(state)
    const maxSupportedVersion = state.start < state.end ? c.uint.decode(state) : 0

    return {
      version: 0,
      digest,
      checkpoint,
      node,
      additional,
      maxSupportedVersion
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

// prefix 0 is reserved for future manifest
const LINEARIZER_PREFIX_V0 = 1
const LINEARIZER_PREFIX = 2

const LinearizerKeyV0 = {
  preencode (state, seq) {
    IndexEncoder.UINT.preencode(state, LINEARIZER_PREFIX_V0)
    IndexEncoder.UINT.preencode(state, seq)
  },
  encode (state, seq) {
    IndexEncoder.UINT.encode(state, LINEARIZER_PREFIX_V0)
    IndexEncoder.UINT.encode(state, seq)
  },
  decode (state) {
    IndexEncoder.UINT.decode(state)
    return IndexEncoder.UINT.decode(state)
  }
}

const LinearizerKey = {
  preencode (state, seq) {
    IndexEncoder.UINT.preencode(state, LINEARIZER_PREFIX)
    IndexEncoder.UINT.preencode(state, seq)
  },
  encode (state, seq) {
    IndexEncoder.UINT.encode(state, LINEARIZER_PREFIX)
    IndexEncoder.UINT.encode(state, seq)
  },
  decode (state) {
    return {
      prefix: IndexEncoder.UINT.decode(state),
      seq: IndexEncoder.UINT.decode(state)
    }
  }
}

const LinearizerUpdateV0 = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.key)
    c.uint.preencode(state, m.length)
    c.uint.preencode(state, m.batch)
    c.uint.preencode(state, m.systemLength)
    c.uint.preencode(state, m.indexers ? 1 : 0)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.key)
    c.uint.encode(state, m.length)
    c.uint.encode(state, m.batch)
    c.uint.encode(state, m.systemLength)
    c.uint.encode(state, m.indexers ? 1 : 0)
  },
  decode (state) {
    const key = c.fixed32.decode(state)
    const length = c.uint.decode(state)
    const batch = c.uint.decode(state)
    const systemLength = c.uint.decode(state)
    const flags = c.uint.decode(state)

    return {
      version: 0,
      seq: 0, // set upstream
      key,
      length,
      batch,
      internalViews: [systemLength],
      indexers: (flags & 1) !== 0
    }
  }
}

const LinearizerUpdate = {
  preencode (state, m) {
    c.uint.preencode(state, 1)
    c.uint.preencode(state, m.indexers ? 1 : 0)
    c.fixed32.preencode(state, m.key)
    c.uint.preencode(state, m.length)
    c.uint.preencode(state, m.batch)
    InternalViews.preencode(state, m.internalViews)
  },
  encode (state, m) {
    c.uint.encode(state, 1)
    c.uint.encode(state, m.indexers ? 1 : 0)
    c.fixed32.encode(state, m.key)
    c.uint.encode(state, m.length)
    c.uint.encode(state, m.batch)
    InternalViews.encode(state, m.internalViews)
  },
  decode (state) {
    const version = c.uint.decode(state)
    if (version > 1) throw new Error('Unsupported version')

    const flags = c.uint.decode(state)
    const key = c.fixed32.decode(state)
    const length = c.uint.decode(state)
    const batch = c.uint.decode(state)
    const internalViews = InternalViews.decode(state)

    return {
      version,
      seq: 0, // set upstream
      key,
      length,
      batch,
      internalViews,
      indexers: (flags & 1) !== 0
    }
  }
}

const EncryptionDescriptor = {
  preencode (state, desc) {
    c.uint.preencode(state, desc.type)
    c.uint.preencode(state, desc.version)
    c.buffer.preencode(state, desc.payload)
  },
  encode (state, desc) {
    c.uint.encode(state, desc.type)
    c.uint.encode(state, desc.version)
    c.buffer.encode(state, desc.payload)
  },
  decode (state) {
    return {
      type: c.uint.decode(state),
      version: c.uint.decode(state),
      payload: c.buffer.decode(state)
    }
  }
}

module.exports = {
  Wakeup,
  Clock,
  Checkout,
  BootRecord,
  OplogMessage,
  Checkpoint,
  Info,
  Member,
  LINEARIZER_PREFIX_V0,
  LINEARIZER_PREFIX,
  LinearizerKeyV0,
  LinearizerUpdateV0,
  LinearizerKey,
  LinearizerUpdate,
  EncryptionDescriptor
}
