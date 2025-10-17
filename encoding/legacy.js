const c = require('compact-encoding')
const IndexEncoder = require('index-encoder')

const Checkout = {
  preencode(state, m) {
    c.fixed32.preencode(state, m.key)
    c.uint.preencode(state, m.length)
  },
  encode(state, m) {
    c.fixed32.encode(state, m.key)
    c.uint.encode(state, m.length)
  },
  decode(state) {
    return {
      key: c.fixed32.decode(state),
      length: c.uint.decode(state)
    }
  }
}

const Clock = c.array(Checkout)

const IndexCheckpoint = {
  preencode(state, m) {
    c.fixed64.preencode(state, m.signature)
    c.uint.preencode(state, m.length)
  },
  encode(state, m) {
    c.fixed64.encode(state, m.signature)
    c.uint.encode(state, m.length)
  },
  decode(state) {
    return {
      signature: c.fixed64.decode(state),
      length: c.uint.decode(state)
    }
  }
}

const KeyV0 = {
  preencode(state, m) {
    c.fixed32.preencode(state, m.key)
  },
  encode(state, m) {
    c.fixed32.encode(state, m.key)
  },
  decode(state) {
    return {
      key: c.fixed32.decode(state),
      length: -1
    }
  }
}

const KeysV0 = c.array(KeyV0)

const WakeupV0 = {
  preencode(state, m) {
    c.uint.preencode(state, 0) // version
    c.uint.preencode(state, m.type)

    if (m.type === 1) {
      KeysV0.preencode(state, m.writers)
    }
  },
  encode(state, m) {
    c.uint.encode(state, 0) // version
    c.uint.encode(state, m.type)

    if (m.type === 1) {
      KeysV0.encode(state, m.writers)
    }
  },
  decode(state) {
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
  preencode(state, m) {
    if (m.version === 0) return WakeupV0.preencode(state, m)

    c.uint.preencode(state, 1) // version
    c.uint.preencode(state, m.type)

    if (m.type === 1) {
      Clock.preencode(state, m.writers)
    }
  },
  encode(state, m) {
    if (m.version === 0) return WakeupV0.encode(state, m)

    c.uint.encode(state, 1) // version
    c.uint.encode(state, m.type)

    if (m.type === 1) {
      Clock.encode(state, m.writers)
    }
  },
  decode(state) {
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

const BootRecordV0 = {
  preencode() {
    throw new Error('version 0 records cannot be encoded')
  },
  encode() {
    throw new Error('version 0 records cannot be encoded')
  },
  decode(state) {
    const indexed = Checkout.decode(state)
    const heads = Clock.decode(state)

    // one cause initial recover is not ff recovery
    return {
      version: 0,
      key: indexed.key,
      systemLength: indexed.length,
      indexersUpdated: false,
      fastForwarding: false,
      recoveries: 1,
      migrating: false,
      heads
    }
  }
}

const Checkpointer = {
  preencode(state, idx) {
    c.uint.preencode(state, idx.checkpointer)
    if (idx.checkpoint !== null) IndexCheckpoint.preencode(state, idx.checkpoint)
  },
  encode(state, idx) {
    c.uint.encode(state, idx.checkpointer)
    if (idx.checkpoint !== null) IndexCheckpoint.encode(state, idx.checkpoint)
  },
  decode(state) {
    const checkpointer = c.uint.decode(state)
    const checkpoint = checkpointer ? null : IndexCheckpoint.decode(state)

    return {
      checkpointer,
      checkpoint
    }
  }
}

const CheckpointerArray = c.array(Checkpointer)

const Indexer = {
  preencode(state, m) {
    c.uint.preencode(state, m.signature)
    c.fixed32.preencode(state, m.namespace)
    c.fixed32.preencode(state, m.publicKey)
  },
  encode(state, m) {
    c.uint.encode(state, m.signature)
    c.fixed32.encode(state, m.namespace)
    c.fixed32.encode(state, m.publicKey)
  },
  decode(state) {
    return {
      signature: c.uint.decode(state),
      namespace: c.fixed32.decode(state),
      publicKey: c.fixed32.decode(state)
    }
  }
}

const Indexers = c.array(Indexer)

const DigestV0 = {
  preencode(state, m) {
    c.uint.preencode(state, m.pointer)
    if (m.pointer === 0) {
      Indexers.preencode(state, m.indexers)
    }
  },
  encode(state, m) {
    c.uint.encode(state, m.pointer)
    if (m.pointer === 0) {
      Indexers.encode(state, m.indexers)
    }
  },
  decode(state) {
    const pointer = c.uint.decode(state)
    return {
      pointer,
      indexers: pointer === 0 ? Indexers.decode(state) : null
    }
  }
}

const Digest = {
  preencode(state, m) {
    c.uint.preencode(state, m.pointer)
    if (m.pointer === 0) {
      c.fixed32.preencode(state, m.key)
    }
  },
  encode(state, m) {
    c.uint.encode(state, m.pointer)
    if (m.pointer === 0) {
      c.fixed32.encode(state, m.key)
    }
  },
  decode(state) {
    const pointer = c.uint.decode(state)
    return {
      pointer,
      key: pointer === 0 ? c.fixed32.decode(state) : null
    }
  }
}

const Node = {
  preencode(state, m) {
    Clock.preencode(state, m.heads)
    c.uint.preencode(state, m.batch)
    c.buffer.preencode(state, m.value)
  },
  encode(state, m) {
    Clock.encode(state, m.heads)
    c.uint.encode(state, m.batch)
    c.buffer.encode(state, m.value)
  },
  decode(state, m) {
    return {
      heads: Clock.decode(state),
      batch: c.uint.decode(state),
      value: c.buffer.decode(state)
    }
  }
}

const Additional = {
  preencode(state, m) {
    c.uint.preencode(state, m.pointer)
    if (m.pointer === 0) {
      AdditionalData.preencode(state, m.data)
    }
  },
  encode(state, m) {
    c.uint.encode(state, m.pointer)
    if (m.pointer === 0) {
      AdditionalData.encode(state, m.data)
    }
  },
  decode(state) {
    const pointer = c.uint.decode(state)
    return {
      pointer,
      data: pointer === 0 ? AdditionalData.decode(state) : null
    }
  }
}

const AdditionalData = {
  preencode(state, m) {
    c.uint.preencode(state, 0)
  },
  encode(state, m) {
    c.uint.encode(state, 0) // empty for now, for the future
  },
  decode(state) {
    const flags = c.uint.decode(state)
    return {
      encryptionId: flags & 1 ? c.fixed32.decode(state) : null, // to help validate the encryption key used
      abi: flags & 2 ? c.uint.decode(state) : 0
    }
  }
}

const OplogMessageV1 = {
  preencode(state, m) {
    throw new Error('Encoding not supported')
  },
  encode(state, m) {
    throw new Error('Encoding not supported')
  },
  decode(state) {
    const maxSupportedVersion = c.uint.decode(state)

    const flags = c.uint.decode(state)

    const isCheckpointer = (flags & 1) !== 0

    const chk = isCheckpointer ? CheckpointerArray.decode(state) : null
    const digest = isCheckpointer ? Digest.decode(state) : null

    const node = Node.decode(state)

    return {
      version: 1,
      maxSupportedVersion,
      digest,
      checkpoint: chk ? { system: chk[0], encryption: null, user: chk.slice(1) } : null,
      optimistic: (flags & 2) !== 0,
      node
    }
  }
}

const OplogMessageV0 = {
  preencode(state, m) {
    throw new Error('Encoding not supported')
  },
  encode(state, m) {
    throw new Error('Encoding not supported')
  },
  decode(state) {
    const flags = c.uint.decode(state)

    const isCheckpointer = (flags & 1) !== 0

    if (isCheckpointer) DigestV0.decode(state)
    const chk = isCheckpointer ? CheckpointerArray.decode(state) : null
    const node = Node.decode(state)
    Additional.decode(state)
    const maxSupportedVersion = state.start < state.end ? c.uint.decode(state) : 0

    return {
      version: 0,
      maxSupportedVersion,
      digest: null,
      checkpoint: chk ? { system: chk[0], encryption: null, user: chk.slice(1) } : null,
      optimistic: null,
      node
    }
  }
}

// prefix 0 is reserved for future manifest
const LINEARIZER_PREFIX = 1

const LinearizerKey = {
  preencode(state, seq) {
    IndexEncoder.UINT.preencode(state, LINEARIZER_PREFIX)
    IndexEncoder.UINT.preencode(state, seq)
  },
  encode(state, seq) {
    IndexEncoder.UINT.encode(state, LINEARIZER_PREFIX)
    IndexEncoder.UINT.encode(state, seq)
  },
  decode(state) {
    IndexEncoder.UINT.decode(state)
    return IndexEncoder.UINT.decode(state)
  }
}

function infoLegacyMap(info) {
  return {
    version: info.version,
    members: info.members,
    pendingIndexers: info.pendingIndexers,
    indexers: info.indexers,
    heads: info.heads,
    views: info.views,
    encryptionLength: 0,
    entropy: null
  }
}

module.exports = {
  Wakeup,
  BootRecordV0,
  OplogMessageV0,
  OplogMessageV1,
  LinearizerKey,
  infoLegacyMap
}
