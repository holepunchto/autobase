const path = require('path')
const Hyperschema = require('hyperschema')

const SPEC = './spec/autobase'

const schema = Hyperschema.from(SPEC, { versioned: true })
const autobase = schema.namespace('autobase')

autobase.require(path.join(__dirname, 'lib/legacy-encodings.js'))

autobase.register({
  name: 'checkout',
  compact: true,
  fields: [
    {
      name: 'key',
      type: 'fixed32',
      required: true
    },
    {
      name: 'length',
      type: 'uint',
      required: true
    }
  ]
})

autobase.register({
  name: 'clock',
  array: true,
  type: '@autobase/checkout'
})

autobase.register({
  name: 'indexCheckpoint',
  compact: true,
  fields: [
    {
      name: 'signature',
      type: 'fixed64',
      required: true
    },
    {
      name: 'length',
      type: 'uint',
      required: true
    }
  ]
})

autobase.register({
  name: 'wakeupLegacy',
  external: 'Wakeup'
})

autobase.register({
  name: 'wakeupv2',
  compact: false,
  fields: [
    {
      name: 'type',
      type: 'uint',
      required: true
    },
    {
      name: 'writers',
      type: '@autobase/clock',
      required: false
    }
  ]
})

autobase.register({
  name: 'wakeup',
  versions: [
    {
      version: 1,
      type: '@autobase/wakeupLegacy'
    },
    {
      version: 2,
      type: '@autobase/wakeupv2'
    }
  ]
})

autobase.register({
  name: 'bootRecordLegacy',
  external: 'BootRecord'
})

autobase.register({
  name: 'bootRecordv4',
  compact: false,
  fields: [
    {
      name: 'key',
      type: 'fixed32',
      required: true
    },
    {
      name: 'systemLength',
      type: 'uint',
      required: true
    },
    {
      name: 'indexersUpdated',
      type: 'bool',
      required: true
    },
    {
      name: 'fastForwarding',
      type: 'bool',
      required: true
    },
    {
      name: 'recoveries',
      type: 'uint',
      required: false
    }
  ]
})

autobase.register({
  name: 'bootRecord',
  versions: [
    {
      version: 3,
      type: '@autobase/bootRecordLegacy'
    },
    {
      version: 4,
      type: '@autobase/bootRecordv4'
    }
  ]
})

autobase.register({
  name: 'checkpointer',
  compact: true,
  fields: [
    {
      name: 'checkpointer',
      type: 'uint',
      required: true
    },
    {
      name: 'checkpoint',
      type: '@autobase/indexCheckpoint',
      required: false
    }
  ]
})

autobase.register({
  name: 'checkpointerArray',
  array: true,
  type: '@autobase/checkpointer'
})

autobase.register({
  name: 'checkpoint',
  compact: false,
  fields: [
    {
      name: 'system',
      type: '@autobase/checkpointer',
      required: false
    },
    {
      name: 'encryption',
      type: '@autobase/checkpointer',
      required: false
    },
    {
      name: 'user',
      type: '@autobase/checkpointerArray',
      required: false
    }
  ]
})

autobase.register({
  name: 'indexer',
  compact: true,
  fields: [
    {
      name: 'signature',
      type: 'uint',
      required: true
    },
    {
      name: 'namespace',
      type: 'fixed32',
      required: true
    },
    {
      name: 'publicKey',
      type: 'fixed32',
      required: true
    }
  ]
})

autobase.register({
  name: 'indexers',
  array: true,
  type: '@autobase/indexer'
})

autobase.register({
  name: 'digest',
  compact: false,
  fields: [
    {
      name: 'pointer',
      type: 'uint',
      required: true
    },
    {
      name: 'key',
      type: 'fixed32',
      required: false
    }
  ]
})

autobase.register({
  name: 'node',
  compact: true,
  fields: [
    {
      name: 'heads',
      type: '@autobase/clock',
      required: true
    },
    {
      name: 'batch',
      type: 'uint',
      required: true
    },
    {
      name: 'value',
      type: 'buffer',
      required: true
    }
  ]
})

autobase.register({
  name: 'additionalData',
  compact: false,
  fields: [
    {
      name: 'encryptionId',
      type: 'bool'
    },
    {
      name: 'abi',
      type: 'bool'
    }
  ]
})

autobase.register({
  name: 'additional',
  compact: true,
  fields: [
    {
      name: 'pointer',
      type: 'uint',
      required: true
    },
    {
      name: 'data',
      type: '@autobase/additionalData',
      required: false
    }
  ]
})

autobase.register({
  name: 'oplogMessageLegacy',
  external: 'OplogMessage'
})

autobase.register({
  name: 'oplogMessagev2',
  compact: false,
  fields: [
    {
      name: 'checkpoint',
      type: '@autobase/checkpoint',
      required: false
    },
    {
      name: 'digest',
      type: '@autobase/digest',
      required: false
    },
    {
      name: 'optimistic',
      type: 'bool'
    },
    {
      name: 'node',
      type: '@autobase/node',
      required: true
    }
  ]
})

autobase.register({
  name: 'oplogMessage',
  versions: [
    {
      version: 1,
      type: '@autobase/oplogMessageLegacy'
    },
    {
      version: 2,
      type: '@autobase/oplogMessagev2'
    }
  ]
})

autobase.register({
  name: 'pendingIndexers',
  array: true,
  type: 'fixed32'
})

autobase.register({
  name: 'pendingIndexers',
  array: true,
  type: 'fixed32'
})

autobase.register({
  name: 'infoLegacy',
  external: 'Info'
})

autobase.register({
  name: 'infov2',
  compact: false,
  fields: [
    {
      name: 'members',
      type: 'uint',
      required: true
    },
    {
      name: 'pendingIndexers',
      type: '@autobase/pendingIndexers',
      required: true
    },
    {
      name: 'indexers',
      type: '@autobase/clock',
      required: true
    },
    {
      name: 'heads',
      type: '@autobase/clock',
      required: true
    },
    {
      name: 'views',
      type: '@autobase/clock',
      required: true
    },
    {
      name: 'encryptionLength',
      type: 'uint',
      required: true
    },
    {
      name: 'entropy',
      type: 'fixed32',
      required: false
    }
  ]
})

autobase.register({
  name: 'info',
  versions: [
    {
      version: 1,
      type: '@autobase/infoLegacy'
    },
    {
      version: 2,
      type: '@autobase/infov2'
    }
  ]
})

autobase.register({
  name: 'member',
  compact: false,
  fields: [
    {
      name: 'isIndexer',
      type: 'bool',
      required: true
    },
    {
      name: 'isRemoved',
      type: 'bool',
      required: true
    },
    {
      name: 'length',
      type: 'uint',
      required: true
    }
  ]
})

autobase.register({
  name: 'linearizerKey',
  external: 'LinearizerKey'
})

autobase.register({
  name: 'linearizerUpdate',
  compact: false,
  fields: [
    {
      name: 'key',
      type: 'fixed32',
      required: true
    },
    {
      name: 'length',
      type: 'uint',
      required: true
    },
    {
      name: 'batch',
      type: 'uint',
      required: true
    },
    {
      name: 'systemLength',
      type: 'uint',
      required: true
    },
    {
      name: 'indexers',
      type: 'bool'
    }
  ]
})

autobase.register({
  name: 'encryptionDescriptor',
  compact: true,
  fields: [
    {
      name: 'type',
      type: 'uint',
      required: true
    },
    {
      name: 'version',
      type: 'uint',
      required: true
    },
    {
      name: 'payload',
      type: 'buffer',
      required: true
    }
  ]
})

autobase.register({
  name: 'manifestData',
  compact: false,
  fields: [
    {
      name: 'version',
      type: 'uint',
      required: true
    },
    {
      name: 'legacyBlocks',
      type: 'uint',
      required: false
    }
  ]
})

Hyperschema.toDisk(schema, SPEC)
