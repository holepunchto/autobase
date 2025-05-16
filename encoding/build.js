const path = require('path')
const Hyperschema = require('hyperschema')

const SPEC = path.join(__dirname, 'spec/autobase')

const schema = Hyperschema.from(SPEC, { versioned: true })
const autobase = schema.namespace('autobase')

autobase.require(path.join(__dirname, './legacy.js'))

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
  name: 'wakeup',
  external: 'Wakeup'
})

autobase.register({
  name: 'bootRecordV0',
  external: 'BootRecordV0'
})

autobase.register({
  name: 'bootRecordRaw',
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
      version: 0,
      type: '@autobase/bootRecordV0'
    },
    {
      version: 3,
      type: '@autobase/bootRecordRaw'
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
  name: 'oplogMessageV0',
  external: 'OplogMessageV0'
})

autobase.register({
  name: 'oplogMessageV1',
  external: 'OplogMessageV1'
})

autobase.register({
  name: 'oplogMessageV2',
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
      version: 0,
      type: '@autobase/oplogMessageV0'
    },
    {
      version: 1,
      type: '@autobase/oplogMessageV1'
    },
    {
      version: 2,
      type: '@autobase/oplogMessageV2'
    }
  ]
})

autobase.register({
  name: 'pendingIndexers',
  compact: true,
  array: true,
  type: 'fixed32'
})

autobase.register({
  name: 'infoLegacy',
  external: 'Info'
})

autobase.register({
  name: 'infoV1',
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
    }
  ]
})

autobase.register({
  name: 'infoV2',
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
      type: '@autobase/infoV1',
      map: 'infoLegacyMap',
      version: 1
    },
    {
      type: '@autobase/infoV2',
      version: 2
    }
  ]
})

autobase.register({
  name: 'member',
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
