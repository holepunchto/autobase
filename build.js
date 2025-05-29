const path = require('path')
const Hyperschema = require('hyperschema')

const DIR = path.join(__dirname, 'encoding')
const SPEC = path.join(DIR, 'spec/autobase')

const schema = Hyperschema.from(SPEC, { versioned: true })
const autobase = schema.namespace('autobase')

autobase.require(path.join(DIR, 'legacy.js'))

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
  compact: true,
  type: '@autobase/checkout'
})

autobase.register({
  name: 'index-checkpoint',
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
  name: 'boot-record-v0',
  external: 'BootRecordV0'
})

autobase.register({
  name: 'boot-record-raw',
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
      required: false
    },
    {
      name: 'fastForwarding',
      type: 'bool',
      required: false
    },
    {
      name: 'recoveries',
      type: 'uint',
      required: false
    },
    {
      name: 'migrating',
      type: 'bool',
      required: false
    }
  ]
})

autobase.register({
  name: 'boot-record',
  versions: [
    {
      version: 0,
      type: '@autobase/boot-record-v0'
    },
    {
      version: 3,
      type: '@autobase/boot-record-raw'
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
      required: false
    },
    {
      name: 'checkpoint',
      type: '@autobase/index-checkpoint',
      required: false
    }
  ]
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
      type: '@autobase/checkpointer',
      array: true,
      required: false
    }
  ]
})

autobase.register({
  name: 'digest',
  compact: false,
  fields: [
    {
      name: 'pointer',
      type: 'uint',
      required: false
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
  name: 'oplog-message-v0',
  external: 'OplogMessageV0'
})

autobase.register({
  name: 'oplog-message-v1',
  external: 'OplogMessageV1'
})

autobase.register({
  name: 'oplog-message-v2',
  compact: false,
  fields: [
    {
      name: 'node',
      type: '@autobase/node',
      required: true
    },
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
      type: 'bool',
      required: false
    }
  ]
})

autobase.register({
  name: 'oplog-message',
  versions: [
    {
      version: 0,
      type: '@autobase/oplog-message-v0'
    },
    {
      version: 1,
      type: '@autobase/oplog-message-v1'
    },
    {
      version: 2,
      type: '@autobase/oplog-message-v2'
    }
  ]
})

autobase.register({
  name: 'info-v1',
  fields: [
    {
      name: 'members',
      type: 'uint',
      required: true
    },
    {
      name: 'pendingIndexers',
      type: 'fixed32',
      array: true,
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
  name: 'info-v2',
  fields: [
    {
      name: 'members',
      type: 'uint',
      required: true
    },
    {
      name: 'pendingIndexers',
      type: 'fixed32',
      array: true,
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
      type: '@autobase/info-v1',
      map: 'infoLegacyMap',
      version: 1
    },
    {
      type: '@autobase/info-v2',
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
  name: 'linearizer-key',
  external: 'LinearizerKey'
})

autobase.register({
  name: 'linearizer-update',
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
      type: 'bool',
      required: false
    }
  ]
})

autobase.register({
  name: 'encryption-descriptor',
  fields: [
    {
      name: 'type',
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
  name: 'manifest-data',
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
