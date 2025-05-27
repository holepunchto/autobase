const schema = require('../encoding/spec/autobase')

module.exports = {
  Wakeup: schema.resolveStruct('@autobase/wakeup'),
  Clock: schema.resolveStruct('@autobase/clock'),
  Checkout: schema.resolveStruct('@autobase/checkout'),
  BootRecord: schema.resolveStruct('@autobase/boot-record'),
  OplogMessage: schema.resolveStruct('@autobase/oplog-message'),
  Checkpoint: schema.resolveStruct('@autobase/checkpoint'),
  Info: schema.resolveStruct('@autobase/info'),
  Member: schema.resolveStruct('@autobase/member'),
  ManifestData: schema.resolveStruct('@autobase/manifest-data'),
  LINEARIZER_PREFIX: 1,
  LinearizerKey: schema.resolveStruct('@autobase/linearizer-key'),
  LinearizerUpdate: schema.resolveStruct('@autobase/linearizer-update'),
  EncryptionDescriptor: schema.resolveStruct('@autobase/encryption-descriptor')
}
