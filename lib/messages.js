const schema = require('../spec/autobase')

module.exports = {
  Wakeup: schema.resolveStruct('@autobase/wakeup'),
  Clock: schema.resolveStruct('@autobase/clock'),
  Checkout: schema.resolveStruct('@autobase/checkout'),
  BootRecord: schema.resolveStruct('@autobase/bootRecord'),
  OplogMessage: schema.resolveStruct('@autobase/oplogMessage'),
  Checkpoint: schema.resolveStruct('@autobase/checkpoint'),
  Info: schema.resolveStruct('@autobase/info'),
  Member: schema.resolveStruct('@autobase/member'),
  ManifestData: schema.resolveStruct('@autobase/manifestData'),
  LINEARIZER_PREFIX: 1,
  LinearizerKey: schema.resolveStruct('@autobase/linearizerKey'),
  LinearizerUpdate: schema.resolveStruct('@autobase/linearizerUpdate'),
  EncryptionDescriptor: schema.resolveStruct('@autobase/encryptionDescriptor')
}
