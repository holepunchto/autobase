const crypto = require('hypercore-crypto')

const DEFAULT_AUTOBASE_VERSION = 1 // default
const MAX_AUTOBASE_VERSION = 2 // optional fork support

const [
  NS_SIGNER_NAMESPACE,
  NS_VIEW_BLOCK_KEY,
  NS_HASH_KEY,
  NS_ENCRYPTION
] = crypto.namespace('autobase', 4)

module.exports = {
  DEFAULT_AUTOBASE_VERSION,
  MAX_AUTOBASE_VERSION,
  NS_SIGNER_NAMESPACE,
  NS_VIEW_BLOCK_KEY,
  NS_HASH_KEY,
  NS_ENCRYPTION
}
