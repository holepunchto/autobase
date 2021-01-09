const sodium = require('sodium-universal')
const uint64be = require('uint64be')

// https://en.wikipedia.org/wiki/Merkle_tree#Second_preimage_attack
const LEAF_TYPE = Buffer.from([0])
const PARENT_TYPE = Buffer.from([1])
const ROOT_TYPE = Buffer.from([2])
const CAP_TYPE = Buffer.from([3])

const HYPERCORE = Buffer.from('hypercore')
const HYPERCORE_CAP = Buffer.from('hypercore capability')

exports.writerCapability = function (key, secretKey, split) {
  if (!split) return null

  const out = Buffer.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, [
    CAP_TYPE,
    HYPERCORE_CAP,
    split.tx.slice(0, 32),
    key
  ], split.rx.slice(0, 32))

  return exports.sign(out, secretKey)
}

exports.verifyRemoteWriterCapability = function (key, cap, split) {
  if (!split) return null

  const out = Buffer.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, [
    CAP_TYPE,
    HYPERCORE_CAP,
    split.rx.slice(0, 32),
    key
  ], split.tx.slice(0, 32))

  return exports.verify(out, cap, key)
}

// TODO: add in the CAP_TYPE in a future version
exports.capability = function (key, split) {
  if (!split) return null

  const out = Buffer.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, [
    HYPERCORE_CAP,
    split.tx.slice(0, 32),
    key
  ], split.rx.slice(0, 32))

  return out
}

// TODO: add in the CAP_TYPE in a future version
exports.remoteCapability = function (key, split) {
  if (!split) return null

  const out = Buffer.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, [
    HYPERCORE_CAP,
    split.rx.slice(0, 32),
    key
  ], split.tx.slice(0, 32))

  return out
}

exports.keyPair = function (seed) {
  const publicKey = Buffer.allocUnsafe(sodium.crypto_sign_PUBLICKEYBYTES)
  const secretKey = Buffer.allocUnsafe(sodium.crypto_sign_SECRETKEYBYTES)

  if (seed) sodium.crypto_sign_seed_keypair(publicKey, secretKey, seed)
  else sodium.crypto_sign_keypair(publicKey, secretKey)

  return {
    publicKey,
    secretKey
  }
}

exports.sign = function (message, secretKey) {
  const signature = Buffer.allocUnsafe(sodium.crypto_sign_BYTES)
  sodium.crypto_sign_detached(signature, message, secretKey)
  return signature
}

exports.verify = function (message, signature, publicKey) {
  return sodium.crypto_sign_verify_detached(signature, message, publicKey)
}

exports.data = function (data) {
  const out = Buffer.allocUnsafe(32)

  sodium.crypto_generichash_batch(out, [
    LEAF_TYPE,
    encodeUInt64(data.length),
    data
  ])

  return out
}

exports.leaf = function (leaf) {
  return exports.data(leaf.data)
}

exports.parent = function (a, b) {
  if (a.index > b.index) {
    const tmp = a
    a = b
    b = tmp
  }

  const out = Buffer.allocUnsafe(32)

  sodium.crypto_generichash_batch(out, [
    PARENT_TYPE,
    encodeUInt64(a.size + b.size),
    a.hash,
    b.hash
  ])

  return out
}

exports.tree = function (roots, out) {
  const buffers = new Array(3 * roots.length + 1)
  var j = 0

  buffers[j++] = ROOT_TYPE

  for (var i = 0; i < roots.length; i++) {
    const r = roots[i]
    buffers[j++] = r.hash
    buffers[j++] = encodeUInt64(r.index)
    buffers[j++] = encodeUInt64(r.size)
  }

  if (!out) out = Buffer.allocUnsafe(32)
  sodium.crypto_generichash_batch(out, buffers)
  return out
}

exports.signable = function (roots, length) {
  const out = Buffer.allocUnsafe(40)

  if (Buffer.isBuffer(roots)) roots.copy(out)
  else exports.tree(roots, out.slice(0, 32))

  uint64be.encode(length, out.slice(32))

  return out
}

exports.randomBytes = function (n) {
  const buf = Buffer.allocUnsafe(n)
  sodium.randombytes_buf(buf)
  return buf
}

exports.discoveryKey = function (publicKey) {
  const digest = Buffer.allocUnsafe(32)
  sodium.crypto_generichash(digest, HYPERCORE, publicKey)
  return digest
}

if (sodium.sodium_free) {
  exports.free = function (secureBuf) {
    if (secureBuf.secure) sodium.sodium_free(secureBuf)
  }
} else {
  exports.free = function () {}
}

function encodeUInt64 (n) {
  return uint64be.encode(n, Buffer.allocUnsafe(8))
}
