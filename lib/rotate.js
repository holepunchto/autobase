const encryptionEncoding = require('encryption-encoding')

module.exports = async function rotateBlindEncrption(base, oldHandler, newHandler) {
  if (!base.blindEncryption) return

  const encryptionKeyEncryptedBuffer = await base.local.getUserData('autobase/blind-encryption')
  if (!encryptionKeyEncryptedBuffer) return

  const decrypted = await encryptionEncoding.decrypt(
    encryptionKeyEncryptedBuffer,
    oldHandler.decrypt
  )

  const encrypted = await encryptionEncoding.encrypt(decrypted, newHandler.encrypt)
  await base.local.setUserData('autobase/blind-encryption', encrypted)
}
