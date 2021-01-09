const binding = require('node-gyp-build')(__dirname)

lock.unlock = unlock
module.exports = lock

function lock (fd) {
  return !!binding.fd_lock(fd)
}

function unlock (fd) {
  return !!binding.fd_unlock(fd)
}
