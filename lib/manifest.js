const c = require('compact-encoding')

const User = {
  preencode (state, u) {
    if (u.input) c.buffer.preencode(state, u.input)
    if (u.index) c.buffer.preencode(state, u.index)
    state.end += 1
  },
  encode (state, u) {
    const s = state.start++
    let bits = 0
    if (u.input) {
      bits |= 1
      c.buffer.encode(state, u.input)
    }
    if (u.index){
      bits |= 2
      c.buffer.encode(state, u.index)
    }
    state.buffer[s] = bits
  },
  decode (state) {
    const bits = c.uint.decode(state)
    return {
      input: (bits & 1) === 0 ? null : c.buffer.decode(state),
      index: (bits & 2) === 0 ? null : c.buffer.decode(state)
    }
  },
  inflate (corestore, u, token) {
    if (Buffer.isBuffer(u)) u = c.decode(User, u)
    return {
      input: inflateCore(corestore, u.input, token),
      index: inflateCore(corestore, u.index, token)
    }
  },
  deflate (u) {
    const deflated = {
      input: u.input ? deflateCore(u.input) : null,
      index: u.index ? deflateCore(u.index) : null
    }
    return c.encode(User, deflated)
  }
}

const UserArray = c.array(User)

const Manifest = {
  preencode(state, m) {
    UserArray.preencode(state, m)
  },
  encode (state, m) {
    UserArray.encode(state, m)
  },
  decode (state) {
    return UserArray.decode(state)
  },
  inflate (corestore, m, token) {
    if (Buffer.isBuffer(m)) m = c.decode(UserArray, m)
    return m.map(u => User.inflate(corestore, u, token))
  },
  deflate (m) {
    const deflated = m.map(u => User.deflate(corestore, u))
    return c.encode(UserArray, deflated)
  }
}

module.exports = {
  Manifest,
  User
}

function inflateCore (corestore, core, token) {
  if (isCore(core)) return core
  if (typeof core === 'string') core = Buffer.from(core, 'hex')
  if (Buffer.isBuffer(core)) return corestore.get({ key: core, token })
  throw new Error('Cannot inflate an invalid Hypercore')
}

function deflateCore (core) {
  if (isCore(core)) return core.key
  if (Buffer.isBuffer(core)) return core
  throw new Error('Cannot deflate an invalid Hypercore')
}

function isCore (core) {
  return typeof core === 'object' && typeof core.append === 'function' && typeof core.replicate === 'function'
}
