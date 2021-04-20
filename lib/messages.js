const c = require('compact-encoding')

// Header Structs

const HeaderMetadata = {
  preencode (state, req) {
    c.buffer.preencode(state, req && req.userData)
  },
  encode (state, req) {
    c.buffer.encode(state, req && req.userData)
  },
  decode (state) {
    return {
      userData: c.buffer.decode(state)
    }
  }
}

const Header = {
  preencode (state, req) {
    c.string.preencode(state, req.protocol)
    HeaderMetadata.preencode(state, req.metadata)
  },
  encode (state, req) {
    c.string.encode(state, req.protocol)
    HeaderMetadata.encode(state, req.metadata)
  },
  decode (state) {
    return {
      protocol: c.string.decode(state),
      metadata: HeaderMetadata.decode(state)
    }
  }
}

module.exports = {
  HeaderMetadata,
  Header
}
