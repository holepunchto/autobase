const codecs = require('codecs')

class AbstractExtension {
  constructor (local, name, handlers = {}) {
    this.id = 0
    this.name = name
    this.encoding = codecs(handlers.encoding || 'binary')
    this.handlers = handlers
    this.local = local
  }

  encode (message) {
    return this.encoding.encode(message)
  }

  remoteSupports () {
    return !!(this.local && this.local.map && this.local.map[this.id] === this)
  }

  onmessage (buf, context) {
    if (!this.handlers.onmessage) return

    let message
    try {
      message = this.encoding.decode(buf)
    } catch (err) {
      if (this.handlers.onerror) this.handlers.onerror(err, context)
      return
    }

    this.handlers.onmessage(message, context)
  }

  get destroyed () {
    return this.local === null
  }

  destroy () {
    if (this.local === null) return
    this.local._remove(this)
    this.local = null
  }

  static createLocal (handlers = null) {
    return new Local(handlers, this)
  }
}

class Remote {
  constructor (local) {
    this.local = local
    this.names = null
    this.map = null
    this.changes = 0
  }

  update (names) {
    this.names = names
    this.changes = 0
  }

  onmessage (id, message, context = null) {
    if (this.changes !== this.local.changes) {
      this.map = this.names ? match(this.local.messages, this.names) : null
      this.changes = this.local.changes
    }
    const m = this.map && this.map[id]
    if (m) m.onmessage(message, context)
  }
}

class Local {
  constructor (handlers = null, M) {
    this.messages = []
    this.handlers = handlers
    this.Extension = M
    this.changes = 1
    this.exclusive = true
  }

  get length () {
    return this.messages.length
  }

  [Symbol.iterator] () {
    return this.messages[Symbol.iterator]()
  }

  get (name) {
    // technically we can bisect here, but yolo
    for (const m of this.messages) {
      if (m.name === name) return m
    }
    return null
  }

  add (name, handlers) {
    let m

    if (typeof handlers !== 'function') {
      m = new this.Extension(this, name, handlers)
    } else {
      m = new this.Extension(this, name, {})
      m.handlers = handlers(m) || {}
      m.encoding = codecs(m.handlers.encoding || 'binary')
    }

    this.changes++
    this.messages.push(m)
    this.messages.sort(sortMessages)
    for (let i = 0; i < this.messages.length; i++) {
      this.messages[i].id = i
    }

    if (this.exclusive) {
      if ((m.id > 0 && this.messages[m.id - 1].name === m.name) || (m.id < this.messages.length - 1 && this.messages[m.id + 1].name === m.name)) {
        this._remove(m)
        throw new Error('Cannot add multiple messages with the same name')
      }
    }

    if (this.handlers && this.handlers.onextensionupdate) this.handlers.onextensionupdate()

    return m
  }

  remote () {
    return new Remote(this)
  }

  _remove (m) {
    this.changes++
    this.messages.splice(m.id, 1)
    m.id = -1
    if (this.handlers && this.handlers.onextensionupdate) this.handlers.onextensionupdate()
  }

  names () {
    const names = new Array(this.messages.length)
    for (let i = 0; i < names.length; i++) {
      names[i] = this.messages[i].name
    }
    return names
  }
}

function sortMessages (a, b) {
  return a.name < b.name ? -1 : a.name > b.name ? 1 : 0
}

function match (local, remote) {
  let i = 0
  let j = 0

  const map = new Array(remote.length)

  while (i < local.length && j < remote.length) {
    const l = local[i].name
    const r = remote[j]

    if (l < r) i++
    else if (l > r) j++
    else map[j++] = local[i]
  }

  return map
}

module.exports = AbstractExtension
