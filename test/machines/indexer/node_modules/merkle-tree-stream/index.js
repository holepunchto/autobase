const { Transform } = require('streamx')
const MerkleGenerator = require('./generator')

module.exports = class MerkleTreeStream extends Transform {
  constructor (opts, roots) {
    super({highWaterMark: (opts && opts.highWaterMark) || 16})
    if (!opts) opts = {}
    this._generator = new MerkleGenerator(opts, roots)
    this.roots = this._generator.roots
    this.blocks = 0
  }

  _transform (data, cb) {
    var nodes = this._generator.next(data)
    for (var i = 0; i < nodes.length; i++) this.push(nodes[i])
    this.blocks = this._generator.blocks
    cb()
  }
}
