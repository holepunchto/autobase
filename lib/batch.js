class MemberBatch {
  constructor (autobase) {
    this.autobase = autobase
    this.batchId = autobase._batchId
    this._ops = []
  }

  addInput (key, opts) {
    this._ops.push({ type: MemberBatch.ADD_INPUT, key, opts })
  }

  addOutput (key, opts) {
    this._ops.push({ type: MemberBatch.ADD_OUTPUT, key, opts })
  }

  removeInput (key, opts) {
    this._ops.push({ type: MemberBatch.REMOVE_INPUT, key, opts })
  }

  removeOutput (key, opts) {
    this._ops.push({ type: MemberBatch.REMOVE_OUTPUT, key, opts })
  }

  async commit () {
    await this.autobase.ready()

    if (this.batchId !== this.autobase._batchId) throw new Error('Batch is out-of-date. Did you commit another batch in parallel?')
    this.autobase._batchId++
    const cleanups = []
    const cores = []

    for (const op of this._ops) {
      switch (op.type) {
        case MemberBatch.ADD_INPUT:
          cores.push(this.autobase._addInput(op.key, op.opts))
          break
        case MemberBatch.ADD_OUTPUT:
          cores.push(...this.autobase._addOutput(op.key, op.opts))
          break
        case MemberBatch.REMOVE_INPUT:
          cleanups.push(this.autobase._removeInput(op.key, op.opts))
          break
        case MemberBatch.REMOVE_OUTPUT:
          cleanups.push(this.autobase._removeOutput(op.key, op.opts))
          break
        default:
          throw new Error('Unsupported MemberBatch operation')
      }
    }

    await Promise.all([
      ...cores.map(c => c && c.ready()),
      ...cleanups.map(c => c())
    ])
    this.autobase._bumpReadStreams()
  }
}
MemberBatch.ADD_INPUT = 0
MemberBatch.ADD_OUTPUT = 1
MemberBatch.REMOVE_INPUT = 2
MemberBatch.REMOVE_OUTPUT = 3

module.exports = MemberBatch
