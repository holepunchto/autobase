class MemberBatch {
  constructor (autobase) {
    this.autobase = autobase
    this.batchId = autobase._batchId
    this._ops = []
  }

  addInput (core) {
    this._ops.push({ type: MemberBatch.ADD_INPUT, core })
  }

  addOutput (core) {
    this._ops.push({ type: MemberBatch.ADD_OUTPUT, core })
  }

  removeInput (core) {
    this._ops.push({ type: MemberBatch.REMOVE_INPUT, core })
  }

  removeOutput (core) {
    this._ops.push({ type: MemberBatch.REMOVE_OUTPUT, core })
  }

  async commit () {
    await this.autobase.ready()
    await Promise.all(this._ops.map(({ core }) => (typeof core.ready === 'function') ? core.ready() : Promise.resolve()))

    if (this.batchId !== this.autobase._batchId) throw new Error('Batch is out-of-date. Did you commit another batch in parallel?')
    this.autobase._batchId++

    for (const op of this._ops) {
      switch (op.type) {
        case MemberBatch.ADD_INPUT:
          this.autobase._addInput(op.core)
          break
        case MemberBatch.ADD_OUTPUT:
          this.autobase._addOutput(op.core)
          break
        case MemberBatch.REMOVE_INPUT:
          this.autobase._removeInput(op.core)
          break
        case MemberBatch.REMOVE_OUTPUT:
          this.autobase._removeOutput(op.core)
          break
        default:
          throw new Error('Unsupported MemberBatch operation')
      }
    }

    this.autobase._bumpReadStreams()
  }
}
MemberBatch.ADD_INPUT = 0
MemberBatch.ADD_OUTPUT = 1
MemberBatch.REMOVE_INPUT = 2
MemberBatch.REMOVE_OUTPUT = 3

module.exports = MemberBatch
