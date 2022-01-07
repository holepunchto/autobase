module.exports = {
  seed: 'autobase-fuzzing',
  numIterations: 1000,
  numOperations: 10000,
  shortening: {
    iterations: 0
  },
  inputs: {},
  operations: {
    append: {
      enabled: true,
      weight: 10
    },
    appendForked: {
      enabled: true,
      weight: 100
    },
    updateLocalView: {
      enabled: true,
      weight: 0
    },
    updateRemoteView: {
      enabled: true,
      weight: 10
    }
  },
  validation: {
    causalOrdering: {
      enabled: true
    }
  }
}
