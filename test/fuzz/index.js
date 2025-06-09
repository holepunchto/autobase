const path = require('path')
const { fuzz } = require('./helpers')

const writers = 30
const indexers = 5
const nodes = 20
const branch = 0.4
const optimistic = 0.1
const log = 1000
const dir = path.join(__dirname, process.argv[2] || 'generated')

fuzz(writers, indexers, nodes, branch, optimistic, log, dir)
