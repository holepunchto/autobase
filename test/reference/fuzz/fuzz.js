const path = require('path')
const { fuzz } = require('./')

const writers = 5
const nodes = 30
const branch = 0.4
const log = 1000
const dir = path.join(__dirname, process.argv[2] || 'generated')

fuzz(writers, nodes, branch, log, dir)
