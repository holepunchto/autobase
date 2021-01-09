const lock = require('./')
const { openSync } = require('fs')

const fd = openSync(__filename, 'r')
const fd1 = require('fs').openSync(__filename, 'r')

console.log('Could lock it?', lock(fd))
console.log('Could lock it for another fd?', lock(fd1))
console.log('Keeping the program running...')
console.log('Try running the example again in another process')

process.stdin.resume()
