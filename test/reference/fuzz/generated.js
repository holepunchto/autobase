const fs = require('fs')
const path = require('path')
const { test } = require('./')

const name = process.argv[2] || 'generated'
const dir = path.join(__dirname, name)

for (const file of fs.readdirSync(dir)) {
  test(file, require(path.join(dir, file)))
}
