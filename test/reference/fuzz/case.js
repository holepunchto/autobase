const path = require('path')
const { testSingle } = require('./')

const name = process.argv[2]
if (!name) throw new Error('must specify test')

const dir = process.argv[3] || 'generated'
const full = path.join(__dirname, dir, name)

testSingle(name, require(full))

// function renderFailure (n, result) {
//   console.log(formatResult(result))
//   // const file = path.resolve('renders', `generated-${n}.md`)
//   // fs.writeFileSync(file, formatResult(result))
// }
