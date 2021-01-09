var ctz = require('../ctz')

for(var i=1; i<=16; ++i) {
  console.log(i+' (bin '+i.toString(2)+') has '+ctz(i)+' trailing zeros')
}
