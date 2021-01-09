var tape = require('tape')
var ctz = require('../ctz')

tape('ctz', function(t) {
  t.equal(ctz(0), 32);
  t.equal(ctz(1), 0);
  t.equal(ctz(-1), 0);
  for(var i=0; i<31; ++i) {
    t.equal(ctz(1<<i), i);
    if(i > 0) {
      t.equal(ctz((1<<i)-1), 0)
    }
  }
  t.equal(ctz(0xf81700), 8);
  t.end()
})
