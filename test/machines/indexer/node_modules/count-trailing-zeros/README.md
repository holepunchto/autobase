count-trailing-zeros
====================
Counts the number of trailing zeros for an integer in binary.

# Example

```javascript
var ctz = require('count-trailing-zeros')

for(var i=1; i<=16; ++i) {
  console.log(i+' (bin '+i.toString(2)+') has '+ctz(i)+' trailing zeros')
}
```

#### Output

```
1 (bin 1) has 0 trailing zeros
2 (bin 10) has 1 trailing zeros
3 (bin 11) has 0 trailing zeros
4 (bin 100) has 2 trailing zeros
5 (bin 101) has 0 trailing zeros
6 (bin 110) has 1 trailing zeros
7 (bin 111) has 0 trailing zeros
8 (bin 1000) has 3 trailing zeros
9 (bin 1001) has 0 trailing zeros
10 (bin 1010) has 1 trailing zeros
11 (bin 1011) has 0 trailing zeros
12 (bin 1100) has 2 trailing zeros
13 (bin 1101) has 0 trailing zeros
14 (bin 1110) has 1 trailing zeros
15 (bin 1111) has 0 trailing zeros
16 (bin 10000) has 4 trailing zeros
```

# Install

```
npm i count-trailing-zeros
```

# API

#### `require('count-trailing-zeros')(v)`
Count the number of trailing zeros.

* `v` is an integer

**Returns** The number of trailing zeros of `v`

**Note** For a full balanced binary tree with 2^n elements arranged in order, this is the trailing zeros the index of an element is the same as its height in the tree.

# License
(c) 2015 Mikola Lysenko. MIT License
