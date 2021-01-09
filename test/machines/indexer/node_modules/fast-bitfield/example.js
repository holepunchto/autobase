const bitfield = require('./')()

bitfield.set(0, true)
bitfield.set(14, true)
bitfield.set(100100, true)
bitfield.set(100000, true)
bitfield.set(10004242444, true)
bitfield.set(1000424244400, true)

const ite = bitfield.iterator()

while (true) {
  const i = ite.next(true)
  console.log(true, i)
  if (i === -1) break
}

const f = bitfield.iterator()
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
console.log(false, f.next(false))
