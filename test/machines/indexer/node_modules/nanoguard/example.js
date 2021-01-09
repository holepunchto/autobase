const Nanoguard = require('./')
const guard = new Nanoguard()

guard.ready(function () {
  console.log('i am in')
})

guard.wait()

guard.ready(function () {
  console.log('i am in after wait/continue')
})

setTimeout(() => guard.continue(), 100)
