/* eslint-disable camelcase */

function sodium_malloc (n) {
  return new Uint8Array(n)
}

function sodium_free (n) {
  sodium_memzero(n)
  loadSink().port1.postMessage(n.buffer, [n.buffer])
}

function sodium_memzero (arr) {
  arr.fill(0)
}

var sink

function loadSink () {
  if (sink) return sink
  var MessageChannel = global.MessageChannel
  if (MessageChannel == null) ({ MessageChannel } = require('worker' + '_threads'))
  sink = new MessageChannel()
  return sink
}

module.exports = {
  sodium_malloc,
  sodium_free,
  sodium_memzero
}
