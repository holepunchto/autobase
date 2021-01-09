{
  "targets": [{
    "target_name": "lock_fd",
    "include_dirs": [
      "<!(node -e \"require('napi-macros')\")"
    ],
    "sources": [ "./binding.cc" ]
  }]
}

