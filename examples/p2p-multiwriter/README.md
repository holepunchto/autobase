# peer-to-peer multi-writer example

This example aims to show how to setup a basic
peer-to-peer multi-writer using Hypercore 10.

![example](https://user-images.githubusercontent.com/8385/159972114-e06dcc52-4ca8-4d4b-84aa-583995c10844.gif)

The `chat-cli.js` "app" runs in the terminal as
one of N users in the `users.js` file.

When you run `node chat-cli.js saimon` you can
think of that process as an "app" that holds
both the public and private keys for `saimon`
but only knows the publicKeys for all the other
users.


## Exercise

In two terminals run the following commands:

```bash
# terminal 1
node chat-cli.js saimon
# terminal 2
node chat-cli.js paul
```

A full screen terminal app should spawn and begin trying to
join a Hyperswarm.

Each instance of `chat-cli.js` stores its Hypercores in `./state/${username}`
so you should be able to author new chat messages before connecting
to and syncing across the swarm.
