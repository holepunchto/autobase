# Encryption

## Key Rotation

Each hypercore block references the identifier of the encryption key that was used.

The Autobase encryption view handles the mapping from key id to encryption key.

### Mechanism

Keys are rotated by appending a new entry to the encryption view.

Autobase will expose a host call for doing so:

```js
function apply (view, nodes, base) {
  await base.updateEncryption(encryption)
}
```

### Views

View encryption always uses the latest encryption key, which is obtained from `base._applyState.encryptionView`. This is already baked in to the current encryption flow.

In the case of reorgs, views are simply truncated down and data reapplied with the updated encryption key.

This way, the mapping from key id to encryption key will _always_ be consistent once indexed, since there is the linearization of encryption key updates is guaranteed to be consistent in the indexed view.

#### System

System encrypted the same as any other view.

#### Encryption

The encryption view is unencrypted, working under the assumption that each entry is a user encrypted blob.

### Oplogs

Writer oplogs need more care, since they are never truncated.

Therefore writers should only ever use the latest _indexed_ encryption key.

The Autobase owns a long lived encryption view that is backed by the main session of the encryption core (ie. not the batch or atomicBatch). Writer encryption providers are instantiated from this view, rather than `_applyState`'s encryption view.

### Key Distribution

The user is responsible for distributing keys via the encryption view.

The API requirement is that the encryption view exposes an `async get () { return seed }` method.

Initially, a user will simply append a blob that is an array containing for each member a ciphertext of the new encryption key encrypted to their public key.

If we use AEAD construction, then we can brute force decryption until we find our entry. A simple optimisation is to introduce a writer index.

If Keet could pass down a `decryptKey` hook to Autobase, then we would only need one entry per member, as opposed to one entry per device in the naive Autobase only version.
