# External Pointer Pattern

The external pointer pattern has a writer append a pointer to large amounts of data stored in another data structure, usually one base on a hypercore.

## Motivation

When using an autobase that need to synchronize large amounts of data, if the data is appended by writers and used to create the view, the autobase can quickly grow in size due to the writer data being copied into the view. This can slow down indexers by requiring them to processing large amounts of data.

## Solution

To avoid this, writers can append a block which includes a pointer to find the data outside of the autobase. This way data isn't duplicated between the writer and the view.

## Example

A simple example is a multiwriter blob store using [`hyperblobs`](https://github.com/holepunchto/hyperblobs) to create a `BlobBase`. The `BlobBase`'s `view` is a [Hyperbee](https://github.com/holepunchto/hyperbee) with filenames as `key`s and a pointer for the blob contents (`{ blobKey, id }`) as the `value`s.

Adding a file to the `BlobBase` (`blobBase.put(filename, blob)`) first `.put()`s the blob into a local hyperblobs instance and then appends a block with:

```js
{
  op: 'put',
  key: filename,
  value: {
    blobKey: this.localBlobs.key,
    id // output from await this.localBlobs.put(blob)
  }
}
```

To retrieve the blobs, a peer can lookup the pointer using the filename to `view.get(filename)` and creating a Hyperblob instance based on the pointer to get the blob from.

This means the peer need to replicate the hyperblobs instance as well so that when peers access the `filename` they can request the blob. A full solution would manage the hyperblobs much like how Autobase manages writers keeping only the relevant hyperblobs open and closing them as needed.

With the pointer a 100MB blob is only stored in the hyperblobs's core of the writer.

Full code in [`./blob-base.mjs`](./blob-base.mjs).

## Run

```console
npm i
bare blob-base.mjs
```
