// Type declarations for the holepunchto/autobase public API.
/// <reference types="node" />
// NOTE: could not resolve external type(s) Stream — rendered as `any`; add a manual import/type if one is available.

import type Corestore from 'corestore'
import type Hypercore from 'hypercore'

/**
 * Options for `base.append()`.
 */
export interface AppendOptions {
  /** Allow appending on an optimistic Autobase while not a writer; the block is only applied if validated in `apply`. */
  optimistic?: boolean
}

/**
 * Configuration: `open`, `apply`, `close`, `valueEncoding`, `ackInterval`, `fastForward`, `encrypt`/`encryptionKey`, `optimistic`, and more (see the options table).
 */
export interface AutobaseOptions {
  /** create the view */
  open?: (store: Corestore) => any
  /** handle nodes to update view */
  apply?: (nodes: any, view: any, host: ApplyHost) => any
  /** Autobase supports optimistic appends */
  optimistic?: any
  /** close the view */
  close?: any
  /** encoding */
  valueEncoding?: any
  /** enable auto acking with the interval */
  ackInterval?: any
  /** Key to encrypt the base */
  encryptionKey?: any
  /** Encrypt the base if unencrypted & no encryptionKey is set */
  encrypt?: any
  /** Expect the base to be encrypted, will throw an error otherwise, defaults to true if encryptionKey is set */
  encrypted?: any
  /** Enable fast forwarding. If passing { key: base.core.key }, they autobase will fastforward to that key first. */
  fastForward?: any
  /** Set a custom wakeup protocol for hinting which writers are active, see `protomux-wakeup` for protocol details */
  wakeup?: any
  /** Enable big batches. See `base.setBigBatches()` for details. */
  bigBatches?: any
}

export class Autobase {
  /**
   * Instantiate an Autobase.
   * @param store - The Corestore that holds the system, writer, and view cores.
   * @param bootstrap - Key of an existing Autobase to load; omit or pass `null` to create a new one.
   * @param handlers - Configuration: `open`, `apply`, `close`, `valueEncoding`, `ackInterval`, `fastForward`, `encrypt`/`encryptionKey`, `optimistic`, and more (see the options table).
   */
  constructor(store: Corestore, bootstrap: Buffer | string, handlers?: AutobaseOptions)

  readonly bootstrap: any

  readonly bootstraps: any

  readonly appending: any

  /**
   * Whether the instance is a writer for the autobase.
   */
  readonly writable: boolean

  readonly ackable: any

  /**
   * The index of the system core that has been signed by a quorum of indexers. The system up until this point will not change.
   */
  readonly signedLength: number

  readonly indexedLength: any

  /**
   * The length of the system core. This is neither the length of the local writer nor the length of the view. The system core tracks the autobase as a whole.
   */
  readonly length: number

  readonly flushing: any

  /**
   * Merkle-tree hash of the system core at its current length.
   * @returns Returns the hash of the system core's merkle tree roots.
   */
  hash(): Promise<Buffer>

  getSystemKey(): any

  readonly system: any

  getIndexedInfo(): Promise<any>

  /**
   * Creates a replication stream for replicating the autobase. Arguments are the same as [corestores's `.replicate()`](https://github.com/holepunchto/corestore?tab=readme-ov-file#const-stream--storereplicateoptsorstream).
   * @param isInitiator - `true`/`false` to open a new stream, or an existing replication stream to attach to.
   * @param opts - Replication options forwarded to the underlying Corestore.
   * @returns The replication stream.
   */
  replicate(isInitiator: boolean | any, opts: object): any

  /**
   * Gets the current writer heads. A writer head is a node which has no causal dependents, aka it is the latest write. If there is more than one head, there is a causal fork which is pretty common.
   * @returns The head nodes (empty until the base is ready).
   */
  heads(): Array<object>

  export(): Promise<any>

  hintWakeup(hints: any): any

  /**
   * Set the autobase to enable or disable big batches. Big batches allow autobase to run the `apply` function on more blocks at once at the expense of being slower and less responsive.
   * @param bool - Whether to enable big batches.
   */
  setBigBatches(bool?: boolean): void

  setLocal(key: any, options?: any): Promise<any>

  setWakeup(cap: any, discoveryKey: any): any

  static getBootRecord(store: any, key: any): Promise<any>

  flush(): Promise<any>

  advance(): Promise<any>

  recouple(): any

  getLastError(): any

  /**
   * Fetch all available data and update the linearizer.
   * @returns Resolves once the linearizer has advanced.
   */
  update(): Promise<void>

  isFastForwarding(): any

  /**
   * Manually acknowledge the current state by appending a `null` node that references known head nodes. `null` nodes are ignored by the `apply` handler and only serve as a way to acknowledge the current linearized state. Only indexers can ack.
   * @param bg - If `bg` is set to `true`, the ack will not be appended immediately but will set the automatic ack timer to trigger as soon as possible.
   * @returns Resolves once the ack has been handled.
   */
  ack(bg?: boolean): Promise<void>

  views(): any

  batch(): any

  /**
   * Append a new entry to the autobase.
   * @param value - The value, or array of values, to append.
   * @param opts - Append options.
   * @returns Resolves once the value is appended locally.
   */
  append(value: any | Array<any>, opts: AppendOptions): Promise<number>

  static decodeValue(value: any, opts: any): any

  static encodeValue(value: any, opts: any): any

  static getLocalKey(store: any, opts?: any): Promise<any>

  /**
   * Generate a local core to be used for an Autobase.
   * @param store - The Corestore to open the core from.
   * @param handlers - `handlers` are any options passed to `store` to get the core.
   * @param encryptionKey - Encryption key, when the core is encrypted.
   * @returns The local writer core.
   */
  static getLocalCore(store: Corestore, handlers: object, encryptionKey: Buffer): Hypercore

  /**
   * Get user data associated with an autobase `core`. `referrer` is the `.key` of the autobase the `core` is from. `view` is the `name` of the view.
   * @param core - The bootstrap/local core to read from.
   * @returns The referrer and view name.
   */
  static getUserData(core: Hypercore): Promise<{ referrer: Buffer; view: string | null }>

  /**
   * Detect whether a core's first block is an Autobase oplog message.
   * @param core - The core to inspect.
   * @param opts - `opts` are the same options as [core.get(index, opts)](https://github.com/holepunchto/hypercore?tab=readme-ov-file#const-block--await-coregetindex-options).
   * @returns Returns whether the core is an autobase core.
   */
  static isAutobase(core: Hypercore, opts?: object): Promise<boolean>

  /**
   * Sets the [User Data](https://github.com/holepunchto/hypercore#user-data) value for the provided `key`. `key` is a string. `value` can be either a string or a buffer.
   * @param key - `key` is a string.
   * @param val - `value` can be either a string or a buffer.
   * @returns Resolves once written.
   */
  setUserData(key: string, val: Buffer | string): Promise<void>

  /**
   * Read a local-only value previously stored with `setUserData()`.
   * @param key - `key` is a string.
   * @returns Returns the [User Data](https://github.com/holepunchto/hypercore#user-data) value for the provided `key`.
   */
  getUserData(key: string): Promise<Buffer | null>

  getWriterEncryption(): any

  repair(): Promise<any>

  forceFastForward(): Promise<any>

  /**
   * Pauses the autobase prevent the next apply from running.
   */
  pause(): void

  /**
   * Resumes a paused autobase and will check for an update.
   */
  resume(): void

  waitForWritable(): any

  /**
   * Whether the writer with `key` can be removed without leaving the system
without a removable indexer set.
   * @param key - The writer's key.
   * @returns Returns whether the writer for the given `key` can be removed.
   */
  removeable(key: Buffer): boolean

  ready(): Promise<any>

  close(): Promise<any>

  readonly opened: any

  readonly closed: any

  emit(event: any, arg1?: any): any

  id: any

  /**
   * The primary key of the autobase.
   */
  key: Buffer

  /**
   * The discovery key associated with the autobase.
   */
  discoveryKey: Buffer

  backoff: any

  keyPair: any

  valueEncoding: any

  store: any

  globalCache: any

  migrated: any

  encrypted: any

  encrypt: any

  encryptionKey: any

  encryption: any

  blindEncryption: any

  activeBatch: any

  local: any

  localWriter: any

  /**
   * Whether the instance is an indexer.
   */
  isIndexer: boolean

  activeWriters: any

  linearizer: any

  updating: any

  nukeTip: any

  wakeupOwner: any

  wakeupCapability: any

  wakeupProtocol: any

  wakeupSession: any

  fastForwardEnabled: any

  fastForwarding: any

  fastForwardTo: any

  fastForwardFailedAt: any

  fastForwardMinimum: any

  bigBatches: any

  /**
   * Whether linearization is currently paused (see `pause()` / `resume()`).
   * @returns Returns `true` if the autobase is currently paused, otherwise returns `false`.
   */
  paused: boolean

  /**
   * The view of the autobase derived from writer inputs. The view is created in the `open` handler and can have any shape. The most common `view` is a [hyperbee](https://github.com/holepunchto/hyperbee).
   */
  view: any

  core: any

  version: any

  interrupted: any

  recoveries: any

  on(event: 'rotate-local-writer', listener: () => void): this
  /**
   * Triggered when `host.interrupt(reason)` is called in the `apply` handler. See [`host.interrupt(reason)`](#hostinterruptreason) for when interrupts are used.
   */
  on(event: 'interrupt', listener: (interrupted: any) => void): this
  /**
   * Triggered when the autobase view updates after `apply` has finished running.
   */
  on(event: 'update', listener: () => void): this
  /**
   * Triggered when an error is triggered while updating the autobase.
   */
  on(event: 'error', listener: (err: any) => void): this
  /**
   * Triggered when the autobase fast forwards to a state already with a quorum. `to` and `from` are the `.signedLength` after and before the fast forward respectively.
   */
  on(event: 'fast-forward', listener: (to: any, from: any) => void): this
  on(event: 'reboot', listener: () => void): this
  /**
   * Triggered when the autobase instance is an indexer.
   */
  on(event: 'is-indexer', listener: () => void): this
  /**
   * Triggered when the autobase instance is not an indexer.
   */
  on(event: 'is-non-indexer', listener: () => void): this
  /**
   * Triggered when the autobase instance is now a writer.
   */
  on(event: 'writable', listener: (...args: any[]) => void): this
  /**
   * Triggered when the autobase instance is no longer a writer.
   */
  on(event: 'unwritable', listener: (...args: any[]) => void): this
  /**
   * Triggered when a warning is triggered.
   */
  on(event: 'warning', listener: (...args: any[]) => void): this
}

declare class WakeupHandler {
  constructor(base: any, discoveryKey: any)

  onpeeractive(peer: any, session: any): any

  onlookup(req: any, peer: any, session: any): any

  onannounce(wakeup: any, peer: any, session: any): any

  active: any

  discoveryKey: any

  base: any
}

declare class ApplyHost {
  /**
   * Add a writer with the given `key` to the autobase allowing their local core to append. If `indexer` is `true`, it will be added as an indexer.
   */
  addWriter(key: any, arg1?: any): Promise<any>

  /**
   * Remove a writer from the autobase. This will throw if the writer cannot be removed.
   */
  removeWriter(key: any): Promise<any>

  /**
   * Acknowledge a writer even if they haven't been added before. This is most useful for applying `optimistic` blocks from writers that are not currently a writer.
   */
  ackWriter(key: any): Promise<any>

  /**
   * Interrupt the applying of writer blocks optionally giving a `reason`. This will emit an `interrupt` event passing the `reason` to the callback and close the autobase.
   */
  interrupt(reason: any): any
}

export default Autobase
