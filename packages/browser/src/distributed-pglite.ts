import { PGlite, type PGliteOptions } from "@electric-sql/pglite";
import {
  SingleWriterRegistry,
  SqlPartitionStore,
  SyncNode,
  assertValidPartitionId,
  emitTelemetry,
  type EventCursor,
  type EventProjection,
  type EventEnvelope,
  type EventId,
  type LocalEventInput,
  type NodeId,
  type PartitionId,
  type PartitionStore,
  type SingleWriterViolationError,
  type SyncTelemetrySink,
  type Transport,
} from "@distrb-pglite/core";
import { BrowserSha256BlobStore } from "./blob-store.js";
import {
  restoreBrowserFilesystemFromCas,
  snapshotBrowserFilesystemToCas,
  transferMissingBrowserCasBlocks,
  type BrowserCasBlock,
  type BrowserCasBlockStore,
  type BrowserCasChunking,
  type BrowserCasManifest,
} from "./cas.js";
import { BrowserPGliteCasFilesystem } from "./pglite-cas-filesystem.js";
import {
  BrowserWebSocketTransport,
  type BrowserWebSocketTransportOptions,
} from "./websocket-transport.js";

export type DistributedBrowserPGliteTransportOptions =
  | Transport
  | ({ type: "websocket" } & BrowserWebSocketTransportOptions);

export type DistributedBrowserPGliteProjection<TPayload = any> = (
  db: PGlite,
  event: EventEnvelope<TPayload>,
) => Promise<void> | void;

export type DistributedBrowserPGlitePartitionOptions<TPayload = any> = {
  id: PartitionId;
  schemaVersion?: number;
  recoverFrom?: NodeId;
  project?: DistributedBrowserPGliteProjection<TPayload>;
};

export type DistributedBrowserPGliteOpenOptions = {
  nodeId: NodeId;
  fs?: BrowserPGliteCasFilesystem;
  blobs?: BrowserSha256BlobStore;
  schema?: string;
  migrate?: (db: PGlite) => Promise<void> | void;
  pgliteOptions?: PGliteOptions;
  transport: DistributedBrowserPGliteTransportOptions;
  partitions?: DistributedBrowserPGlitePartitionOptions[];
  autoStart?: boolean;
  autoRecoverSubscriptions?: boolean;
  onAppliedRemote?: (event: EventEnvelope) => Promise<void> | void;
  singleWriter?: SingleWriterRegistry | { owners: Record<PartitionId, NodeId> };
  onSingleWriterViolation?: (error: SingleWriterViolationError) => void;
  telemetry?: SyncTelemetrySink;
};

export type DistributedBrowserPGliteWriteOptions<TPayload> = {
  partitionId: PartitionId;
  payload: TPayload;
  schemaVersion?: number;
  project?: DistributedBrowserPGliteProjection<TPayload>;
};

export type DistributedBrowserPGliteRestoreOptions = {
  manifest: BrowserCasManifest;
  sourceCasStore: BrowserCasBlockStore;
  targetCasStore: BrowserCasBlockStore;
  fs?: BrowserPGliteCasFilesystem;
  blobs?: BrowserSha256BlobStore;
  autoStart?: boolean;
};

export type DistributedBrowserPGliteSnapshotOptions = {
  casStore: BrowserCasBlockStore;
  snapshotId?: string;
  parentSnapshotIds?: string[];
  chunkSize?: number;
  chunking?: BrowserCasChunking;
};

export type DistributedBrowserPGliteRestoreResult = {
  handle: DistributedBrowserPGlite;
  transferredBlocks: BrowserCasBlock[];
};

export type DistributedBrowserPGliteInternals = {
  readonly db: PGlite;
  readonly fs: BrowserPGliteCasFilesystem;
  readonly blobs: BrowserSha256BlobStore;
  readonly eventStore: SqlPartitionStore;
  readonly store: PartitionStore;
  readonly syncNode: SyncNode;
};

type OpenPartition = Required<Pick<DistributedBrowserPGlitePartitionOptions, "schemaVersion">> &
  Omit<DistributedBrowserPGlitePartitionOptions, "schemaVersion">;

export class DistributedBrowserPGlite {
  private readonly blobs: BrowserSha256BlobStore;
  private readonly fs: BrowserPGliteCasFilesystem;
  private readonly db: PGlite;
  private readonly baseStore: SqlPartitionStore;
  private readonly projectedStore: ProjectingBrowserStore;
  private readonly syncNode: SyncNode;
  private online = false;
  private readonly partitions = new Map<PartitionId, OpenPartition>();
  private readonly singleWriterRegistry?: SingleWriterRegistry;

  private constructor(
    private readonly options: DistributedBrowserPGliteOpenOptions,
    db: PGlite,
    fs: BrowserPGliteCasFilesystem,
    blobs: BrowserSha256BlobStore,
  ) {
    this.db = db;
    this.fs = fs;
    this.blobs = blobs;
    this.baseStore = new SqlPartitionStore(options.nodeId, this.db);
    this.projectedStore = new ProjectingBrowserStore(
      this.baseStore,
      this.db,
      options.onAppliedRemote,
    );
    this.singleWriterRegistry = resolveSingleWriterRegistry(options.singleWriter);
    this.syncNode = new SyncNode(
      options.nodeId,
      this.projectedStore,
      createTransport(options.transport),
      {
        singleWriter: this.singleWriterRegistry,
        onSingleWriterViolation: options.onSingleWriterViolation,
        telemetry: options.telemetry,
      },
    );
    for (const partition of options.partitions ?? []) {
      this.partitions.set(partition.id, {
        ...partition,
        schemaVersion: partition.schemaVersion ?? 1,
      });
      this.projectedStore.setProjection(partition.id, partition.project);
    }
  }

  static async open(
    options: DistributedBrowserPGliteOpenOptions,
  ): Promise<DistributedBrowserPGlite> {
    const blobs = options.blobs ?? new BrowserSha256BlobStore();
    const fs = options.fs ?? new BrowserPGliteCasFilesystem(blobs);
    const db = new PGlite({
      fs,
      relaxedDurability: false,
      ...options.pgliteOptions,
    });
    await db.waitReady;
    if (options.schema) {
      await db.exec(options.schema);
    }
    await options.migrate?.(db);

    const handle = new DistributedBrowserPGlite(options, db, fs, blobs);
    await handle.baseStore.migrate();
    if (options.autoStart ?? true) {
      await handle.start();
    }
    return handle;
  }

  get internals(): DistributedBrowserPGliteInternals {
    return {
      db: this.db,
      fs: this.fs,
      blobs: this.blobs,
      eventStore: this.baseStore,
      store: this.projectedStore,
      syncNode: this.syncNode,
    };
  }

  get isOnline(): boolean {
    return this.online;
  }

  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[]; affectedRows?: number }> {
    return this.db.query<T>(sql, params);
  }

  exec(sql: string): Promise<unknown> {
    return this.db.exec(sql);
  }

  async start(): Promise<void> {
    if (this.online) {
      return;
    }
    await this.syncNode.start({
      autoRecoverSubscriptions: this.options.autoRecoverSubscriptions ?? true,
    });
    for (const partition of this.partitions.values()) {
      await this.syncNode.subscribe(partition.id, { recoverFrom: partition.recoverFrom });
    }
    this.online = true;
  }

  async stop(): Promise<void> {
    if (!this.online) {
      return;
    }
    await this.syncNode.stop();
    this.online = false;
  }

  async close(): Promise<void> {
    await this.stop();
    await this.db.close();
  }

  async createSnapshot(
    options: DistributedBrowserPGliteSnapshotOptions,
  ): Promise<BrowserCasManifest> {
    const startedAt = Date.now();
    const manifest = await snapshotBrowserFilesystemToCas({
      fs: this.fs.asSnapshotDataFilesystem(),
      casStore: options.casStore,
      snapshotId: options.snapshotId,
      parentSnapshotIds: options.parentSnapshotIds,
      chunkSize: options.chunkSize,
      chunking: options.chunking,
    });
    emitTelemetry(this.options.telemetry, {
      type: "snapshot-created",
      nodeId: this.options.nodeId,
      snapshotId: manifest.snapshotId,
      durationMs: Date.now() - startedAt,
    });
    return manifest;
  }

  /** @deprecated Use restoreSnapshot. */
  async restoreFromCasSnapshot(
    options: DistributedBrowserPGliteRestoreOptions,
  ): Promise<DistributedBrowserPGliteRestoreResult> {
    const startedAt = Date.now();
    await this.close();
    const transferredBlocks = await transferMissingBrowserCasBlocks({
      manifest: options.manifest,
      sourceCasStore: options.sourceCasStore,
      targetCasStore: options.targetCasStore,
    });
    const blobs = options.blobs ?? new BrowserSha256BlobStore();
    const fs = options.fs ?? new BrowserPGliteCasFilesystem(blobs);
    await restoreBrowserFilesystemFromCas({
      manifest: options.manifest,
      casStore: options.targetCasStore,
      fs: fs.asSnapshotDataFilesystem(),
    });
    const handle = await DistributedBrowserPGlite.open({
      ...this.options,
      fs,
      blobs,
      autoStart: options.autoStart ?? false,
    });
    emitTelemetry(this.options.telemetry, {
      type: "snapshot-pulled",
      nodeId: this.options.nodeId,
      snapshotId: options.manifest.snapshotId,
      transferredBlocks: transferredBlocks.length,
      durationMs: Date.now() - startedAt,
    });
    return { handle, transferredBlocks };
  }

  restoreSnapshot(
    options: DistributedBrowserPGliteRestoreOptions,
  ): Promise<DistributedBrowserPGliteRestoreResult> {
    return this.restoreFromCasSnapshot(options);
  }

  async subscribe(options: DistributedBrowserPGlitePartitionOptions): Promise<void> {
    const partition = { ...options, schemaVersion: options.schemaVersion ?? 1 };
    this.partitions.set(partition.id, partition);
    this.projectedStore.setProjection(partition.id, partition.project);
    if (this.online) {
      await this.syncNode.subscribe(partition.id, { recoverFrom: partition.recoverFrom });
    } else {
      await this.baseStore.registerPartition(partition.id);
    }
  }

  async write<TPayload>(
    partitionIdOrOptions: PartitionId | DistributedBrowserPGliteWriteOptions<TPayload>,
    payload?: TPayload,
  ): Promise<EventEnvelope<TPayload>> {
    const { input, projection } = this.localEventInput(partitionIdOrOptions, payload);
    return this.syncNode.writeEvent(input, projection);
  }

  async writeLocal<TPayload>(
    partitionIdOrOptions: PartitionId | DistributedBrowserPGliteWriteOptions<TPayload>,
    payload?: TPayload,
  ): Promise<EventEnvelope<TPayload>> {
    const { input, projection } = this.localEventInput(partitionIdOrOptions, payload);
    return this.baseStore.appendLocalEventTransaction(input, projection);
  }

  // ADR 0006 ownership helpers. Ownership is local knowledge for violation
  // detection only: claims and transfers are not replicated to peers, and
  // nothing prevents a node that skips the registry from writing anyway.
  partitionOwner(partitionId: PartitionId): NodeId | undefined {
    assertValidPartitionId(partitionId);
    return this.requireSingleWriter().ownerOf(partitionId);
  }

  claimPartition(partitionId: PartitionId, ownerNodeId: NodeId = this.options.nodeId): void {
    assertValidPartitionId(partitionId);
    this.requireSingleWriter().assign(partitionId, ownerNodeId);
  }

  transferPartition(partitionId: PartitionId, toNodeId: NodeId): void {
    assertValidPartitionId(partitionId);
    const registry = this.requireSingleWriter();
    const fromNodeId = registry.ownerOf(partitionId);
    if (fromNodeId === undefined) {
      throw new Error(`partition ${partitionId} has no writer to transfer from`);
    }
    registry.transfer({ partitionId, fromNodeId, toNodeId });
  }

  private requireSingleWriter(): SingleWriterRegistry {
    if (!this.singleWriterRegistry) {
      throw new Error(
        "single-writer ownership is not enabled; pass singleWriter when opening DistributedBrowserPGlite",
      );
    }
    return this.singleWriterRegistry;
  }

  catchUpFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    return this.syncNode.catchUpFrom(peerNodeId, partitionId);
  }

  highWatermark(partitionId: PartitionId): Promise<ReadonlyMap<NodeId, EventId>> {
    return this.syncNode.highWatermark(partitionId);
  }

  listEventsAfter(partitionId: PartitionId, cursor?: EventCursor): Promise<EventEnvelope[]> {
    return this.baseStore.listEventsAfter(partitionId, cursor);
  }

  listEventsAfterCursors(
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<EventEnvelope[]> {
    return this.baseStore.listEventsAfterCursors(partitionId, cursors);
  }

  applyEvent<TPayload>(event: EventEnvelope<TPayload>): Promise<boolean> {
    return this.projectedStore.applyRemoteEventTransaction(event);
  }

  private localEventInput<TPayload>(
    partitionIdOrOptions: PartitionId | DistributedBrowserPGliteWriteOptions<TPayload>,
    payload?: TPayload,
  ): {
    input: LocalEventInput<TPayload>;
    projection: EventProjection<TPayload> | undefined;
  } {
    const input =
      typeof partitionIdOrOptions === "string"
        ? {
            partitionId: partitionIdOrOptions,
            payload: payload as TPayload,
            schemaVersion: this.partitions.get(partitionIdOrOptions)?.schemaVersion ?? 1,
          }
        : {
            partitionId: partitionIdOrOptions.partitionId,
            payload: partitionIdOrOptions.payload,
            schemaVersion:
              partitionIdOrOptions.schemaVersion ??
              this.partitions.get(partitionIdOrOptions.partitionId)?.schemaVersion ??
              1,
          };
    assertValidPartitionId(input.partitionId);
    const configuredProjection = this.partitions.get(input.partitionId)?.project;
    const writeProjection =
      typeof partitionIdOrOptions === "string" ? undefined : partitionIdOrOptions.project;
    return {
      input,
      projection: this.wrapProjection(writeProjection ?? configuredProjection),
    };
  }

  private wrapProjection<TPayload>(
    projection: DistributedBrowserPGliteProjection<TPayload> | undefined,
  ): EventProjection<TPayload> | undefined {
    return projection ? (event) => projection(this.db, event) : undefined;
  }
}

class ProjectingBrowserStore implements PartitionStore {
  private readonly projections = new Map<PartitionId, EventProjection>();

  constructor(
    private readonly inner: SqlPartitionStore,
    private readonly db: PGlite,
    private readonly onAppliedRemote?: (event: EventEnvelope) => Promise<void> | void,
  ) {}

  setProjection<TPayload>(
    partitionId: PartitionId,
    projection: DistributedBrowserPGliteProjection<TPayload> | undefined,
  ): void {
    if (projection) {
      this.projections.set(partitionId, ((event) =>
        projection(this.db, event as EventEnvelope<TPayload>)) as EventProjection);
    } else {
      this.projections.delete(partitionId);
    }
  }

  registerPartition(partitionId: PartitionId) {
    return this.inner.registerPartition(partitionId);
  }

  hasPartition(partitionId: PartitionId) {
    return this.inner.hasPartition(partitionId);
  }

  appendLocal<TPayload>(input: LocalEventInput<TPayload>) {
    return this.inner.appendLocalEventTransaction(input, this.projections.get(input.partitionId));
  }

  appendLocalEventTransaction<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ) {
    return this.inner.appendLocalEventTransaction(
      input,
      project ?? this.projections.get(input.partitionId),
    );
  }

  async applyRemote(event: EventEnvelope): Promise<boolean> {
    return this.applyRemoteEventTransaction(event);
  }

  async applyRemoteEventTransaction<TPayload>(
    event: EventEnvelope<TPayload>,
    project?: EventProjection<TPayload>,
  ) {
    return this.inner.applyRemoteEventTransaction(event, async (appliedEvent) => {
      await (project ?? this.projections.get(appliedEvent.partitionId))?.(appliedEvent);
      await this.onAppliedRemote?.(appliedEvent);
    });
  }

  listEventsAfter(...args: Parameters<SqlPartitionStore["listEventsAfter"]>) {
    return this.inner.listEventsAfter(...args);
  }

  listEventsAfterCursors(...args: Parameters<SqlPartitionStore["listEventsAfterCursors"]>) {
    return this.inner.listEventsAfterCursors(...args);
  }

  getHighWatermark(partitionId: PartitionId) {
    return this.inner.getHighWatermark(partitionId);
  }

  getPrunedThrough(partitionId: PartitionId) {
    return this.inner.getPrunedThrough(partitionId);
  }

  pruneEvents(partitionId: PartitionId, through: ReadonlyMap<NodeId, EventId>) {
    return this.inner.pruneEvents(partitionId, through);
  }

  createSnapshotMetadata(partitionId: PartitionId) {
    return this.inner.createSnapshotMetadata(partitionId);
  }
}

function resolveSingleWriterRegistry(
  option: DistributedBrowserPGliteOpenOptions["singleWriter"],
): SingleWriterRegistry | undefined {
  if (!option) {
    return undefined;
  }
  if (option instanceof SingleWriterRegistry) {
    return option;
  }
  const registry = new SingleWriterRegistry();
  for (const [partitionId, ownerNodeId] of Object.entries(option.owners)) {
    registry.assign(partitionId, ownerNodeId);
  }
  return registry;
}

function createTransport(options: DistributedBrowserPGliteTransportOptions): Transport {
  if ("connect" in options) {
    return options;
  }
  const { type: _type, ...transportOptions } = options;
  return new BrowserWebSocketTransport(transportOptions);
}
