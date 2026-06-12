import { mkdir } from "node:fs/promises";
import { PGlite, type PGliteOptions } from "@electric-sql/pglite";
import {
  SingleWriterRegistry,
  SyncNode,
  assertValidPartitionId,
  emitTelemetry,
  type EventEnvelope,
  type EventCursor,
  type EventProjection,
  type EventId,
  type LocalEventInput,
  type NodeId,
  type PartitionId,
  type PartitionStore,
  type StorageCatalogManifest,
  type StoragePullRequirement,
  type StorageSnapshotCatalogEntry,
  type StorageSnapshotComponent,
  type StorageSnapshotScope,
  type SingleWriterViolationError,
  type SyncTelemetrySink,
  type Transport,
} from "@distrb-pglite/core";
import { P2PWebSocketTransport, type P2PTransportOptions } from "./p2p-transport.js";
import { PGlitePartitionStore, type PGliteClient } from "./pglite-store.js";
import {
  bindStoragePartition,
  createStorageBindingGroup,
  listStorageSnapshotCatalogEntries,
  loadStorageCatalogManifest,
  loadStoragePartitionBinding,
} from "./storage-binding.js";
import {
  createManagedPartitionCasSnapshot,
  pullPartitionCasSnapshot,
  type ManagedPartitionCasSnapshot,
  type PulledPartitionCasSnapshot,
} from "./storage-orchestration.js";
import { WebSocketTransport, type WebSocketTransportOptions } from "./websocket-transport.js";
import type { CasChunking, CasManifest } from "./cas.js";

export type DistributedPGliteTransportOptions =
  | Transport
  | {
      type: "websocket";
      url: string;
      compress?: boolean;
      reconnect?: WebSocketTransportOptions["reconnect"];
    }
  | ({ type: "p2p" } & P2PTransportOptions);

export type DistributedPGlitePartitionOptions<TPayload = any> = {
  id: PartitionId;
  schemaVersion?: number;
  recoverFrom?: NodeId;
  project?: DistributedPGliteProjection<TPayload>;
  storage?: {
    groupId: string;
    rootRelation: string;
    relations: string[];
    snapshotScope?: StorageSnapshotScope;
    snapshotComponents?: StorageSnapshotComponent[];
    pullRequires?: StorageSnapshotComponent[];
  };
};

export type DistributedPGliteOpenOptions = {
  nodeId: NodeId;
  dataDir: string;
  casDir?: string;
  schema?: string;
  migrate?: (db: PGliteClient) => Promise<void> | void;
  pgliteOptions?: PGliteOptions;
  relaxedDurability?: boolean;
  transport: DistributedPGliteTransportOptions;
  partitions?: DistributedPGlitePartitionOptions[];
  autoStart?: boolean;
  autoRecoverSubscriptions?: boolean;
  singleWriter?: SingleWriterRegistry | { owners: Record<PartitionId, NodeId> };
  onSingleWriterViolation?: (error: SingleWriterViolationError) => void;
  telemetry?: SyncTelemetrySink;
};

export type DistributedPGliteWriteOptions<TPayload> = {
  partitionId: PartitionId;
  payload: TPayload;
  schemaVersion?: number;
  project?: DistributedPGliteProjection<TPayload>;
};

export type DistributedPGliteProjection<TPayload = any> = (
  db: PGliteClient,
  event: EventEnvelope<TPayload>,
) => Promise<void> | void;

export type DistributedPGliteInternals = {
  readonly db: PGlite;
  readonly eventStore: PGlitePartitionStore;
  readonly store: PartitionStore;
  readonly syncNode: SyncNode;
};

type OpenPartition = Required<Pick<DistributedPGlitePartitionOptions, "schemaVersion">> &
  Omit<DistributedPGlitePartitionOptions, "schemaVersion">;

export class DistributedPGlite {
  private dbInstance!: PGlite;
  private storeInstance!: PGlitePartitionStore;
  private projectedStore!: ProjectingPartitionStore;
  private syncNode!: SyncNode;
  private online = false;
  private readonly partitions = new Map<PartitionId, OpenPartition>();
  // Resolved once so ownership learned at runtime survives reopen() after a
  // snapshot pull instead of being rebuilt from the original open options.
  private readonly singleWriterRegistry?: SingleWriterRegistry;

  private constructor(private readonly options: DistributedPGliteOpenOptions) {
    this.singleWriterRegistry = resolveSingleWriterRegistry(options.singleWriter);
    for (const partition of options.partitions ?? []) {
      this.partitions.set(partition.id, {
        ...partition,
        schemaVersion: partition.schemaVersion ?? 1,
      });
    }
  }

  static async open(options: DistributedPGliteOpenOptions): Promise<DistributedPGlite> {
    const handle = new DistributedPGlite(options);
    await handle.openDb();
    await handle.setupStorageBindings();
    handle.createSyncNode();
    if (options.autoStart ?? true) {
      await handle.start();
    } else {
      await handle.registerConfiguredPartitions();
    }
    return handle;
  }

  get internals(): DistributedPGliteInternals {
    return {
      db: this.dbInstance,
      eventStore: this.storeInstance,
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
    return this.dbInstance.query<T>(sql, params);
  }

  exec(sql: string): Promise<unknown> {
    return this.dbInstance.exec(sql);
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
    await this.dbInstance.close();
  }

  async subscribe(options: DistributedPGlitePartitionOptions): Promise<void> {
    const partition = { ...options, schemaVersion: options.schemaVersion ?? 1 };
    this.partitions.set(partition.id, partition);
    this.projectedStore.setProjection(partition.id, this.wrapProjection(partition.project));
    await this.setupStorageBinding(partition);
    if (this.online) {
      await this.syncNode.subscribe(partition.id, { recoverFrom: partition.recoverFrom });
    } else {
      await this.storeInstance.registerPartition(partition.id);
    }
  }

  async write<TPayload>(
    partitionIdOrOptions: PartitionId | DistributedPGliteWriteOptions<TPayload>,
    payload?: TPayload,
  ): Promise<EventEnvelope<TPayload>> {
    const { input, projection } = this.localEventInput(partitionIdOrOptions, payload);
    return this.syncNode.writeEvent(input, projection);
  }

  async writeLocal<TPayload>(
    partitionIdOrOptions: PartitionId | DistributedPGliteWriteOptions<TPayload>,
    payload?: TPayload,
  ): Promise<EventEnvelope<TPayload>> {
    const { input, projection } = this.localEventInput(partitionIdOrOptions, payload);
    return this.storeInstance.appendLocalEventTransaction(input, projection);
  }

  private localEventInput<TPayload>(
    partitionIdOrOptions: PartitionId | DistributedPGliteWriteOptions<TPayload>,
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
        "single-writer ownership is not enabled; pass singleWriter when opening DistributedPGlite",
      );
    }
    return this.singleWriterRegistry;
  }

  catchUpFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    return this.syncNode.catchUpFrom(peerNodeId, partitionId);
  }

  reconcileFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    return this.syncNode.reconcileFrom(peerNodeId, partitionId);
  }

  highWatermark(partitionId: PartitionId) {
    return this.syncNode.highWatermark(partitionId);
  }

  listEventsAfter(partitionId: PartitionId, cursor?: EventCursor): Promise<EventEnvelope[]> {
    return this.storeInstance.listEventsAfter(partitionId, cursor);
  }

  listEventsAfterCursors(
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<EventEnvelope[]> {
    return this.storeInstance.listEventsAfterCursors(partitionId, cursors);
  }

  applyEvent<TPayload>(event: EventEnvelope<TPayload>): Promise<boolean> {
    return this.projectedStore.applyRemoteEventTransaction(event);
  }

  async createSnapshot(
    partitionId: PartitionId,
    options?: {
      chunkSize?: number;
      chunking?: CasChunking;
      compress?: boolean;
      saveCatalogEntry?: boolean;
      casDir?: string;
      beforeStorageSnapshot?: () => Promise<void>;
    },
  ): Promise<ManagedPartitionCasSnapshot> {
    const partition = this.requireStoragePartition(partitionId);
    const startedAt = Date.now();
    const snapshot = await createManagedPartitionCasSnapshot({
      db: this.dbInstance,
      store: this.storeInstance,
      groupId: partition.storage.groupId,
      partitionId,
      dataDir: this.options.dataDir,
      casDir: options?.casDir ?? this.requireCasDir(),
      chunkSize: options?.chunkSize,
      chunking: options?.chunking,
      compress: options?.compress,
      saveCatalogEntry: options?.saveCatalogEntry,
      beforeStorageSnapshot: options?.beforeStorageSnapshot,
    });
    emitTelemetry(this.options.telemetry, {
      type: "snapshot-created",
      nodeId: this.options.nodeId,
      partitionId,
      snapshotId: snapshot.entry.snapshotId,
      durationMs: Date.now() - startedAt,
    });
    return snapshot;
  }

  async loadCatalog(groupId: string): Promise<StorageCatalogManifest | undefined> {
    return loadStorageCatalogManifest({ db: this.dbInstance, groupId });
  }

  async listSnapshotEntries(groupId: string): Promise<StorageSnapshotCatalogEntry<CasManifest>[]> {
    return listStorageSnapshotCatalogEntries<CasManifest>({
      db: this.dbInstance,
      groupId,
    });
  }

  async pullSnapshot(options: {
    partitionId: PartitionId;
    catalogManifest?: StorageCatalogManifest;
    snapshotEntries: readonly StorageSnapshotCatalogEntry<CasManifest>[];
    requirement?: StoragePullRequirement;
    sourceCasDir?: string;
  }): Promise<PulledPartitionCasSnapshot> {
    const partition = this.requireStoragePartition(options.partitionId);
    const storageBinding =
      options.catalogManifest?.bindings.find(
        (binding) => binding.partitionId === options.partitionId,
      ) ??
      (await loadStoragePartitionBinding({
        db: this.dbInstance,
        groupId: partition.storage.groupId,
        partitionId: options.partitionId,
      }));
    const wasOnline = this.online;
    const startedAt = Date.now();
    await this.stop();
    await this.dbInstance.close();

    let pulled: PulledPartitionCasSnapshot;
    try {
      pulled = await pullPartitionCasSnapshot({
        snapshotEntries: options.snapshotEntries,
        catalogManifest: options.catalogManifest,
        groupId: partition.storage.groupId,
        partitionId: options.partitionId,
        requirement: options.requirement ?? "bootstrap",
        sourceCasDir: options.sourceCasDir,
        targetCasDir: this.requireCasDir(),
        targetDir: this.options.dataDir,
        storageBinding,
      });
    } catch (error) {
      await this.reopen(wasOnline);
      throw error;
    }

    await this.reopen(wasOnline);
    emitTelemetry(this.options.telemetry, {
      type: "snapshot-pulled",
      nodeId: this.options.nodeId,
      partitionId: options.partitionId,
      snapshotId: pulled.entry.snapshotId,
      transferredBlocks: pulled.transferredBlocks,
      durationMs: Date.now() - startedAt,
    });
    return pulled;
  }

  private async reopen(startAfterOpen: boolean): Promise<void> {
    await this.openDb();
    await this.setupStorageBindings();
    this.createSyncNode();
    if (startAfterOpen) {
      await this.start();
    } else {
      await this.registerConfiguredPartitions();
    }
  }

  private async openDb(): Promise<void> {
    await mkdir(this.options.dataDir, { recursive: true });
    if (this.options.casDir) {
      await mkdir(this.options.casDir, { recursive: true });
    }
    this.dbInstance = new PGlite(this.options.dataDir, {
      ...this.options.pgliteOptions,
      relaxedDurability:
        this.options.relaxedDurability ?? this.options.pgliteOptions?.relaxedDurability ?? false,
    });
    if (this.options.schema) {
      await this.dbInstance.exec(this.options.schema);
    }
    await this.options.migrate?.(this.dbInstance);
    this.storeInstance = new PGlitePartitionStore(this.options.nodeId, this.dbInstance);
    await this.storeInstance.migrate();
    this.projectedStore = new ProjectingPartitionStore(this.storeInstance);
    for (const partition of this.partitions.values()) {
      this.projectedStore.setProjection(partition.id, this.wrapProjection(partition.project));
    }
  }

  private createSyncNode(): void {
    this.syncNode = new SyncNode(
      this.options.nodeId,
      this.projectedStore,
      createTransport(this.options.transport),
      {
        singleWriter: this.singleWriterRegistry,
        onSingleWriterViolation: this.options.onSingleWriterViolation,
        telemetry: this.options.telemetry,
      },
    );
  }

  private async setupStorageBindings(): Promise<void> {
    const groups = new Map<string, string>();
    for (const partition of this.partitions.values()) {
      if (!partition.storage) {
        continue;
      }
      const previousRoot = groups.get(partition.storage.groupId);
      if (!previousRoot) {
        groups.set(partition.storage.groupId, partition.storage.rootRelation);
        await createStorageBindingGroup({
          db: this.dbInstance,
          groupId: partition.storage.groupId,
          rootRelation: partition.storage.rootRelation,
        });
      } else if (previousRoot !== partition.storage.rootRelation) {
        throw new Error(`storage group root relation mismatch: ${partition.storage.groupId}`);
      }
      await this.setupStorageBinding(partition);
    }
  }

  private async setupStorageBinding(partition: OpenPartition): Promise<void> {
    if (!partition.storage) {
      return;
    }
    await createStorageBindingGroup({
      db: this.dbInstance,
      groupId: partition.storage.groupId,
      rootRelation: partition.storage.rootRelation,
    });
    await bindStoragePartition({
      db: this.dbInstance,
      groupId: partition.storage.groupId,
      partitionId: partition.id,
      relations: partition.storage.relations,
      snapshotScope: partition.storage.snapshotScope ?? "catalog-and-data",
      snapshotComponents: partition.storage.snapshotComponents,
      pullRequires: partition.storage.pullRequires,
    });
  }

  private async registerConfiguredPartitions(): Promise<void> {
    for (const partition of this.partitions.values()) {
      await this.storeInstance.registerPartition(partition.id);
    }
  }

  private requireStoragePartition(partitionId: PartitionId): OpenPartition & {
    storage: NonNullable<OpenPartition["storage"]>;
  } {
    const partition = this.partitions.get(partitionId);
    if (!partition?.storage) {
      throw new Error(`partition has no storage binding: ${partitionId}`);
    }
    return partition as OpenPartition & { storage: NonNullable<OpenPartition["storage"]> };
  }

  private requireCasDir(): string {
    if (!this.options.casDir) {
      throw new Error("casDir is required for snapshot operations");
    }
    return this.options.casDir;
  }

  private wrapProjection<TPayload>(
    projection: DistributedPGliteProjection<TPayload> | undefined,
  ): EventProjection<TPayload> | undefined {
    return projection ? (event) => projection(this.dbInstance, event) : undefined;
  }
}

class ProjectingPartitionStore implements PartitionStore {
  private readonly projections = new Map<PartitionId, EventProjection>();

  constructor(private readonly inner: PGlitePartitionStore) {}

  setProjection<TPayload>(
    partitionId: PartitionId,
    projection: EventProjection<TPayload> | undefined,
  ): void {
    if (projection) {
      this.projections.set(partitionId, projection as EventProjection);
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

  applyRemote(event: EventEnvelope): Promise<boolean> {
    return this.inner.applyRemoteEventTransaction(event, this.projections.get(event.partitionId));
  }

  applyRemoteEventTransaction<TPayload>(
    event: EventEnvelope<TPayload>,
    project?: EventProjection<TPayload>,
  ) {
    return this.inner.applyRemoteEventTransaction(
      event,
      project ?? this.projections.get(event.partitionId),
    );
  }

  listEventsAfter(...args: Parameters<PGlitePartitionStore["listEventsAfter"]>) {
    return this.inner.listEventsAfter(...args);
  }

  listEventsAfterCursors(...args: Parameters<PGlitePartitionStore["listEventsAfterCursors"]>) {
    return this.inner.listEventsAfterCursors(...args);
  }

  getHighWatermark(partitionId: PartitionId) {
    return this.inner.getHighWatermark(partitionId);
  }

  createSnapshotMetadata(partitionId: PartitionId) {
    return this.inner.createSnapshotMetadata(partitionId);
  }
}

function resolveSingleWriterRegistry(
  option: DistributedPGliteOpenOptions["singleWriter"],
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

function createTransport(options: DistributedPGliteTransportOptions): Transport {
  if ("connect" in options) {
    return options;
  }
  if (options.type === "websocket") {
    return new WebSocketTransport({
      url: options.url,
      compress: options.compress,
      reconnect: options.reconnect,
    });
  }
  const { type: _type, ...p2pOptions } = options;
  return new P2PWebSocketTransport(p2pOptions);
}
