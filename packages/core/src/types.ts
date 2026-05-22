export type NodeId = string;
export type PartitionId = string;
export type EventId = string;
export type SnapshotId = string;
export type PgOid = number;

export type StorageSnapshotScope = "data-only" | "catalog-and-data";
export type StorageSnapshotComponent = "catalog" | "data";
export type StoragePullRequirement = "bootstrap" | "catalog-refresh" | "data-refresh";
export type StorageRelationRole = "root" | "child" | "index" | "toast";

export type StorageRelationLocator = {
  databaseOid: PgOid;
  tablespaceOid: PgOid;
  relationOid: PgOid;
  relfilenode?: PgOid;
  storagePath?: string;
  role?: StorageRelationRole;
  parentRelationOid?: PgOid;
};

export type StoragePartitionBinding = {
  partitionId: PartitionId;
  rootRelationOid: PgOid;
  relations: StorageRelationLocator[];
  snapshotScope: StorageSnapshotScope;
  snapshotComponents?: StorageSnapshotComponent[];
  pullRequires?: StorageSnapshotComponent[];
  ddlGeneration?: number;
};

export type StorageBindingGroup = {
  groupId: string;
  rootRelationOid: PgOid;
  bindings: StoragePartitionBinding[];
  ddlGeneration: number;
  vacuumPolicy?: {
    retainSnapshotGenerations: number;
    gcAfterGeneration: number;
  };
};

export type StorageGcPlan = {
  ddlGeneration: number;
  retainFromGeneration: number;
  protectedGenerations: number[];
  gcAllowedThroughGeneration: number;
  canGcGeneration: (generation: number) => boolean;
};

export type StorageSnapshotManifestKind = "cas" | "filesystem";

export type StorageSnapshotCatalogEntry<TManifest = unknown> = {
  groupId: string;
  partitionId: PartitionId;
  snapshotId: SnapshotId;
  components: StorageSnapshotComponent[];
  highWatermark: [NodeId, EventId][];
  ddlGeneration: number;
  manifestKind: StorageSnapshotManifestKind;
  manifest: TManifest;
};

export type EventCursor = {
  sourceNodeId: NodeId;
  eventId: EventId;
};

export type HighWatermark = ReadonlyMap<NodeId, EventId>;

export type EventEnvelope<TPayload = unknown> = {
  eventId: EventId;
  sourceNodeId: NodeId;
  partitionId: PartitionId;
  schemaVersion: number;
  payload: TPayload;
};

export type LocalEventInput<TPayload = unknown> = {
  partitionId: PartitionId;
  schemaVersion: number;
  payload: TPayload;
};

export type SnapshotMetadata = {
  snapshotId: SnapshotId;
  partitionId: PartitionId;
  storageBinding?: StoragePartitionBinding;
  highWatermark: HighWatermark;
};

export type ReplayRequest = {
  requesterNodeId: NodeId;
  partitionId: PartitionId;
  cursors: ReadonlyMap<NodeId, EventId>;
};

export type ReplayResponse = {
  partitionId: PartitionId;
  events: EventEnvelope[];
};
