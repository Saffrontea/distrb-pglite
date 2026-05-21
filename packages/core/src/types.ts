export type NodeId = string;
export type PartitionId = string;
export type EventId = string;
export type SnapshotId = string;
export type PgOid = number;

export type StorageSnapshotScope = "data-only" | "catalog-and-data";

export type StorageRelationLocator = {
  databaseOid: PgOid;
  tablespaceOid: PgOid;
  relationOid: PgOid;
  relfilenode?: PgOid;
};

export type StoragePartitionBinding = {
  partitionId: PartitionId;
  hypertableOid: PgOid;
  relations: StorageRelationLocator[];
  snapshotScope: StorageSnapshotScope;
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
