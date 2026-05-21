export type NodeId = string;
export type PartitionId = string;
export type EventId = string;
export type SnapshotId = string;

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

