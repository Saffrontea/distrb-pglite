import type {
  EventEnvelope,
  EventId,
  NodeId,
  PartitionId,
  ReplayRequest,
  ReplayResponse,
} from "./types.js";

export type TransportHandlers = {
  onEvent(event: EventEnvelope): Promise<void> | void;
  onReplayRequest?(request: ReplayRequest): Promise<ReplayResponse> | ReplayResponse;
  onReconnect?(partitionIds: ReadonlySet<PartitionId>): Promise<void> | void;
};

export type TransportConnection = {
  subscribe(partitionId: PartitionId): Promise<void>;
  publish(event: EventEnvelope): Promise<void>;
  requestReplay(
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse>;
  close(): Promise<void>;
};

export type Transport = {
  connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection>;
};
