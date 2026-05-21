import type {
  EventEnvelope,
  EventId,
  NodeId,
  PartitionId,
  ReplayResponse,
} from "./types.js";
import type { Transport, TransportConnection, TransportHandlers } from "./transport.js";

export class MemoryTransport implements Transport {
  private readonly connections = new Map<NodeId, MemoryConnection>();

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    const connection = new MemoryConnection(nodeId, handlers, this);
    this.connections.set(nodeId, connection);
    return connection;
  }

  async publish(sourceNodeId: NodeId, event: EventEnvelope): Promise<void> {
    await Promise.all(
      [...this.connections.values()]
        .filter((connection) => connection.nodeId !== sourceNodeId)
        .filter((connection) => connection.subscriptions.has(event.partitionId))
        .map((connection) => connection.handlers.onEvent(event)),
    );
  }

  async requestReplay(
    requesterNodeId: NodeId,
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    const target = this.connections.get(targetNodeId);
    if (!target?.handlers.onReplayRequest) {
      return { partitionId, events: [] };
    }

    return target.handlers.onReplayRequest({
      requesterNodeId,
      partitionId,
      cursors,
    });
  }

  disconnect(nodeId: NodeId): void {
    this.connections.delete(nodeId);
  }
}

class MemoryConnection implements TransportConnection {
  readonly subscriptions = new Set<PartitionId>();

  constructor(
    readonly nodeId: NodeId,
    readonly handlers: TransportHandlers,
    private readonly transport: MemoryTransport,
  ) {}

  async subscribe(partitionId: PartitionId): Promise<void> {
    this.subscriptions.add(partitionId);
  }

  async publish(event: EventEnvelope): Promise<void> {
    await this.transport.publish(this.nodeId, event);
  }

  async requestReplay(
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    return this.transport.requestReplay(this.nodeId, targetNodeId, partitionId, cursors);
  }

  async close(): Promise<void> {
    this.transport.disconnect(this.nodeId);
  }
}

