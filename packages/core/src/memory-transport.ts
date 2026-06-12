import type { EventEnvelope, EventId, NodeId, PartitionId, ReplayResponse } from "./types.js";
import type { Transport, TransportConnection, TransportHandlers } from "./transport.js";
import {
  assertValidEventCursors,
  assertValidEventEnvelope,
  assertValidNodeId,
  assertValidPartitionId,
  validateEventCursorEntries,
} from "./store.js";

export class MemoryTransport implements Transport {
  private readonly connections = new Map<NodeId, MemoryConnection>();

  async connect(nodeId: NodeId, handlers: TransportHandlers): Promise<TransportConnection> {
    assertValidNodeId(nodeId);
    this.connections.get(nodeId)?.deactivate();
    const connection = new MemoryConnection(nodeId, handlers, this);
    this.connections.set(nodeId, connection);
    return connection;
  }

  async publish(source: MemoryConnection, event: EventEnvelope): Promise<void> {
    this.assertCurrent(source);
    await Promise.all(
      [...this.connections.values()]
        .filter((connection) => connection.nodeId !== source.nodeId)
        .filter((connection) => connection.subscriptions.has(event.partitionId))
        .map((connection) => connection.handlers.onEvent(event)),
    );
  }

  async requestReplay(
    requester: MemoryConnection,
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    this.assertCurrent(requester);
    const target = this.connections.get(targetNodeId);
    if (!target?.handlers.onReplayRequest) {
      return { partitionId, events: [] };
    }

    const response = await target.handlers.onReplayRequest({
      requesterNodeId: requester.nodeId,
      partitionId,
      cursors,
    });
    assertValidReplayResponse(response, partitionId);
    return response;
  }

  disconnect(connection: MemoryConnection): void {
    if (this.connections.get(connection.nodeId) === connection) {
      this.connections.delete(connection.nodeId);
    }
  }

  private assertCurrent(connection: MemoryConnection): void {
    if (!connection.isOpen || this.connections.get(connection.nodeId) !== connection) {
      throw new Error(`MemoryTransport connection for ${connection.nodeId} is closed`);
    }
  }
}

class MemoryConnection implements TransportConnection {
  readonly subscriptions = new Set<PartitionId>();
  private open = true;

  constructor(
    readonly nodeId: NodeId,
    readonly handlers: TransportHandlers,
    private readonly transport: MemoryTransport,
  ) {}

  get isOpen(): boolean {
    return this.open;
  }

  deactivate(): void {
    this.open = false;
    this.subscriptions.clear();
  }

  async subscribe(partitionId: PartitionId): Promise<void> {
    this.ensureOpen();
    assertValidPartitionId(partitionId);
    this.subscriptions.add(partitionId);
  }

  async publish(event: EventEnvelope): Promise<void> {
    this.ensureOpen();
    assertValidEventEnvelope(event);
    assertNodeMatches("event source", this.nodeId, event.sourceNodeId);
    await this.transport.publish(this, event);
  }

  async requestReplay(
    targetNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<ReplayResponse> {
    this.ensureOpen();
    assertValidNodeId(targetNodeId);
    assertValidPartitionId(partitionId);
    assertValidEventCursors(cursors);
    return this.transport.requestReplay(this, targetNodeId, partitionId, cursors);
  }

  async close(): Promise<void> {
    if (!this.open) {
      return;
    }
    this.deactivate();
    this.transport.disconnect(this);
  }

  private ensureOpen(): void {
    if (!this.open) {
      throw new Error(`MemoryTransport connection for ${this.nodeId} is closed`);
    }
  }
}

function assertNodeMatches(name: string, expected: NodeId, actual: NodeId): void {
  if (actual !== expected) {
    throw new Error(`${name} mismatch: expected=${expected} actual=${actual}`);
  }
}

function assertValidReplayResponse(
  response: ReplayResponse,
  requestedPartitionId: PartitionId,
): void {
  assertValidPartitionId(response.partitionId);
  if (response.partitionId !== requestedPartitionId) {
    throw new Error(
      `replay response partition mismatch: requested=${requestedPartitionId} response=${response.partitionId}`,
    );
  }
  if (response.snapshotRequired) {
    validateEventCursorEntries(response.snapshotRequired.prunedThrough);
    if (response.events.length > 0) {
      throw new Error("snapshot-required replay response must not carry events");
    }
  }
  for (const event of response.events) {
    assertValidEventEnvelope(event);
    if (event.partitionId !== response.partitionId) {
      throw new Error(
        `replay event partition mismatch: response=${response.partitionId} event=${event.partitionId}`,
      );
    }
  }
}
