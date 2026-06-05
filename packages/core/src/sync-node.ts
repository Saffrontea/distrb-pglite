import {
  assertValidEventEnvelope,
  assertValidNodeId,
  assertValidPartitionId,
  isEventTransactionalPartitionStore,
  type EventProjection,
  type PartitionStore,
} from "./store.js";
import type {
  EventEnvelope,
  EventId,
  LocalEventInput,
  NodeId,
  PartitionId,
  ReplayResponse,
} from "./types.js";
import type { Transport, TransportConnection } from "./transport.js";
import {
  convergenceStatus,
  planCatchupFromStatus,
  type CatchupPlan,
  type PartitionConvergenceStatus,
} from "./convergence.js";

export type SyncNodeStartOptions = {
  autoRecoverSubscriptions?: boolean;
};

export type SubscribeOptions = {
  recoverFrom?: NodeId;
};

export class SyncNode {
  private connection?: TransportConnection;
  private readonly recoverFrom = new Map<PartitionId, NodeId>();

  constructor(
    readonly nodeId: NodeId,
    private readonly store: PartitionStore,
    private readonly transport: Transport,
  ) {
    assertValidNodeId(nodeId);
  }

  async start(options?: SyncNodeStartOptions): Promise<void> {
    if (this.connection) {
      throw new Error(`SyncNode ${this.nodeId} has already been started`);
    }
    this.connection = await this.transport.connect(this.nodeId, {
      onEvent: async (event) => {
        await this.store.applyRemote(event);
      },
      onReplayRequest: async (request): Promise<ReplayResponse> => {
        const events = await this.store.listEventsAfterCursors(
          request.partitionId,
          request.cursors,
        );
        return { partitionId: request.partitionId, events };
      },
      onReconnect: options?.autoRecoverSubscriptions
        ? async (partitionIds) => {
            for (const partitionId of partitionIds) {
              const peer = this.recoverFrom.get(partitionId);
              if (peer) {
                await this.catchUpFrom(peer, partitionId);
              }
            }
          }
        : undefined,
    });
  }

  async subscribe(partitionId: PartitionId, options?: SubscribeOptions): Promise<void> {
    assertValidPartitionId(partitionId);
    const connection = this.requireConnection();
    if (options?.recoverFrom) {
      this.recoverFrom.set(partitionId, options.recoverFrom);
    }
    await this.store.registerPartition(partitionId);
    await connection.subscribe(partitionId);
  }

  async write<TPayload>(input: LocalEventInput<TPayload>): Promise<EventEnvelope<TPayload>> {
    const connection = this.requireConnection();
    return this.store
      .appendLocal(input)
      .then((event) => connection.publish(event).then(() => event));
  }

  async writeEvent<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    if (!isEventTransactionalPartitionStore(this.store)) {
      throw new Error("SyncNode.writeEvent requires an event-transactional partition store");
    }

    const connection = this.requireConnection();
    return this.store
      .appendLocalEventTransaction(input, project)
      .then((event) => connection.publish(event).then(() => event));
  }

  async catchUpFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    assertValidNodeId(peerNodeId);
    assertValidPartitionId(partitionId);
    const cursors = await this.store.getHighWatermark(partitionId);
    return this.applyReplayFrom(peerNodeId, partitionId, cursors);
  }

  async reconcileFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    assertValidNodeId(peerNodeId);
    assertValidPartitionId(partitionId);
    return this.applyReplayFrom(peerNodeId, partitionId, new Map());
  }

  private async applyReplayFrom(
    peerNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<number> {
    const response = await this.requireConnection().requestReplay(peerNodeId, partitionId, cursors);

    if (response.partitionId !== partitionId) {
      throw new Error(
        `replay response partition mismatch: requested=${partitionId} response=${response.partitionId}`,
      );
    }

    validateReplayEvents(response.events, partitionId);

    let applied = 0;
    for (const event of response.events) {
      if (await this.store.applyRemote(event)) {
        applied += 1;
      }
    }

    return applied;
  }

  async highWatermark(partitionId: PartitionId): Promise<ReadonlyMap<NodeId, EventId>> {
    assertValidPartitionId(partitionId);
    return this.store.getHighWatermark(partitionId);
  }

  async convergenceStatus(
    partitionId: PartitionId,
    remote?: ReadonlyMap<NodeId, EventId>,
  ): Promise<PartitionConvergenceStatus> {
    assertValidPartitionId(partitionId);
    return convergenceStatus({
      partitionId,
      local: await this.store.getHighWatermark(partitionId),
      remote,
    });
  }

  async catchupPlan(
    partitionId: PartitionId,
    remote: ReadonlyMap<NodeId, EventId>,
  ): Promise<CatchupPlan> {
    assertValidPartitionId(partitionId);
    return planCatchupFromStatus(await this.convergenceStatus(partitionId, remote));
  }

  async stop(): Promise<void> {
    await this.connection?.close();
    this.connection = undefined;
  }

  private requireConnection(): TransportConnection {
    if (!this.connection) {
      throw new Error(`SyncNode ${this.nodeId} has not been started`);
    }

    return this.connection;
  }
}

function validateReplayEvents(events: readonly EventEnvelope[], partitionId: PartitionId): void {
  const seen = new Map<string, EventEnvelope>();
  for (const event of events) {
    assertValidEventEnvelope(event);
    if (event.partitionId !== partitionId) {
      throw new Error(
        `replay event partition mismatch: requested=${partitionId} event=${event.partitionId}`,
      );
    }

    const key = `${event.sourceNodeId}/${event.eventId}`;
    const existing = seen.get(key);
    if (existing) {
      assertSameReplayEvent(existing, event);
      continue;
    }
    seen.set(key, event);
  }
}

function assertSameReplayEvent(left: EventEnvelope, right: EventEnvelope): void {
  if (
    left.partitionId !== right.partitionId ||
    left.schemaVersion !== right.schemaVersion ||
    canonicalJson(left.payload) !== canonicalJson(right.payload)
  ) {
    throw new Error(`replay response event id conflict: ${right.sourceNodeId}/${right.eventId}`);
  }
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, canonicalize(child)]),
    );
  }
  return value;
}
