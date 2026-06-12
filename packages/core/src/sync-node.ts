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
import { SingleWriterViolationError, type SingleWriterRegistry } from "./single-writer.js";
import { emitTelemetry, type SyncTelemetrySink } from "./telemetry.js";

export type SyncNodeStartOptions = {
  autoRecoverSubscriptions?: boolean;
};

export type SubscribeOptions = {
  recoverFrom?: NodeId;
};

// ADR 0006 detection is opt-in: pass a registry to reject events that break
// the single-writer-per-partition invariant. Without onViolation the error
// propagates; with it, violating remote events are skipped and reported.
export type SyncNodeOptions = {
  singleWriter?: SingleWriterRegistry;
  onSingleWriterViolation?: (error: SingleWriterViolationError) => void;
  telemetry?: SyncTelemetrySink;
};

export class SyncNode {
  private connection?: TransportConnection;
  private connectionEpoch = 0;
  private activeHandlers = 0;
  private readonly handlerIdleWaiters = new Set<() => void>();
  private readonly recoverFrom = new Map<PartitionId, NodeId>();

  constructor(
    readonly nodeId: NodeId,
    private readonly store: PartitionStore,
    private readonly transport: Transport,
    private readonly options: SyncNodeOptions = {},
  ) {
    assertValidNodeId(nodeId);
  }

  async start(options?: SyncNodeStartOptions): Promise<void> {
    if (this.connection) {
      throw new Error(`SyncNode ${this.nodeId} has already been started`);
    }
    const epoch = ++this.connectionEpoch;
    this.connection = await this.transport.connect(this.nodeId, {
      onEvent: async (event) => {
        await this.runConnectionHandler(epoch, undefined, async () => {
          await this.applyRemoteEvent(event, "live");
        });
      },
      onReplayRequest: async (request): Promise<ReplayResponse> => {
        return await this.runConnectionHandler(
          epoch,
          { partitionId: request.partitionId, events: [] },
          async () => {
            const events = await this.store.listEventsAfterCursors(
              request.partitionId,
              request.cursors,
            );
            emitTelemetry(this.options.telemetry, {
              type: "replay-served",
              nodeId: this.nodeId,
              partitionId: request.partitionId,
              events: events.length,
            });
            return { partitionId: request.partitionId, events };
          },
        );
      },
      onReconnect: options?.autoRecoverSubscriptions
        ? async (partitionIds) => {
            await this.runConnectionHandler(epoch, undefined, async () => {
              for (const partitionId of partitionIds) {
                const peer = this.recoverFrom.get(partitionId);
                if (peer) {
                  const applied = await this.catchUpFrom(peer, partitionId);
                  emitTelemetry(this.options.telemetry, {
                    type: "subscription-recovered",
                    nodeId: this.nodeId,
                    partitionId,
                    peerNodeId: peer,
                    applied,
                  });
                }
              }
            });
          }
        : undefined,
    });
    emitTelemetry(this.options.telemetry, { type: "sync-started", nodeId: this.nodeId });
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
    this.claimLocalWrite(input.partitionId);
    return this.store.appendLocal(input).then((event) => {
      this.emitLocalWrite(event);
      return connection.publish(event).then(() => event);
    });
  }

  async writeEvent<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    if (!isEventTransactionalPartitionStore(this.store)) {
      throw new Error("SyncNode.writeEvent requires an event-transactional partition store");
    }

    const connection = this.requireConnection();
    this.claimLocalWrite(input.partitionId);
    return this.store.appendLocalEventTransaction(input, project).then((event) => {
      this.emitLocalWrite(event);
      return connection.publish(event).then(() => event);
    });
  }

  async catchUpFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    assertValidNodeId(peerNodeId);
    assertValidPartitionId(partitionId);
    const cursors = await this.store.getHighWatermark(partitionId);
    return this.applyReplayFrom(peerNodeId, partitionId, cursors, "catchup");
  }

  async reconcileFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    assertValidNodeId(peerNodeId);
    assertValidPartitionId(partitionId);
    return this.applyReplayFrom(peerNodeId, partitionId, new Map(), "reconcile");
  }

  private async applyReplayFrom(
    peerNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
    mode: "catchup" | "reconcile",
  ): Promise<number> {
    const startedAt = Date.now();
    const response = await this.requireConnection().requestReplay(peerNodeId, partitionId, cursors);

    if (response.partitionId !== partitionId) {
      throw new Error(
        `replay response partition mismatch: requested=${partitionId} response=${response.partitionId}`,
      );
    }

    validateReplayEvents(response.events, partitionId);

    let applied = 0;
    for (const event of response.events) {
      if (await this.applyRemoteEvent(event, "replay")) {
        applied += 1;
      }
    }

    emitTelemetry(this.options.telemetry, {
      type: "catchup",
      nodeId: this.nodeId,
      peerNodeId,
      partitionId,
      mode,
      received: response.events.length,
      applied,
      durationMs: Date.now() - startedAt,
    });

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
    this.connectionEpoch += 1;
    const connection = this.connection;
    this.connection = undefined;
    await connection?.close();
    await this.waitForConnectionHandlers();
    if (connection) {
      emitTelemetry(this.options.telemetry, { type: "sync-stopped", nodeId: this.nodeId });
    }
  }

  // Local writes always throw on violation: writing into a partition owned by
  // another node is a local protocol error, not something to skip silently.
  private claimLocalWrite(partitionId: PartitionId): void {
    this.options.singleWriter?.observe({ partitionId, sourceNodeId: this.nodeId });
  }

  private emitLocalWrite(event: EventEnvelope): void {
    emitTelemetry(this.options.telemetry, {
      type: "local-write",
      nodeId: this.nodeId,
      partitionId: event.partitionId,
      eventId: event.eventId,
    });
  }

  // Admits, applies, and reports one remote event. Returns true only when the
  // event was newly merged into the local store.
  private async applyRemoteEvent(
    event: EventEnvelope,
    context: "live" | "replay",
  ): Promise<boolean> {
    const emitOutcome = (outcome: "applied" | "duplicate" | "rejected-single-writer") =>
      emitTelemetry(this.options.telemetry, {
        type: "remote-event",
        nodeId: this.nodeId,
        partitionId: event.partitionId,
        sourceNodeId: event.sourceNodeId,
        eventId: event.eventId,
        outcome,
        context,
      });

    if (!this.admitEvent(event)) {
      emitOutcome("rejected-single-writer");
      return false;
    }
    const applied = await this.store.applyRemote(event);
    emitOutcome(applied ? "applied" : "duplicate");
    return applied;
  }

  private admitEvent(event: EventEnvelope): boolean {
    const registry = this.options.singleWriter;
    if (!registry) {
      return true;
    }
    try {
      registry.observe(event);
      return true;
    } catch (error) {
      if (error instanceof SingleWriterViolationError && this.options.onSingleWriterViolation) {
        this.options.onSingleWriterViolation(error);
        return false;
      }
      throw error;
    }
  }

  private requireConnection(): TransportConnection {
    if (!this.connection) {
      throw new Error(`SyncNode ${this.nodeId} has not been started`);
    }

    return this.connection;
  }

  private async runConnectionHandler<T>(
    epoch: number,
    staleValue: T,
    run: () => Promise<T>,
  ): Promise<T> {
    this.activeHandlers += 1;
    try {
      if (epoch !== this.connectionEpoch) {
        return staleValue;
      }
      return await run();
    } finally {
      this.activeHandlers -= 1;
      if (this.activeHandlers === 0) {
        for (const resolve of this.handlerIdleWaiters) {
          resolve();
        }
        this.handlerIdleWaiters.clear();
      }
    }
  }

  private async waitForConnectionHandlers(): Promise<void> {
    if (this.activeHandlers === 0) {
      return;
    }
    await new Promise<void>((resolve) => {
      this.handlerIdleWaiters.add(resolve);
    });
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
