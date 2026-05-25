import { isEventTransactionalPartitionStore, type EventProjection, type PartitionStore } from "./store.js";
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
  ) {}

  async start(options?: SyncNodeStartOptions): Promise<void> {
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
    if (options?.recoverFrom) {
      this.recoverFrom.set(partitionId, options.recoverFrom);
    }
    await this.requireConnection().subscribe(partitionId);
    await this.store.registerPartition(partitionId);
  }

  async write<TPayload>(
    input: LocalEventInput<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    return this.store
      .appendLocal(input)
      .then((event) => this.requireConnection().publish(event).then(() => event));
  }

  async writeEvent<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    if (!isEventTransactionalPartitionStore(this.store)) {
      throw new Error("SyncNode.writeEvent requires an event-transactional partition store");
    }

    return this.store
      .appendLocalEventTransaction(input, project)
      .then((event) => this.requireConnection().publish(event).then(() => event));
  }

  async catchUpFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    const cursors = await this.store.getHighWatermark(partitionId);
    return this.applyReplayFrom(peerNodeId, partitionId, cursors);
  }

  async reconcileFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    return this.applyReplayFrom(peerNodeId, partitionId, new Map());
  }

  private async applyReplayFrom(
    peerNodeId: NodeId,
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<number> {
    const response = await this.requireConnection().requestReplay(
      peerNodeId,
      partitionId,
      cursors,
    );

    let applied = 0;
    for (const event of response.events) {
      if (await this.store.applyRemote(event)) {
        applied += 1;
      }
    }

    return applied;
  }

  async highWatermark(partitionId: PartitionId): Promise<ReadonlyMap<NodeId, EventId>> {
    return this.store.getHighWatermark(partitionId);
  }

  async convergenceStatus(
    partitionId: PartitionId,
    remote?: ReadonlyMap<NodeId, EventId>,
  ): Promise<PartitionConvergenceStatus> {
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
