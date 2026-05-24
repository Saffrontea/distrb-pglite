import type { PartitionStore } from "./store.js";
import type {
  EventEnvelope,
  EventId,
  LocalEventInput,
  NodeId,
  PartitionId,
  ReplayResponse,
} from "./types.js";
import type { Transport, TransportConnection } from "./transport.js";

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
    const event = await this.store.appendLocal(input);
    await this.requireConnection().publish(event);
    return event;
  }

  async catchUpFrom(peerNodeId: NodeId, partitionId: PartitionId): Promise<number> {
    const cursors = await this.store.getHighWatermark(partitionId);
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

