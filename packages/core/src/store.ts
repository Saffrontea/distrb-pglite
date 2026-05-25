import type {
  EventCursor,
  EventEnvelope,
  EventId,
  HighWatermark,
  LocalEventInput,
  NodeId,
  PartitionId,
  SnapshotMetadata,
} from "./types.js";
import { compareEventIds, UuidV7Factory } from "./uuid-v7.js";

export type PartitionStore = {
  registerPartition(partitionId: PartitionId): Promise<void>;
  hasPartition(partitionId: PartitionId): Promise<boolean>;
  appendLocal<TPayload>(input: LocalEventInput<TPayload>): Promise<EventEnvelope<TPayload>>;
  applyRemote(event: EventEnvelope): Promise<boolean>;
  listEventsAfter(partitionId: PartitionId, cursor?: EventCursor): Promise<EventEnvelope[]>;
  listEventsAfterCursors(
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<EventEnvelope[]>;
  getHighWatermark(partitionId: PartitionId): Promise<HighWatermark>;
  createSnapshotMetadata(partitionId: PartitionId): Promise<SnapshotMetadata>;
};

export type EventProjection<TPayload = unknown> = (
  event: EventEnvelope<TPayload>,
) => Promise<void> | void;

export type EventTransactionalPartitionStore = PartitionStore & {
  appendLocalEventTransaction<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<EventEnvelope<TPayload>>;
  applyRemoteEventTransaction<TPayload>(
    event: EventEnvelope<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<boolean>;
};

export function isEventTransactionalPartitionStore(
  store: PartitionStore,
): store is EventTransactionalPartitionStore {
  return "appendLocalEventTransaction" in store && "applyRemoteEventTransaction" in store;
}

export class InMemoryPartitionStore implements PartitionStore {
  private readonly partitions = new Set<PartitionId>();
  private readonly events: EventEnvelope[] = [];
  private readonly applied = new Set<string>();

  constructor(
    private readonly nodeId: NodeId,
    private readonly ids = new UuidV7Factory(),
  ) {}

  async registerPartition(partitionId: PartitionId): Promise<void> {
    this.partitions.add(partitionId);
  }

  async hasPartition(partitionId: PartitionId): Promise<boolean> {
    return this.partitions.has(partitionId);
  }

  async appendLocal<TPayload>(
    input: LocalEventInput<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    await this.registerPartition(input.partitionId);
    const event: EventEnvelope<TPayload> = {
      eventId: this.ids.next(),
      sourceNodeId: this.nodeId,
      partitionId: input.partitionId,
      schemaVersion: input.schemaVersion,
      payload: input.payload,
    };
    this.record(event);
    return event;
  }

  async applyRemote(event: EventEnvelope): Promise<boolean> {
    if (!(await this.hasPartition(event.partitionId))) {
      return false;
    }

    return this.record(event);
  }

  async listEventsAfter(
    partitionId: PartitionId,
    cursor?: EventCursor,
  ): Promise<EventEnvelope[]> {
    return this.events
      .filter((event) => event.partitionId === partitionId)
      .filter((event) => !cursor || event.sourceNodeId !== cursor.sourceNodeId || compareEventIds(event.eventId, cursor.eventId) > 0)
      .sort(compareEvents);
  }

  async listEventsAfterCursors(
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<EventEnvelope[]> {
    return this.events
      .filter((event) => event.partitionId === partitionId)
      .filter((event) => {
        const cursor = cursors.get(event.sourceNodeId);
        return !cursor || compareEventIds(event.eventId, cursor) > 0;
      })
      .sort(compareEvents);
  }

  async getHighWatermark(partitionId: PartitionId): Promise<HighWatermark> {
    const watermark = new Map<NodeId, EventId>();
    for (const event of this.events) {
      if (event.partitionId !== partitionId) {
        continue;
      }

      const current = watermark.get(event.sourceNodeId);
      if (!current || compareEventIds(event.eventId, current) > 0) {
        watermark.set(event.sourceNodeId, event.eventId);
      }
    }

    return watermark;
  }

  async createSnapshotMetadata(partitionId: PartitionId): Promise<SnapshotMetadata> {
    const highWatermark = await this.getHighWatermark(partitionId);
    const ownCursor = highWatermark.get(this.nodeId) ?? this.ids.next();
    return {
      snapshotId: `snapshot:${partitionId}:${this.nodeId}:${ownCursor}`,
      partitionId,
      highWatermark,
    };
  }

  private record(event: EventEnvelope): boolean {
    const key = appliedKey(event.sourceNodeId, event.eventId);
    if (this.applied.has(key)) {
      return false;
    }

    this.applied.add(key);
    this.events.push(event);
    return true;
  }
}

function appliedKey(nodeId: NodeId, eventId: EventId): string {
  return `${nodeId}/${eventId}`;
}

function compareEvents(left: EventEnvelope, right: EventEnvelope): number {
  const source = left.sourceNodeId.localeCompare(right.sourceNodeId);
  if (source !== 0) {
    return source;
  }

  return compareEventIds(left.eventId, right.eventId);
}
