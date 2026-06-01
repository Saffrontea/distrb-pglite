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
import { compareEventIds, isUuidV7, UuidV7Factory } from "./uuid-v7.js";

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
  private readonly applied = new Map<string, EventEnvelope>();

  constructor(
    private readonly nodeId: NodeId,
    private readonly ids = new UuidV7Factory(),
  ) {
    assertValidNodeId(nodeId);
  }

  async registerPartition(partitionId: PartitionId): Promise<void> {
    assertValidPartitionId(partitionId);
    this.partitions.add(partitionId);
  }

  async hasPartition(partitionId: PartitionId): Promise<boolean> {
    assertValidPartitionId(partitionId);
    return this.partitions.has(partitionId);
  }

  async appendLocal<TPayload>(
    input: LocalEventInput<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    assertValidLocalEventInput(input);
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
    assertValidEventEnvelope(event);

    return this.record(event);
  }

  async listEventsAfter(
    partitionId: PartitionId,
    cursor?: EventCursor,
  ): Promise<EventEnvelope[]> {
    assertValidPartitionId(partitionId);
    if (cursor) {
      assertValidEventCursor(cursor);
    }
    return this.events
      .filter((event) => event.partitionId === partitionId)
      .filter((event) => !cursor || event.sourceNodeId !== cursor.sourceNodeId || compareEventIds(event.eventId, cursor.eventId) > 0)
      .sort(compareEvents);
  }

  async listEventsAfterCursors(
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<EventEnvelope[]> {
    assertValidPartitionId(partitionId);
    assertValidEventCursors(cursors);
    return this.events
      .filter((event) => event.partitionId === partitionId)
      .filter((event) => {
        const cursor = cursors.get(event.sourceNodeId);
        return !cursor || compareEventIds(event.eventId, cursor) > 0;
      })
      .sort(compareEvents);
  }

  async getHighWatermark(partitionId: PartitionId): Promise<HighWatermark> {
    assertValidPartitionId(partitionId);
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
    assertValidPartitionId(partitionId);
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
    const existing = this.applied.get(key);
    if (existing) {
      assertSameEvent(existing, event);
      return false;
    }

    this.applied.set(key, event);
    this.events.push(event);
    return true;
  }
}

export function assertValidLocalEventInput(input: LocalEventInput): void {
  assertValidPartitionId(input.partitionId);
  assertSchemaVersion(input.schemaVersion);
  assertJsonPayload(input.payload);
}

export function assertValidEventEnvelope(event: EventEnvelope): void {
  assertValidNodeId(event.sourceNodeId);
  assertValidPartitionId(event.partitionId);
  assertValidEventId(event.eventId, "eventId");
  assertSchemaVersion(event.schemaVersion);
  assertJsonPayload(event.payload);
}

export function assertValidEventCursor(cursor: EventCursor): void {
  assertValidNodeId(cursor.sourceNodeId);
  assertValidEventId(cursor.eventId, "cursor eventId");
}

export function assertValidEventCursors(cursors: ReadonlyMap<NodeId, EventId>): void {
  for (const [sourceNodeId, eventId] of cursors) {
    assertValidNodeId(sourceNodeId);
    assertValidEventId(eventId, "cursor eventId");
  }
}

export function validateEventCursorEntries(cursors: unknown): [NodeId, EventId][] {
  if (!Array.isArray(cursors)) {
    throw new Error("cursors must be an array");
  }

  const seen = new Set<NodeId>();
  return cursors.map((entry, index): [NodeId, EventId] => {
    if (!Array.isArray(entry) || entry.length !== 2) {
      throw new Error(`cursors[${index}] must be a [nodeId, eventId] tuple`);
    }
    const [sourceNodeId, eventId] = entry;
    assertValidNodeId(sourceNodeId);
    if (seen.has(sourceNodeId)) {
      throw new Error(`duplicate cursor sourceNodeId: ${sourceNodeId}`);
    }
    seen.add(sourceNodeId);
    assertValidEventId(eventId, "cursor eventId");
    return [sourceNodeId, eventId];
  });
}

export function assertValidPartitionId(partitionId: PartitionId): void {
  assertNonEmptyString(partitionId, "partitionId");
}

export function assertValidNodeId(nodeId: NodeId): void {
  assertNonEmptyString(nodeId, "nodeId");
}

function assertNonEmptyString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function assertSchemaVersion(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error("schemaVersion must be a positive integer");
  }
}

function assertValidEventId(value: unknown, name: string): asserts value is EventId {
  if (typeof value !== "string" || !isUuidV7(value)) {
    throw new Error(`${name} must be a UUIDv7: ${value}`);
  }
}

function assertJsonPayload(value: unknown): void {
  assertJsonValue(value, "payload", new Set<object>());
}

function assertJsonValue(value: unknown, path: string, seen: Set<object>): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`${path} must be JSON-serializable`);
    }
    return;
  }
  if (Array.isArray(value)) {
    assertJsonObjectNotCircular(value, path, seen);
    for (let index = 0; index < value.length; index += 1) {
      assertJsonValue(value[index], `${path}[${index}]`, seen);
    }
    seen.delete(value);
    return;
  }
  if (value && typeof value === "object") {
    if (!isPlainObject(value)) {
      throw new Error(`${path} must be JSON-serializable`);
    }
    assertJsonObjectNotCircular(value, path, seen);
    for (const [key, child] of Object.entries(value)) {
      assertJsonValue(child, `${path}.${key}`, seen);
    }
    seen.delete(value);
    return;
  }
  throw new Error(`${path} must be JSON-serializable`);
}

function assertJsonObjectNotCircular(value: object, path: string, seen: Set<object>): void {
  if (seen.has(value)) {
    throw new Error(`${path} must be JSON-serializable`);
  }
  seen.add(value);
}

function isPlainObject(value: object): boolean {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
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

function assertSameEvent(left: EventEnvelope, right: EventEnvelope): void {
  if (
    left.partitionId !== right.partitionId
    || left.schemaVersion !== right.schemaVersion
    || canonicalJson(left.payload) !== canonicalJson(right.payload)
  ) {
    throw new Error(
      `event id conflict: ${right.sourceNodeId}/${right.eventId}`,
    );
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
