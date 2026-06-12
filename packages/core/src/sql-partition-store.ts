import {
  assertValidEventCursor,
  assertValidEventCursors,
  assertValidEventEnvelope,
  assertValidLocalEventInput,
  assertValidNodeId,
  assertValidPartitionId,
  type EventProjection,
  type PartitionStore,
} from "./store.js";
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

export type SqlClient = {
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<{ rows: T[]; affectedRows?: number }>;
  exec(sql: string): Promise<unknown>;
};

type EventRow = {
  source_node_id: string;
  event_id: string;
  partition_id: string;
  schema_version: number;
  payload: unknown;
};

export class SqlPartitionStore implements PartitionStore {
  constructor(
    private readonly nodeId: NodeId,
    private readonly db: SqlClient,
    private readonly ids = new UuidV7Factory(),
  ) {
    assertValidNodeId(nodeId);
  }

  async migrate(): Promise<void> {
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS distrb_partitions (
        partition_id text PRIMARY KEY
      );

      CREATE TABLE IF NOT EXISTS distrb_events (
        source_node_id text NOT NULL,
        event_id uuid NOT NULL,
        partition_id text NOT NULL,
        schema_version integer NOT NULL,
        payload jsonb NOT NULL,
        PRIMARY KEY (source_node_id, event_id)
      );

      CREATE INDEX IF NOT EXISTS distrb_events_partition_source_event_idx
        ON distrb_events (partition_id, source_node_id, event_id);

      CREATE TABLE IF NOT EXISTS distrb_applied_events (
        source_node_id text NOT NULL,
        event_id uuid NOT NULL,
        partition_id text NOT NULL,
        PRIMARY KEY (source_node_id, event_id)
      );

      CREATE TABLE IF NOT EXISTS distrb_snapshots (
        snapshot_id text PRIMARY KEY,
        partition_id text NOT NULL,
        high_watermark jsonb NOT NULL
      );

      CREATE TABLE IF NOT EXISTS distrb_pruned_through (
        partition_id text NOT NULL,
        source_node_id text NOT NULL,
        event_id uuid NOT NULL,
        PRIMARY KEY (partition_id, source_node_id)
      );
    `);
  }

  async registerPartition(partitionId: PartitionId): Promise<void> {
    assertValidPartitionId(partitionId);
    await this.db.query(
      "INSERT INTO distrb_partitions (partition_id) VALUES ($1) ON CONFLICT DO NOTHING",
      [partitionId],
    );
  }

  async hasPartition(partitionId: PartitionId): Promise<boolean> {
    assertValidPartitionId(partitionId);
    const result = await this.db.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM distrb_partitions WHERE partition_id = $1) AS exists",
      [partitionId],
    );
    return result.rows[0]?.exists ?? false;
  }

  async appendLocal<TPayload>(input: LocalEventInput<TPayload>): Promise<EventEnvelope<TPayload>> {
    return this.appendLocalEventTransaction(input);
  }

  async appendLocalEventTransaction<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    assertValidLocalEventInput(input);
    const event: EventEnvelope<TPayload> = {
      eventId: this.ids.next(),
      sourceNodeId: this.nodeId,
      partitionId: input.partitionId,
      schemaVersion: input.schemaVersion,
      payload: input.payload,
    };

    await this.withTransaction(async () => {
      await this.registerPartition(input.partitionId);
      await this.insertEvent(event);
      await project?.(event);
    });
    return event;
  }

  async applyRemote(event: EventEnvelope): Promise<boolean> {
    return this.applyRemoteEventTransaction(event);
  }

  async applyRemoteEventTransaction<TPayload>(
    event: EventEnvelope<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<boolean> {
    assertValidEventEnvelope(event);
    if (!(await this.hasPartition(event.partitionId))) {
      await this.assertExistingEventMatchesIfPresent(event);
      return false;
    }
    // ADR 0008: the pruned floor doubles as the idempotence floor. Events at
    // or below it had their dedupe rows deleted with them, so re-deliveries
    // from non-pruned peers must be rejected here, not re-applied.
    if (await this.isAtOrBelowPrunedThrough(event)) {
      return false;
    }

    return this.withTransaction(async () => {
      const applied = await this.db.query<{ event_id: string }>(
        `
          INSERT INTO distrb_applied_events (source_node_id, event_id, partition_id)
          VALUES ($1, $2::uuid, $3)
          ON CONFLICT DO NOTHING
          RETURNING event_id::text AS event_id
        `,
        [event.sourceNodeId, event.eventId, event.partitionId],
      );

      if (applied.rows.length === 0) {
        await this.assertExistingEventMatches(event);
        return false;
      }

      await this.insertEvent(event);
      await project?.(event);
      return true;
    });
  }

  async listEventsAfter(partitionId: PartitionId, cursor?: EventCursor): Promise<EventEnvelope[]> {
    assertValidPartitionId(partitionId);
    if (cursor) {
      assertValidEventCursor(cursor);
    }
    const rows = await this.loadPartitionEvents(partitionId);
    return rows
      .map(rowToEvent)
      .filter(
        (event) =>
          !cursor ||
          event.sourceNodeId !== cursor.sourceNodeId ||
          compareEventIds(event.eventId, cursor.eventId) > 0,
      );
  }

  async listEventsAfterCursors(
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<EventEnvelope[]> {
    assertValidPartitionId(partitionId);
    assertValidEventCursors(cursors);
    const rows = await this.loadPartitionEvents(partitionId);
    return rows.map(rowToEvent).filter((event) => {
      const cursor = cursors.get(event.sourceNodeId);
      return !cursor || compareEventIds(event.eventId, cursor) > 0;
    });
  }

  async getHighWatermark(partitionId: PartitionId): Promise<HighWatermark> {
    assertValidPartitionId(partitionId);
    // Seed with the pruned floor so the watermark never regresses below it
    // after compaction (otherwise catchup would re-request pruned ranges).
    const highWatermark = new Map<NodeId, EventId>(await this.getPrunedThrough(partitionId));
    for (const event of (await this.loadPartitionEvents(partitionId)).map(rowToEvent)) {
      const current = highWatermark.get(event.sourceNodeId);
      if (!current || compareEventIds(event.eventId, current) > 0) {
        highWatermark.set(event.sourceNodeId, event.eventId);
      }
    }
    return highWatermark;
  }

  async getPrunedThrough(partitionId: PartitionId): Promise<HighWatermark> {
    assertValidPartitionId(partitionId);
    const result = await this.db.query<{ source_node_id: string; event_id: string }>(
      `
        SELECT source_node_id, event_id::text AS event_id
        FROM distrb_pruned_through
        WHERE partition_id = $1
      `,
      [partitionId],
    );
    return new Map(result.rows.map((row) => [row.source_node_id, row.event_id]));
  }

  async pruneEvents(partitionId: PartitionId, through: HighWatermark): Promise<number> {
    assertValidPartitionId(partitionId);
    assertValidEventCursors(through);
    return this.withTransaction(async () => {
      let pruned = 0;
      for (const [sourceNodeId, eventId] of through) {
        const deleted = await this.db.query(
          `
            DELETE FROM distrb_events
            WHERE partition_id = $1
              AND source_node_id = $2
              AND event_id <= $3::uuid
          `,
          [partitionId, sourceNodeId, eventId],
        );
        pruned += deleted.affectedRows ?? 0;
        await this.db.query(
          `
            DELETE FROM distrb_applied_events
            WHERE partition_id = $1
              AND source_node_id = $2
              AND event_id <= $3::uuid
          `,
          [partitionId, sourceNodeId, eventId],
        );
        // Monotone floor: never move pruned_through backwards.
        await this.db.query(
          `
            INSERT INTO distrb_pruned_through (partition_id, source_node_id, event_id)
            VALUES ($1, $2, $3::uuid)
            ON CONFLICT (partition_id, source_node_id)
            DO UPDATE SET event_id = EXCLUDED.event_id
            WHERE distrb_pruned_through.event_id < EXCLUDED.event_id
          `,
          [partitionId, sourceNodeId, eventId],
        );
      }
      return pruned;
    });
  }

  private async isAtOrBelowPrunedThrough(event: EventEnvelope): Promise<boolean> {
    const result = await this.db.query<{ pruned: boolean }>(
      `
        SELECT EXISTS (
          SELECT 1
          FROM distrb_pruned_through
          WHERE partition_id = $1
            AND source_node_id = $2
            AND event_id >= $3::uuid
        ) AS pruned
      `,
      [event.partitionId, event.sourceNodeId, event.eventId],
    );
    return result.rows[0]?.pruned ?? false;
  }

  async createSnapshotMetadata(partitionId: PartitionId): Promise<SnapshotMetadata> {
    assertValidPartitionId(partitionId);
    const highWatermark = await this.getHighWatermark(partitionId);
    const snapshotId = `snapshot:${partitionId}:${this.nodeId}:${this.ids.next()}`;
    await this.db.query(
      `
        INSERT INTO distrb_snapshots (snapshot_id, partition_id, high_watermark)
        VALUES ($1, $2, $3::jsonb)
      `,
      [snapshotId, partitionId, JSON.stringify(Object.fromEntries(highWatermark))],
    );

    return { snapshotId, partitionId, highWatermark };
  }

  private async insertEvent(event: EventEnvelope): Promise<void> {
    const inserted = await this.db.query<{ event_id: string }>(
      `
        INSERT INTO distrb_events (
          source_node_id,
          event_id,
          partition_id,
          schema_version,
          payload
        )
        VALUES ($1, $2::uuid, $3, $4, $5::jsonb)
        ON CONFLICT DO NOTHING
        RETURNING event_id::text AS event_id
      `,
      [
        event.sourceNodeId,
        event.eventId,
        event.partitionId,
        event.schemaVersion,
        JSON.stringify(event.payload),
      ],
    );
    if (inserted.rows.length === 0) {
      await this.assertExistingEventMatches(event);
    }

    await this.db.query(
      `
        INSERT INTO distrb_applied_events (source_node_id, event_id, partition_id)
        VALUES ($1, $2::uuid, $3)
        ON CONFLICT DO NOTHING
      `,
      [event.sourceNodeId, event.eventId, event.partitionId],
    );
  }

  private async assertExistingEventMatches(event: EventEnvelope): Promise<void> {
    const matched = await this.assertExistingEventMatchesIfPresent(event);
    if (!matched) {
      throw new Error(
        `event id conflict without stored event: ${event.sourceNodeId}/${event.eventId}`,
      );
    }
  }

  private async assertExistingEventMatchesIfPresent(event: EventEnvelope): Promise<boolean> {
    const existing = await this.db.query<EventRow>(
      `
        SELECT
          source_node_id,
          event_id::text AS event_id,
          partition_id,
          schema_version,
          payload
        FROM distrb_events
        WHERE source_node_id = $1
          AND event_id = $2::uuid
      `,
      [event.sourceNodeId, event.eventId],
    );
    const row = existing.rows[0];
    if (!row) {
      return false;
    }

    if (
      row.partition_id !== event.partitionId ||
      row.schema_version !== event.schemaVersion ||
      canonicalJson(row.payload) !== canonicalJson(event.payload)
    ) {
      throw new Error(`event id conflict: ${event.sourceNodeId}/${event.eventId}`);
    }
    return true;
  }

  private async loadPartitionEvents(partitionId: PartitionId): Promise<EventRow[]> {
    const result = await this.db.query<EventRow>(
      `
        SELECT
          source_node_id,
          event_id::text AS event_id,
          partition_id,
          schema_version,
          payload
        FROM distrb_events
        WHERE partition_id = $1
        ORDER BY source_node_id, event_id
      `,
      [partitionId],
    );

    return result.rows;
  }

  private async withTransaction<T>(run: () => Promise<T>): Promise<T> {
    await this.db.exec("BEGIN");
    try {
      const result = await run();
      await this.db.exec("COMMIT");
      return result;
    } catch (error) {
      await this.db.exec("ROLLBACK");
      throw error;
    }
  }
}

function rowToEvent(row: EventRow): EventEnvelope {
  const event = {
    sourceNodeId: row.source_node_id,
    eventId: row.event_id,
    partitionId: row.partition_id,
    schemaVersion: row.schema_version,
    payload: row.payload,
  };
  assertValidEventEnvelope(event);
  return event;
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
