import {
  UuidV7Factory,
  type EventCursor,
  type EventEnvelope,
  type EventId,
  type HighWatermark,
  type LocalEventInput,
  type EventProjection,
  type NodeId,
  type PartitionId,
  type PartitionStore,
  type SnapshotMetadata,
} from "@distrb-pglite/core";

export type PGliteClient = {
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

export class PGlitePartitionStore implements PartitionStore {
  private readonly ids = new UuidV7Factory();

  constructor(
    private readonly nodeId: NodeId,
    private readonly db: PGliteClient,
  ) {}

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
    `);
  }

  async registerPartition(partitionId: PartitionId): Promise<void> {
    await this.db.query(
      "INSERT INTO distrb_partitions (partition_id) VALUES ($1) ON CONFLICT DO NOTHING",
      [partitionId],
    );
  }

  async hasPartition(partitionId: PartitionId): Promise<boolean> {
    const result = await this.db.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM distrb_partitions WHERE partition_id = $1) AS exists",
      [partitionId],
    );
    return result.rows[0]?.exists ?? false;
  }

  async appendLocal<TPayload>(
    input: LocalEventInput<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    return this.appendLocalEventTransaction(input);
  }

  async appendLocalEventTransaction<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
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
    if (!(await this.hasPartition(event.partitionId))) {
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
        return false;
      }

      await this.insertEvent(event);
      await project?.(event);
      return true;
    });
  }

  async listEventsAfter(
    partitionId: PartitionId,
    cursor?: EventCursor,
  ): Promise<EventEnvelope[]> {
    const rows = await this.loadPartitionEvents(partitionId);
    return rows
      .map(rowToEvent)
      .filter(
        (event) =>
          !cursor ||
          event.sourceNodeId !== cursor.sourceNodeId ||
          event.eventId > cursor.eventId,
      );
  }

  async listEventsAfterCursors(
    partitionId: PartitionId,
    cursors: ReadonlyMap<NodeId, EventId>,
  ): Promise<EventEnvelope[]> {
    const rows = await this.loadPartitionEvents(partitionId);
    return rows
      .map(rowToEvent)
      .filter((event) => {
        const cursor = cursors.get(event.sourceNodeId);
        return !cursor || event.eventId > cursor;
      });
  }

  async getHighWatermark(partitionId: PartitionId): Promise<HighWatermark> {
    const result = await this.db.query<{ source_node_id: string; event_id: string }>(
      `
        SELECT DISTINCT ON (source_node_id)
          source_node_id,
          event_id::text AS event_id
        FROM distrb_events
        WHERE partition_id = $1
        ORDER BY source_node_id, event_id DESC
      `,
      [partitionId],
    );

    return new Map(result.rows.map((row) => [row.source_node_id, row.event_id]));
  }

  async createSnapshotMetadata(partitionId: PartitionId): Promise<SnapshotMetadata> {
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
    await this.db.query(
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
      `,
      [
        event.sourceNodeId,
        event.eventId,
        event.partitionId,
        event.schemaVersion,
        JSON.stringify(event.payload),
      ],
    );

    await this.db.query(
      `
        INSERT INTO distrb_applied_events (source_node_id, event_id, partition_id)
        VALUES ($1, $2::uuid, $3)
        ON CONFLICT DO NOTHING
      `,
      [event.sourceNodeId, event.eventId, event.partitionId],
    );
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
  return {
    sourceNodeId: row.source_node_id,
    eventId: row.event_id,
    partitionId: row.partition_id,
    schemaVersion: row.schema_version,
    payload: row.payload,
  };
}
