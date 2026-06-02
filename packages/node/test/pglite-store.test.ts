import { UuidV7Factory } from "@distrb-pglite/core";
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import { PGlitePartitionStore } from "../src/index.js";

describe("PGlitePartitionStore", () => {
  it("rejects empty local node ids", () => {
    expect(() => new PGlitePartitionStore("", new PGlite()))
      .toThrow("nodeId must be a non-empty string");
  });

  it("stores events in PGlite and applies remote events idempotently", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    await store.registerPartition("tenant:jp");
    const local = await store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "local" },
    });

    expect(local.sourceNodeId).toBe("node-a");

    const remote = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "remote" },
    };

    await expect(store.applyRemote(remote)).resolves.toBe(true);
    await expect(store.applyRemote(remote)).resolves.toBe(false);

    const events = await store.listEventsAfterCursors("tenant:jp", new Map());
    expect(events).toHaveLength(2);
    await expect(store.getHighWatermark("tenant:jp")).resolves.toEqual(
      new Map([
        ["node-a", local.eventId],
        ["node-b", remote.eventId],
      ]),
    );
  });

  it("does not apply remote events for unregistered partitions", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    await expect(
      store.applyRemote({
        eventId: "018bcfe5-6800-7001-8000-000000000001",
        sourceNodeId: "node-b",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: { title: "ignored" },
      }),
    ).resolves.toBe(false);

    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
  });

  it("rejects empty partition ids on direct store access", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    await expect(store.registerPartition("")).rejects.toThrow("partitionId must be a non-empty string");
    await expect(store.hasPartition("")).rejects.toThrow("partitionId must be a non-empty string");
    await expect(store.listEventsAfterCursors("", new Map()))
      .rejects.toThrow("partitionId must be a non-empty string");
    await expect(store.getHighWatermark(""))
      .rejects.toThrow("partitionId must be a non-empty string");
  });

  it("rejects invalid replay cursors on direct store access", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    await expect(store.listEventsAfter("tenant:jp", {
      sourceNodeId: "node-b",
      eventId: "not-a-uuid",
    })).rejects.toThrow("cursor eventId must be a UUIDv7");
    await expect(store.listEventsAfterCursors(
      "tenant:jp",
      new Map([["node-b", "not-a-uuid"]]),
    )).rejects.toThrow("cursor eventId must be a UUIDv7");
  });

  it("uses source node plus event id as the idempotency key", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.registerPartition("tenant:jp");

    const eventId = "018bcfe5-6800-7001-8000-000000000001";
    await expect(
      store.applyRemote({
        eventId,
        sourceNodeId: "node-b",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: { source: "b" },
      }),
    ).resolves.toBe(true);
    await expect(
      store.applyRemote({
        eventId,
        sourceNodeId: "node-c",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: { source: "c" },
      }),
    ).resolves.toBe(true);

    const events = await store.listEventsAfterCursors("tenant:jp", new Map());
    expect(events).toHaveLength(2);
    expect(events.map((event) => event.sourceNodeId)).toEqual(["node-b", "node-c"]);
  });

  it("rejects conflicting duplicate events from the same source node and event id", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.registerPartition("tenant:jp");
    await store.registerPartition("tenant:us");
    const event = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { source: "original" },
    };

    await expect(store.applyRemote(event)).resolves.toBe(true);
    await expect(store.applyRemote({
      ...event,
      partitionId: "tenant:us",
      payload: { source: "conflict" },
    })).rejects.toThrow("event id conflict");
    await expect(store.listEventsAfterCursors("tenant:us", new Map())).resolves.toHaveLength(0);
  });

  it("rejects local appends when the generated event id already exists", async () => {
    const db = new PGlite();
    const ids = new FixedUuidFactory("018bcfe5-6800-7001-8000-000000000001");
    const store = new PGlitePartitionStore("node-a", db, ids);
    await store.migrate();

    await expect(store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { source: "first" },
    })).resolves.toMatchObject({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
    });
    await expect(store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { source: "second" },
    })).rejects.toThrow("event id conflict");
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(1);
  });

  it("treats duplicate JSON payloads with different object key order as the same event", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.registerPartition("tenant:jp");
    const event = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { a: 1, b: 2 },
    };

    await expect(store.applyRemote(event)).resolves.toBe(true);
    await expect(store.applyRemote({
      ...event,
      payload: { b: 2, a: 1 },
    })).resolves.toBe(false);
  });

  it("rejects local events whose payload cannot round-trip as JSON", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    await expect(store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: Number.NaN },
    })).rejects.toThrow("payload.value must be JSON-serializable");
    await expect(store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: undefined },
    })).rejects.toThrow("payload.value must be JSON-serializable");
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
  });

  it("rejects registered remote events with invalid envelope fields", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.registerPartition("tenant:jp");

    await expect(store.applyRemote({
      eventId: "not-a-uuid",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { ok: true },
    })).rejects.toThrow("eventId must be a UUIDv7");
    await expect(store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 0,
      payload: { ok: true },
    })).rejects.toThrow("schemaVersion must be a positive integer");
    await expect(store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: Infinity },
    })).rejects.toThrow("payload.value must be JSON-serializable");
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
  });

  it("rejects malformed remote events before checking partition registration", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    await expect(store.applyRemote({
      eventId: "not-a-uuid",
      sourceNodeId: "node-b",
      partitionId: "tenant:missing",
      schemaVersion: 1,
      payload: { ok: true },
    })).rejects.toThrow("eventId must be a UUIDv7");
    await expect(store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:missing",
      schemaVersion: 0,
      payload: { ok: true },
    })).rejects.toThrow("schemaVersion must be a positive integer");
    await expect(store.listEventsAfterCursors("tenant:missing", new Map())).resolves.toHaveLength(0);
  });

  it("rejects corrupted persisted events before replaying or snapshotting cursors", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.registerPartition("tenant:jp");
    await db.query(
      `
        INSERT INTO distrb_events (
          source_node_id,
          event_id,
          partition_id,
          schema_version,
          payload
        )
        VALUES ($1, $2::uuid, $3, $4, $5::jsonb)
      `,
      [
        "node-b",
        "550e8400-e29b-41d4-a716-446655440000",
        "tenant:jp",
        1,
        JSON.stringify({ value: "corrupt" }),
      ],
    );

    await expect(store.listEventsAfterCursors("tenant:jp", new Map()))
      .rejects.toThrow("eventId must be a UUIDv7");
    await expect(store.getHighWatermark("tenant:jp"))
      .rejects.toThrow("eventId must be a UUIDv7");
    await expect(store.createSnapshotMetadata("tenant:jp"))
      .rejects.toThrow("eventId must be a UUIDv7");
  });

  it("rejects corrupted older persisted events before snapshotting cursors", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.registerPartition("tenant:jp");
    await db.query(
      `
        INSERT INTO distrb_events (
          source_node_id,
          event_id,
          partition_id,
          schema_version,
          payload
        )
        VALUES
          ($1, $2::uuid, $4, 1, $5::jsonb),
          ($1, $3::uuid, $4, 1, $6::jsonb)
      `,
      [
        "node-b",
        "00000000-0000-4000-8000-000000000000",
        "018bcfe5-6800-7001-8000-000000000001",
        "tenant:jp",
        JSON.stringify({ value: "corrupt-older" }),
        JSON.stringify({ value: "valid-newer" }),
      ],
    );

    await expect(store.getHighWatermark("tenant:jp"))
      .rejects.toThrow("eventId must be a UUIDv7");
    await expect(store.createSnapshotMetadata("tenant:jp"))
      .rejects.toThrow("eventId must be a UUIDv7");
  });

  it("replays events after each source node cursor independently", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.registerPartition("tenant:jp");

    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b2" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-c",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "c1" },
    });

    const replay = await store.listEventsAfterCursors(
      "tenant:jp",
      new Map([["node-b", "018bcfe5-6800-7001-8000-000000000001"]]),
    );

    expect(replay.map((event) => event.payload)).toEqual([
      { value: "b2" },
      { value: "c1" },
    ]);
  });

  it("serializes concurrent local appends on the same store", async () => {
    const db = new PGlite();
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        store.appendLocal({
          partitionId: "tenant:jp",
          schemaVersion: 1,
          payload: { index },
        }),
      ),
    );

    const events = await store.listEventsAfterCursors("tenant:jp", new Map());
    expect(events.map((event) => event.payload)).toEqual(
      Array.from({ length: 8 }, (_, index) => ({ index })),
    );
  });
});

class FixedUuidFactory extends UuidV7Factory {
  constructor(private readonly value: string) {
    super();
  }

  override next(): string {
    return this.value;
  }
}
