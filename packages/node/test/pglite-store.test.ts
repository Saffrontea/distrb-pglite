import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import { PGlitePartitionStore } from "../src/index.js";

describe("PGlitePartitionStore", () => {
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
});
