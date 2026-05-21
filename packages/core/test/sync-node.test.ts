import { describe, expect, it } from "vitest";
import {
  InMemoryPartitionStore,
  isUuidV7,
  MemoryTransport,
  SyncNode,
  UuidV7Factory,
} from "../src/index.js";

describe("UuidV7Factory", () => {
  it("generates UUID v7 ids that sort in generation order for one factory", () => {
    let now = 1_700_000_000_000;
    const ids = new UuidV7Factory(
      () => now,
      (length) => new Uint8Array(length).fill(1),
    );

    const first = ids.next();
    const second = ids.next();
    now += 1;
    const third = ids.next();

    expect(isUuidV7(first)).toBe(true);
    expect([first, second, third].toSorted()).toEqual([first, second, third]);
  });
});

describe("SyncNode", () => {
  it("replicates subscribed partitions and ignores unsubscribed partitions", async () => {
    const transport = new MemoryTransport();
    const nodeA = new SyncNode("node-a", new InMemoryPartitionStore("node-a"), transport);
    const nodeBStore = new InMemoryPartitionStore("node-b");
    const nodeB = new SyncNode("node-b", nodeBStore, transport);

    await nodeA.start();
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    await nodeA.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "visible" },
    });
    await nodeA.write({
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { title: "ignored" },
    });

    const visible = await nodeBStore.listEventsAfterCursors("tenant:jp", new Map());
    const ignored = await nodeBStore.listEventsAfterCursors("tenant:us", new Map());

    expect(visible).toHaveLength(1);
    expect(visible[0]?.payload).toEqual({ title: "visible" });
    expect(ignored).toHaveLength(0);
  });

  it("applies duplicate events idempotently by source node and event id", async () => {
    const store = new InMemoryPartitionStore("node-b");
    await store.registerPartition("tenant:jp");

    const event = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "once" },
    };

    await expect(store.applyRemote(event)).resolves.toBe(true);
    await expect(store.applyRemote(event)).resolves.toBe(false);
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(1);
  });

  it("treats the same event id from different source nodes as distinct events", async () => {
    const store = new InMemoryPartitionStore("node-c");
    await store.registerPartition("tenant:jp");
    const eventId = "018bcfe5-6800-7001-8000-000000000001";

    await expect(
      store.applyRemote({
        eventId,
        sourceNodeId: "node-a",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: { source: "a" },
      }),
    ).resolves.toBe(true);
    await expect(
      store.applyRemote({
        eventId,
        sourceNodeId: "node-b",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: { source: "b" },
      }),
    ).resolves.toBe(true);

    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(2);
  });

  it("filters replay with per-source-node cursors instead of a global cursor", async () => {
    const store = new InMemoryPartitionStore("node-c");
    await store.registerPartition("tenant:jp");

    const nodeAFirst = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    };
    const nodeASecond = {
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a2" },
    };
    const nodeBOlderThanNodeACursor = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    };

    await store.applyRemote(nodeAFirst);
    await store.applyRemote(nodeASecond);
    await store.applyRemote(nodeBOlderThanNodeACursor);

    const replay = await store.listEventsAfterCursors(
      "tenant:jp",
      new Map([["node-a", nodeAFirst.eventId]]),
    );

    expect(replay.map((event) => event.payload)).toEqual([
      { value: "a2" },
      { value: "b1" },
    ]);
  });

  it("does not apply remote events for unregistered partitions", async () => {
    const store = new InMemoryPartitionStore("node-b");

    await expect(
      store.applyRemote({
        eventId: "018bcfe5-6800-7001-8000-000000000001",
        sourceNodeId: "node-a",
        partitionId: "tenant:jp",
        schemaVersion: 1,
        payload: { title: "ignored" },
      }),
    ).resolves.toBe(false);
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
  });

  it("catches up from a peer using per-source-node cursors", async () => {
    const transport = new MemoryTransport();
    const nodeA = new SyncNode("node-a", new InMemoryPartitionStore("node-a"), transport);
    const nodeBStore = new InMemoryPartitionStore("node-b");
    const nodeB = new SyncNode("node-b", nodeBStore, transport);

    await nodeA.start();
    await nodeA.subscribe("tenant:jp");
    await nodeA.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: 1 },
    });
    await nodeA.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: 2 },
    });

    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    await expect(nodeB.catchUpFrom("node-a", "tenant:jp")).resolves.toBe(2);
    await expect(nodeB.catchUpFrom("node-a", "tenant:jp")).resolves.toBe(0);

    const watermark = await nodeB.highWatermark("tenant:jp");
    expect(watermark.has("node-a")).toBe(true);
  });

  it("captures snapshot metadata as a source-node high-watermark vector", async () => {
    const store = new InMemoryPartitionStore("node-c");
    await store.registerPartition("tenant:jp");
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a2" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });

    const snapshot = await store.createSnapshotMetadata("tenant:jp");

    expect(snapshot.partitionId).toBe("tenant:jp");
    expect(snapshot.highWatermark).toEqual(
      new Map([
        ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
        ["node-b", "018bcfe5-6800-7001-8000-000000000003"],
      ]),
    );
  });
});
