import { describe, expect, it, vi } from "vitest";
import type {
  EventEnvelope,
  EventProjection,
  LocalEventInput,
  ReplayResponse,
  Transport,
  TransportConnection,
  TransportHandlers,
} from "../src/index.js";
import {
  InMemoryPartitionStore,
  compareHighWatermarks,
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

  it("keeps UUID v7 ids unique and ordered when one millisecond overflows the sequence", () => {
    const ids = new UuidV7Factory(
      () => 1_700_000_000_000,
      (length) => new Uint8Array(length).fill(0),
    );

    const generated = Array.from({ length: 4_097 }, () => ids.next());

    expect(new Set(generated).size).toBe(generated.length);
    expect(generated.toSorted()).toEqual(generated);
    expect(generated[4_096]!.slice(0, 13)).toBe(generated[0]!.slice(0, 13));
  });
});

describe("SyncNode", () => {
  it("rejects empty local node ids", () => {
    expect(() => new InMemoryPartitionStore(""))
      .toThrow("nodeId must be a non-empty string");
    expect(() => new SyncNode("", new InMemoryPartitionStore("node-a"), new MemoryTransport()))
      .toThrow("nodeId must be a non-empty string");
  });

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

  it("does not append local events when write is attempted before start", async () => {
    const store = new InMemoryPartitionStore("node-a");
    const node = new SyncNode("node-a", store, new MemoryTransport());

    await expect(node.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "not connected" },
    })).rejects.toThrow("SyncNode node-a has not been started");
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
  });

  it("does not run transactional writes when writeEvent is attempted before start", async () => {
    const store = new TransactionalMemoryStore("node-a");
    const node = new SyncNode("node-a", store, new MemoryTransport());
    const project = vi.fn();

    await expect(node.writeEvent({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "not connected" },
    }, project)).rejects.toThrow("SyncNode node-a has not been started");
    expect(project).not.toHaveBeenCalled();
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
  });

  it("rejects empty partition ids before registering subscriptions", async () => {
    const store = new InMemoryPartitionStore("node-a");
    const node = new SyncNode("node-a", store, new MemoryTransport());

    await node.start();
    await expect(node.subscribe("")).rejects.toThrow("partitionId must be a non-empty string");
    await expect(store.hasPartition("")).rejects.toThrow("partitionId must be a non-empty string");
  });

  it("registers the local partition before exposing a transport subscription", async () => {
    const event: EventEnvelope = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "during subscribe" },
    };
    const store = new InMemoryPartitionStore("node-b");
    const node = new SyncNode("node-b", store, new SubscribeEventTransport(event));

    await node.start();
    await node.subscribe("tenant:jp");

    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toEqual([event]);
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

  it("rejects empty partition ids on direct store access", async () => {
    const store = new InMemoryPartitionStore("node-a");

    await expect(store.registerPartition("")).rejects.toThrow("partitionId must be a non-empty string");
    await expect(store.listEventsAfterCursors("", new Map()))
      .rejects.toThrow("partitionId must be a non-empty string");
    await expect(store.getHighWatermark(""))
      .rejects.toThrow("partitionId must be a non-empty string");
  });

  it("rejects invalid replay cursors on direct store access", async () => {
    const store = new InMemoryPartitionStore("node-a");

    await expect(store.listEventsAfter("tenant:jp", {
      sourceNodeId: "node-b",
      eventId: "not-a-uuid",
    })).rejects.toThrow("cursor eventId must be a UUIDv7");
    await expect(store.listEventsAfterCursors(
      "tenant:jp",
      new Map([["node-b", "not-a-uuid"]]),
    )).rejects.toThrow("cursor eventId must be a UUIDv7");
  });

  it("rejects conflicting duplicate events from the same source node and event id", async () => {
    const store = new InMemoryPartitionStore("node-b");
    await store.registerPartition("tenant:jp");
    await store.registerPartition("tenant:us");
    const event = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "once" },
    };

    await expect(store.applyRemote(event)).resolves.toBe(true);
    await expect(store.applyRemote({
      ...event,
      partitionId: "tenant:us",
      payload: { title: "conflict" },
    })).rejects.toThrow("event id conflict");
  });

  it("rejects conflicting duplicate event ids even when the conflicting partition is not registered", async () => {
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
    await expect(store.applyRemote({
      ...event,
      partitionId: "tenant:missing",
      payload: { title: "conflict" },
    })).rejects.toThrow("event id conflict");
  });

  it("treats duplicate JSON payloads with different object key order as the same event", async () => {
    const store = new InMemoryPartitionStore("node-b");
    await store.registerPartition("tenant:jp");
    const event = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
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
    const store = new InMemoryPartitionStore("node-a");

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
    const store = new InMemoryPartitionStore("node-b");
    await store.registerPartition("tenant:jp");

    await expect(store.applyRemote({
      eventId: "not-a-uuid",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { ok: true },
    })).rejects.toThrow("eventId must be a UUIDv7");
    await expect(store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 0,
      payload: { ok: true },
    })).rejects.toThrow("schemaVersion must be a positive integer");
    await expect(store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: Infinity },
    })).rejects.toThrow("payload.value must be JSON-serializable");
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
  });

  it("rejects malformed remote events before checking partition registration", async () => {
    const store = new InMemoryPartitionStore("node-b");

    await expect(store.applyRemote({
      eventId: "not-a-uuid",
      sourceNodeId: "node-a",
      partitionId: "tenant:missing",
      schemaVersion: 1,
      payload: { ok: true },
    })).rejects.toThrow("eventId must be a UUIDv7");
    await expect(store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:missing",
      schemaVersion: 0,
      payload: { ok: true },
    })).rejects.toThrow("schemaVersion must be a positive integer");
    await expect(store.listEventsAfterCursors("tenant:missing", new Map())).resolves.toHaveLength(0);
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

  it("keeps high-watermarks partition-scoped and source-scoped when events arrive out of order", async () => {
    const store = new InMemoryPartitionStore("node-c");
    await store.registerPartition("tenant:jp");
    await store.registerPartition("tenant:us");

    await store.applyRemote({
      eventId: "018bcfe5-6800-7003-8000-000000000003",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a3" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1-late" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b2" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7009-8000-000000000009",
      sourceNodeId: "node-a",
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { value: "us-a9" },
    });

    await expect(store.getHighWatermark("tenant:jp")).resolves.toEqual(
      new Map([
        ["node-a", "018bcfe5-6800-7003-8000-000000000003"],
        ["node-b", "018bcfe5-6800-7002-8000-000000000002"],
      ]),
    );
    await expect(store.getHighWatermark("tenant:us")).resolves.toEqual(
      new Map([["node-a", "018bcfe5-6800-7009-8000-000000000009"]]),
    );
  });

  it("catch-up sends the receiver high-watermark so peers only replay missing source streams", async () => {
    const transport = new MemoryTransport();
    const sourceStore = new InMemoryPartitionStore("source");
    const targetStore = new InMemoryPartitionStore("target");
    await sourceStore.registerPartition("tenant:jp");
    await targetStore.registerPartition("tenant:jp");

    await sourceStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });
    await sourceStore.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a2" },
    });
    await sourceStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });

    await targetStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });
    await targetStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });

    const source = new SyncNode("source", sourceStore, transport);
    const target = new SyncNode("target", targetStore, transport);
    await source.start();
    await target.start();

    await expect(target.catchUpFrom("source", "tenant:jp")).resolves.toBe(1);
    await expect(targetStore.listEventsAfterCursors("tenant:jp", new Map())).resolves.toMatchObject([
      { sourceNodeId: "node-a", payload: { value: "a1" } },
      { sourceNodeId: "node-a", payload: { value: "a2" } },
      { sourceNodeId: "node-b", payload: { value: "b1" } },
    ]);
  });

  it("rejects replay responses that do not match the requested partition", async () => {
    const store = new InMemoryPartitionStore("target");
    const node = new SyncNode(
      "target",
      store,
      new ReplayResponseTransport({
        partitionId: "tenant:us",
        events: [
          {
            eventId: "018bcfe5-6800-7001-8000-000000000001",
            sourceNodeId: "source",
            partitionId: "tenant:us",
            schemaVersion: 1,
            payload: { leaked: true },
          },
        ],
      }),
    );

    await node.start();
    await node.subscribe("tenant:jp");
    await node.subscribe("tenant:us");

    await expect(node.catchUpFrom("source", "tenant:jp")).rejects.toThrow(
      "replay response partition mismatch",
    );
    await expect(store.listEventsAfterCursors("tenant:us", new Map())).resolves.toHaveLength(0);
  });

  it("rejects replay events that do not match the requested partition", async () => {
    const store = new InMemoryPartitionStore("target");
    const node = new SyncNode(
      "target",
      store,
      new ReplayResponseTransport({
        partitionId: "tenant:jp",
        events: [
          {
            eventId: "018bcfe5-6800-7001-8000-000000000001",
            sourceNodeId: "source",
            partitionId: "tenant:us",
            schemaVersion: 1,
            payload: { leaked: true },
          },
        ],
      }),
    );

    await node.start();
    await node.subscribe("tenant:jp");
    await node.subscribe("tenant:us");

    await expect(node.catchUpFrom("source", "tenant:jp")).rejects.toThrow(
      "replay event partition mismatch",
    );
    await expect(store.listEventsAfterCursors("tenant:us", new Map())).resolves.toHaveLength(0);
  });

  it("rejects conflicting duplicate events inside a replay response before applying any of them", async () => {
    const store = new InMemoryPartitionStore("target");
    const node = new SyncNode(
      "target",
      store,
      new ReplayResponseTransport({
        partitionId: "tenant:jp",
        events: [
          {
            eventId: "018bcfe5-6800-7001-8000-000000000001",
            sourceNodeId: "source",
            partitionId: "tenant:jp",
            schemaVersion: 1,
            payload: { value: "first" },
          },
          {
            eventId: "018bcfe5-6800-7001-8000-000000000001",
            sourceNodeId: "source",
            partitionId: "tenant:jp",
            schemaVersion: 1,
            payload: { value: "conflict" },
          },
        ],
      }),
    );

    await node.start();
    await node.subscribe("tenant:jp");

    await expect(node.catchUpFrom("source", "tenant:jp")).rejects.toThrow(
      "replay response event id conflict",
    );
    await expect(store.listEventsAfterCursors("tenant:jp", new Map())).resolves.toHaveLength(0);
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

  it("rejects empty partition ids before replay requests", async () => {
    const node = new SyncNode("node-a", new InMemoryPartitionStore("node-a"), new MemoryTransport());

    await node.start();

    await expect(node.catchUpFrom("node-b", "")).rejects.toThrow("partitionId must be a non-empty string");
    await expect(node.reconcileFrom("node-b", "")).rejects.toThrow("partitionId must be a non-empty string");
    await expect(node.highWatermark("")).rejects.toThrow("partitionId must be a non-empty string");
  });

  it("rejects empty peer node ids before replay requests", async () => {
    const node = new SyncNode("node-a", new InMemoryPartitionStore("node-a"), new MemoryTransport());

    await node.start();

    await expect(node.catchUpFrom("", "tenant:jp")).rejects.toThrow("nodeId must be a non-empty string");
    await expect(node.reconcileFrom("", "tenant:jp")).rejects.toThrow("nodeId must be a non-empty string");
  });

  it("can reconcile a partition to recover older events hidden behind a newer high-watermark", async () => {
    const transport = new MemoryTransport();
    const sourceStore = new InMemoryPartitionStore("source");
    const targetStore = new InMemoryPartitionStore("target");
    await sourceStore.registerPartition("tenant:jp");
    await targetStore.registerPartition("tenant:jp");

    const older = {
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "older" },
    };
    const newer = {
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "newer" },
    };
    await sourceStore.applyRemote(older);
    await sourceStore.applyRemote(newer);
    await targetStore.applyRemote(newer);

    const source = new SyncNode("source", sourceStore, transport);
    const target = new SyncNode("target", targetStore, transport);
    await source.start();
    await target.start();

    await expect(target.catchUpFrom("source", "tenant:jp")).resolves.toBe(0);
    await expect(target.reconcileFrom("source", "tenant:jp")).resolves.toBe(1);
    await expect(targetStore.listEventsAfterCursors("tenant:jp", new Map())).resolves.toMatchObject([
      { payload: { value: "older" } },
      { payload: { value: "newer" } },
    ]);
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

  it("reports convergence gaps and plans catch-up from remote cursors", async () => {
    const local = new Map([
      ["node-a", "018bcfe5-6800-7001-8000-000000000001"],
      ["node-c", "018bcfe5-6800-7009-8000-000000000009"],
    ]);
    const remote = new Map([
      ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
      ["node-b", "018bcfe5-6800-7001-8000-000000000003"],
      ["node-c", "018bcfe5-6800-7009-8000-000000000009"],
    ]);

    expect(compareHighWatermarks(local, remote)).toEqual([
      {
        sourceNodeId: "node-a",
        localEventId: "018bcfe5-6800-7001-8000-000000000001",
        remoteEventId: "018bcfe5-6800-7002-8000-000000000002",
        comparison: "local-behind",
      },
      {
        sourceNodeId: "node-b",
        localEventId: undefined,
        remoteEventId: "018bcfe5-6800-7001-8000-000000000003",
        comparison: "local-behind",
      },
      {
        sourceNodeId: "node-c",
        localEventId: "018bcfe5-6800-7009-8000-000000000009",
        remoteEventId: "018bcfe5-6800-7009-8000-000000000009",
        comparison: "equal",
      },
    ]);

    const node = new SyncNode("node-local", new InMemoryPartitionStore("node-local"), new MemoryTransport());
    const status = await node.convergenceStatus("tenant:jp", remote);
    const plan = await node.catchupPlan("tenant:jp", remote);

    expect(status.missingSources).toEqual(["node-a", "node-b", "node-c"]);
    expect(plan).toMatchObject({
      partitionId: "tenant:jp",
      shouldRequestReplay: true,
      missingSources: ["node-a", "node-b", "node-c"],
    });
  });

  it("converges under duplicated, out-of-order, and lost event delivery after catch-up", async () => {
    const stores = new Map([
      ["node-a", new InMemoryPartitionStore("node-a")],
      ["node-b", new InMemoryPartitionStore("node-b")],
      ["node-c", new InMemoryPartitionStore("node-c")],
    ]);
    for (const store of stores.values()) {
      await store.registerPartition("tenant:jp");
    }

    const produced = [
      await stores.get("node-a")!.appendLocal({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 1 } }),
      await stores.get("node-b")!.appendLocal({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 2 } }),
      await stores.get("node-c")!.appendLocal({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 3 } }),
      await stores.get("node-a")!.appendLocal({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 4 } }),
    ];

    const deliveries = [
      [stores.get("node-b")!, produced[3]],
      [stores.get("node-c")!, produced[0]],
      [stores.get("node-b")!, produced[0]],
      [stores.get("node-b")!, produced[0]],
      [stores.get("node-a")!, produced[1]],
      [stores.get("node-a")!, produced[2]],
    ] as const;
    for (const [store, event] of deliveries) {
      await store.applyRemote(event);
    }

    for (const target of stores.values()) {
      const cursors = await target.getHighWatermark("tenant:jp");
      for (const source of stores.values()) {
        const replay = await source.listEventsAfterCursors("tenant:jp", cursors);
        for (const event of replay) {
          await target.applyRemote(event);
        }
      }
    }

    const eventSets = await Promise.all([...stores.values()].map(async (store) =>
      (await store.listEventsAfterCursors("tenant:jp", new Map()))
        .map((event) => `${event.sourceNodeId}/${event.eventId}`)
        .sort()
    ));
    expect(eventSets[1]).toEqual(eventSets[0]);
    expect(eventSets[2]).toEqual(eventSets[0]);
    expect(eventSets[0]).toHaveLength(4);
  });
});

describe("MemoryTransport", () => {
  it("rejects empty node ids before connecting", async () => {
    await expect(new MemoryTransport().connect("", { onEvent: async () => {} }))
      .rejects.toThrow("nodeId must be a non-empty string");
  });

  it("keeps the latest duplicate node connection when an older connection closes", async () => {
    const transport = new MemoryTransport();
    const first = await transport.connect("node-a", {
      onEvent: async () => {},
    });
    const second = await transport.connect("node-a", {
      onEvent: async () => {},
      onReplayRequest: async (request) => ({
        partitionId: request.partitionId,
        events: [{
          eventId: "018bcfe5-6800-7001-8000-000000000001",
          sourceNodeId: "node-a",
          partitionId: request.partitionId,
          schemaVersion: 1,
          payload: { from: "latest" },
        }],
      }),
    });
    const requester = await transport.connect("requester", {
      onEvent: async () => {},
    });

    await first.close();

    await expect(requester.requestReplay("node-a", "tenant:jp", new Map()))
      .resolves.toMatchObject({
        partitionId: "tenant:jp",
        events: [{ payload: { from: "latest" } }],
      });
    await expect(second.requestReplay("missing", "tenant:jp", new Map()))
      .resolves.toEqual({ partitionId: "tenant:jp", events: [] });
  });

  it("rejects operations attempted through closed memory connections", async () => {
    const transport = new MemoryTransport();
    const connection = await transport.connect("node-a", {
      onEvent: async () => {},
    });

    await connection.close();

    await expect(connection.subscribe("tenant:jp")).rejects.toThrow("connection for node-a is closed");
    await expect(connection.publish({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { closed: true },
    })).rejects.toThrow("connection for node-a is closed");
    await expect(connection.requestReplay("node-b", "tenant:jp", new Map()))
      .rejects.toThrow("connection for node-a is closed");
  });

  it("rejects invalid events before publishing through memory transport", async () => {
    const connection = await new MemoryTransport().connect("node-a", {
      onEvent: async () => {},
    });

    await expect(connection.publish({
      eventId: "not-a-uuid",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: {},
    })).rejects.toThrow("eventId must be a UUIDv7");

    await connection.close();
  });

  it("rejects events whose source does not match the memory connection", async () => {
    const connection = await new MemoryTransport().connect("node-a", {
      onEvent: async () => {},
    });

    await expect(connection.publish({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: {},
    })).rejects.toThrow("event source mismatch");

    await connection.close();
  });

  it("rejects invalid cursors before requesting memory replay", async () => {
    const connection = await new MemoryTransport().connect("node-a", {
      onEvent: async () => {},
    });

    await expect(connection.requestReplay(
      "node-b",
      "tenant:jp",
      new Map([["node-a", "not-a-uuid"]]),
    )).rejects.toThrow("cursor eventId must be a UUIDv7");

    await connection.close();
  });

  it("rejects memory replay responses for a different partition", async () => {
    const transport = new MemoryTransport();
    await transport.connect("target", {
      onEvent: async () => {},
      onReplayRequest: async () => ({
        partitionId: "tenant:us",
        events: [],
      }),
    });
    const requester = await transport.connect("requester", {
      onEvent: async () => {},
    });

    await expect(requester.requestReplay("target", "tenant:jp", new Map()))
      .rejects.toThrow("replay response partition mismatch");

    await requester.close();
  });

  it("rejects an older duplicate connection after it has been replaced", async () => {
    const transport = new MemoryTransport();
    const first = await transport.connect("node-a", {
      onEvent: async () => {},
    });
    await transport.connect("node-a", {
      onEvent: async () => {},
    });

    await expect(first.subscribe("tenant:jp")).rejects.toThrow("connection for node-a is closed");
    await expect(first.requestReplay("node-b", "tenant:jp", new Map()))
      .rejects.toThrow("connection for node-a is closed");
  });
});

class ReplayResponseTransport implements Transport {
  constructor(private readonly response: ReplayResponse) {}

  async connect(
    _nodeId: string,
    _handlers: TransportHandlers,
  ): Promise<TransportConnection> {
    return {
      subscribe: async () => {},
      publish: async () => {},
      requestReplay: async () => this.response,
      close: async () => {},
    };
  }
}

class SubscribeEventTransport implements Transport {
  constructor(private readonly event: EventEnvelope) {}

  async connect(
    _nodeId: string,
    handlers: TransportHandlers,
  ): Promise<TransportConnection> {
    return {
      subscribe: async () => {
        await handlers.onEvent(this.event);
      },
      publish: async () => {},
      requestReplay: async () => ({ partitionId: this.event.partitionId, events: [] }),
      close: async () => {},
    };
  }
}

class TransactionalMemoryStore extends InMemoryPartitionStore {
  async appendLocalEventTransaction<TPayload>(
    input: LocalEventInput<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<EventEnvelope<TPayload>> {
    const event = await this.appendLocal(input);
    await project?.(event);
    return event;
  }

  async applyRemoteEventTransaction<TPayload>(
    event: EventEnvelope<TPayload>,
    project?: EventProjection<TPayload>,
  ): Promise<boolean> {
    const applied = await this.applyRemote(event);
    if (applied) {
      await project?.(event);
    }
    return applied;
  }
}
