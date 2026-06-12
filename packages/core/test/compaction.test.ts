import { describe, expect, it } from "vitest";
import {
  assertPruneAnchored,
  hasReplayGap,
  InMemoryPartitionStore,
  MemoryTransport,
  selectPruneAnchor,
  SnapshotRequiredError,
  SyncNode,
  UuidV7Factory,
} from "../src/index.js";
import type { HighWatermark, SyncTelemetryEvent, SyncTelemetrySink } from "../src/index.js";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeSink(): { events: SyncTelemetryEvent[]; sink: SyncTelemetrySink } {
  const events: SyncTelemetryEvent[] = [];
  return { events, sink: (e) => events.push(e) };
}

// Build a HighWatermark (Map<NodeId, EventId>) from a plain object.
function hwm(entries: Record<string, string>): HighWatermark {
  return new Map(Object.entries(entries));
}

// Generate a sequence of UUIDv7 IDs at increasing timestamps.
function makeIds(count: number): string[] {
  let ts = 1_700_000_000_000;
  const ids = new UuidV7Factory(
    () => ts,
    (length) => new Uint8Array(length).fill(0x01),
  );
  return Array.from({ length: count }, () => {
    const id = ids.next();
    ts += 1;
    return id;
  });
}

// ---------------------------------------------------------------------------
// 1. hasReplayGap
// ---------------------------------------------------------------------------

describe("hasReplayGap", () => {
  it("returns false when prunedThrough is empty (no floor set)", () => {
    const cursors = hwm({ "node-a": "018bcfe5-6800-7001-8000-000000000001" });
    expect(hasReplayGap(cursors, new Map())).toBe(false);
  });

  it("returns true when the requester cursor is missing for a pruned source", () => {
    // node-a was pruned; the requester has no cursor for it (full-history request)
    const floor = hwm({ "node-a": "018bcfe5-6800-7001-8000-000000000001" });
    const cursors = new Map<string, string>(); // no cursor for node-a
    expect(hasReplayGap(cursors, floor)).toBe(true);
  });

  it("returns true when the requester cursor is strictly below the pruned floor", () => {
    const [id1, id2] = makeIds(2);
    const floor = hwm({ "node-a": id2! });
    const cursors = hwm({ "node-a": id1! }); // behind floor
    expect(hasReplayGap(cursors, floor)).toBe(true);
  });

  it("returns false when the requester cursor equals the pruned floor", () => {
    const [id1] = makeIds(1);
    const floor = hwm({ "node-a": id1! });
    const cursors = hwm({ "node-a": id1! }); // at floor
    expect(hasReplayGap(cursors, floor)).toBe(false);
  });

  it("returns false when the requester cursor is strictly above the pruned floor", () => {
    const [id1, id2] = makeIds(2);
    const floor = hwm({ "node-a": id1! });
    const cursors = hwm({ "node-a": id2! }); // ahead of floor
    expect(hasReplayGap(cursors, floor)).toBe(false);
  });

  it("returns true when only one of multiple pruned sources is missing from cursors", () => {
    const [id1] = makeIds(1);
    const floor = hwm({ "node-a": id1!, "node-b": id1! });
    const cursors = hwm({ "node-a": id1! }); // node-b missing
    expect(hasReplayGap(cursors, floor)).toBe(true);
  });

  it("returns false when all pruned sources have cursors at or above the floor", () => {
    const [id1, id2] = makeIds(2);
    const floor = hwm({ "node-a": id1!, "node-b": id1! });
    const cursors = hwm({ "node-a": id2!, "node-b": id1! });
    expect(hasReplayGap(cursors, floor)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 2. selectPruneAnchor / assertPruneAnchored
// ---------------------------------------------------------------------------

describe("selectPruneAnchor", () => {
  it("returns index 0 when the single anchor fully covers the boundary", () => {
    const [id1] = makeIds(1);
    const through = hwm({ "node-a": id1! });
    const anchors: HighWatermark[] = [hwm({ "node-a": id1! })];
    expect(selectPruneAnchor(through, anchors)).toBe(0);
  });

  it("returns the first anchor index when multiple anchors exist and the first covers", () => {
    const [id1, id2] = makeIds(2);
    const through = hwm({ "node-a": id1! });
    const anchors: HighWatermark[] = [
      hwm({ "node-a": id2! }), // id2 > id1, so this covers
      hwm({ "node-a": id1! }),
    ];
    expect(selectPruneAnchor(through, anchors)).toBe(0);
  });

  it("returns index 1 when only the second anchor covers the boundary", () => {
    const [id1, id2] = makeIds(2);
    const through = hwm({ "node-a": id2! });
    const anchors: HighWatermark[] = [
      hwm({ "node-a": id1! }), // id1 < id2 → does not cover
      hwm({ "node-a": id2! }),
    ];
    expect(selectPruneAnchor(through, anchors)).toBe(1);
  });

  it("returns undefined when no single anchor covers the boundary by itself", () => {
    // node-a covered by anchor 0; node-b covered by anchor 1; neither covers both.
    const [id1] = makeIds(1);
    const through = hwm({ "node-a": id1!, "node-b": id1! });
    const anchors: HighWatermark[] = [
      hwm({ "node-a": id1! }), // missing node-b
      hwm({ "node-b": id1! }), // missing node-a
    ];
    expect(selectPruneAnchor(through, anchors)).toBeUndefined();
  });

  it("returns undefined when the anchor list is empty", () => {
    const [id1] = makeIds(1);
    const through = hwm({ "node-a": id1! });
    expect(selectPruneAnchor(through, [])).toBeUndefined();
  });

  it("returns undefined when an anchor cursor for a required source is strictly below the boundary", () => {
    const [id1, id2] = makeIds(2);
    const through = hwm({ "node-a": id2! });
    const anchors: HighWatermark[] = [hwm({ "node-a": id1! })]; // anchor is behind
    expect(selectPruneAnchor(through, anchors)).toBeUndefined();
  });
});

describe("assertPruneAnchored", () => {
  it("returns the anchor index when the boundary is covered", () => {
    const [id1] = makeIds(1);
    const through = hwm({ "node-a": id1! });
    const anchors: HighWatermark[] = [hwm({ "node-a": id1! })];
    expect(assertPruneAnchored("tenant:jp", through, anchors)).toBe(0);
  });

  it("throws when no anchor covers the boundary (ADR 0008)", () => {
    const [id1] = makeIds(1);
    const through = hwm({ "node-a": id1! });
    expect(() => assertPruneAnchored("tenant:jp", through, [])).toThrow(
      /prune boundary.*not covered/,
    );
  });

  it("includes the partition id in the error message", () => {
    const [id1] = makeIds(1);
    expect(() => assertPruneAnchored("tenant:special", hwm({ "node-a": id1! }), [])).toThrow(
      /tenant:special/,
    );
  });
});

// ---------------------------------------------------------------------------
// 3. InMemoryPartitionStore: pruneEvents / getPrunedThrough / getHighWatermark
// ---------------------------------------------------------------------------

describe("InMemoryPartitionStore compaction", () => {
  async function buildPrunedStore() {
    const [id1, id2, id3] = makeIds(3);
    const store = new InMemoryPartitionStore("node-a");
    await store.registerPartition("tenant:jp");

    const ev1 = {
      eventId: id1!,
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 1 },
    };
    const ev2 = {
      eventId: id2!,
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 2 },
    };
    const ev3 = {
      eventId: id3!,
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 3 },
    };

    // Apply ev1 and ev2 as remote events from node-b's perspective; ev3 is local.
    await store.applyRemote(ev1);
    await store.applyRemote(ev2);
    await store.applyRemote(ev3);

    return { store, ids: [id1!, id2!, id3!], events: [ev1, ev2, ev3] };
  }

  it("pruneEvents deletes events at or below the cursor and returns their count", async () => {
    const { store, ids } = await buildPrunedStore();
    const pruned = await store.pruneEvents("tenant:jp", hwm({ "node-a": ids[1]! }));
    expect(pruned).toBe(2); // id1 and id2 deleted
  });

  it("listEventsAfterCursors no longer returns pruned events", async () => {
    const { store, ids, events } = await buildPrunedStore();
    await store.pruneEvents("tenant:jp", hwm({ "node-a": ids[1]! }));

    const remaining = await store.listEventsAfterCursors("tenant:jp", new Map());
    expect(remaining).toHaveLength(1);
    expect(remaining[0]?.eventId).toBe(events[2]!.eventId);
  });

  it("getPrunedThrough reflects the pruned floor after pruning", async () => {
    const { store, ids } = await buildPrunedStore();
    await store.pruneEvents("tenant:jp", hwm({ "node-a": ids[1]! }));

    const floor = await store.getPrunedThrough("tenant:jp");
    expect(floor.get("node-a")).toBe(ids[1]);
  });

  it("getPrunedThrough returns an empty map for a partition that has never been pruned", async () => {
    const store = new InMemoryPartitionStore("node-a");
    await store.registerPartition("tenant:jp");
    const floor = await store.getPrunedThrough("tenant:jp");
    expect(floor.size).toBe(0);
  });

  it("floor is monotone: pruning with an older cursor does not regress it", async () => {
    const { store, ids } = await buildPrunedStore();

    // Prune up to id2 first.
    await store.pruneEvents("tenant:jp", hwm({ "node-a": ids[1]! }));
    // Attempt to prune with id1 (older) — should not regress.
    await store.pruneEvents("tenant:jp", hwm({ "node-a": ids[0]! }));

    const floor = await store.getPrunedThrough("tenant:jp");
    expect(floor.get("node-a")).toBe(ids[1]); // still id2, not id1
  });

  it("getHighWatermark never drops below the pruned floor after pruning everything", async () => {
    const { store, ids } = await buildPrunedStore();

    // Prune ALL events (up to id3).
    await store.pruneEvents("tenant:jp", hwm({ "node-a": ids[2]! }));

    // No events remain in the log, but the watermark must be seeded by the floor.
    const watermark = await store.getHighWatermark("tenant:jp");
    expect(watermark.get("node-a")).toBe(ids[2]);
  });

  it("applyRemote returns false for an event at or below the pruned floor (idempotence floor)", async () => {
    const { store, ids, events } = await buildPrunedStore();
    await store.pruneEvents("tenant:jp", hwm({ "node-a": ids[1]! }));

    // Attempt to re-apply pruned events.
    expect(await store.applyRemote(events[0]!)).toBe(false);
    expect(await store.applyRemote(events[1]!)).toBe(false);
  });

  it("applyRemote still applies an event above the pruned floor", async () => {
    // Fresh store: prune id1, then try to apply id2 (above floor) and id3 (new).
    const [id1, id2, id3] = makeIds(3);
    const store = new InMemoryPartitionStore("node-x");
    await store.registerPartition("tenant:jp");

    const ev1 = {
      eventId: id1!,
      sourceNodeId: "node-x",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 1 },
    };
    const ev2 = {
      eventId: id2!,
      sourceNodeId: "node-x",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 2 },
    };
    const ev3 = {
      eventId: id3!,
      sourceNodeId: "node-x",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 3 },
    };

    await store.applyRemote(ev1);
    await store.applyRemote(ev2);

    // Prune up to id1 only.
    await store.pruneEvents("tenant:jp", hwm({ "node-x": id1! }));

    // ev2 (above floor) was already applied before pruning — should be duplicate (false).
    expect(await store.applyRemote(ev2)).toBe(false);

    // ev3 (above floor, not yet stored) should be applied.
    expect(await store.applyRemote(ev3)).toBe(true);
  });

  it("pruneEvents only affects the specified partition", async () => {
    // Distinct event ids: (source_node_id, event_id) is the global event
    // identity, so one source never reuses an id across partitions.
    const [id1, id2] = makeIds(2);
    const store = new InMemoryPartitionStore("node-a");
    await store.registerPartition("tenant:jp");
    await store.registerPartition("tenant:us");

    const ev = {
      eventId: id1!,
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { x: 1 },
    };
    const evUs = {
      eventId: id2!,
      sourceNodeId: "node-a",
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { x: 2 },
    };
    await store.applyRemote(ev);
    await store.applyRemote(evUs);

    await store.pruneEvents("tenant:jp", hwm({ "node-a": id1! }));

    // tenant:jp is pruned
    const jpFloor = await store.getPrunedThrough("tenant:jp");
    expect(jpFloor.size).toBe(1);

    // tenant:us is untouched
    const usFloor = await store.getPrunedThrough("tenant:us");
    expect(usFloor.size).toBe(0);

    // tenant:us event still retrievable
    const usEvents = await store.listEventsAfterCursors("tenant:us", new Map());
    expect(usEvents).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// 4. SyncNode over MemoryTransport: snapshot-required on gap
// ---------------------------------------------------------------------------

describe("SyncNode compaction: snapshot-required on replay gap", () => {
  it("catchUpFrom rejects with SnapshotRequiredError when server has pruned past the requester's cursor", async () => {
    const transport = new MemoryTransport();

    // Node A is the server with a pruned store.
    const aTelemetry = makeSink();
    const aStore = new InMemoryPartitionStore("node-a");
    await aStore.registerPartition("tenant:jp");

    // Write three events then prune the first two.
    const aIds = new UuidV7Factory();
    const ev1 = {
      eventId: aIds.next(),
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 1 },
    };
    const ev2 = {
      eventId: aIds.next(),
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 2 },
    };
    const ev3 = {
      eventId: aIds.next(),
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 3 },
    };
    await aStore.applyRemote(ev1);
    await aStore.applyRemote(ev2);
    await aStore.applyRemote(ev3);
    await aStore.pruneEvents("tenant:jp", hwm({ "node-a": ev2.eventId }));

    const nodeA = new SyncNode("node-a", aStore, transport, { telemetry: aTelemetry.sink });
    await nodeA.start();
    await nodeA.subscribe("tenant:jp");

    // Node B is a fresh requester with an empty store (cursors will be empty → gap).
    const bTelemetry = makeSink();
    const bStore = new InMemoryPartitionStore("node-b");
    const nodeB = new SyncNode("node-b", bStore, transport, { telemetry: bTelemetry.sink });
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    // catchUpFrom must throw SnapshotRequiredError
    await expect(nodeB.catchUpFrom("node-a", "tenant:jp")).rejects.toThrow(SnapshotRequiredError);

    // Verify error carries prunedThrough
    try {
      await nodeB.catchUpFrom("node-a", "tenant:jp");
    } catch (error) {
      expect(error).toBeInstanceOf(SnapshotRequiredError);
      const sre = error as SnapshotRequiredError;
      expect(sre.partitionId).toBe("tenant:jp");
      expect(sre.peerNodeId).toBe("node-a");
      expect(sre.prunedThrough.get("node-a")).toBe(ev2.eventId);
    }

    // A emitted snapshot-required direction:"served"
    const aSnapRequired = aTelemetry.events.filter((e) => e.type === "snapshot-required");
    expect(aSnapRequired.length).toBeGreaterThan(0);
    expect(aSnapRequired[0]).toMatchObject({
      type: "snapshot-required",
      nodeId: "node-a",
      partitionId: "tenant:jp",
      peerNodeId: "node-b",
      direction: "served",
    });

    // B emitted snapshot-required direction:"received"
    const bSnapRequired = bTelemetry.events.filter((e) => e.type === "snapshot-required");
    expect(bSnapRequired.length).toBeGreaterThan(0);
    expect(bSnapRequired[0]).toMatchObject({
      type: "snapshot-required",
      nodeId: "node-b",
      partitionId: "tenant:jp",
      peerNodeId: "node-a",
      direction: "received",
    });

    await nodeA.stop();
    await nodeB.stop();
  });

  it("catchUpFrom succeeds when the requester's cursors are exactly at the pruned floor", async () => {
    const transport = new MemoryTransport();
    const aIds = new UuidV7Factory();

    // Node A: write two events, prune the first.
    const aStore = new InMemoryPartitionStore("node-a");
    await aStore.registerPartition("tenant:jp");

    const ev1 = {
      eventId: aIds.next(),
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 1 },
    };
    const ev2 = {
      eventId: aIds.next(),
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 2 },
    };
    await aStore.applyRemote(ev1);
    await aStore.applyRemote(ev2);
    await aStore.pruneEvents("tenant:jp", hwm({ "node-a": ev1.eventId }));

    const nodeA = new SyncNode("node-a", aStore, transport);
    await nodeA.start();
    await nodeA.subscribe("tenant:jp");

    // Node B already has ev1 (cursor = ev1.eventId, exactly at the floor).
    const bStore = new InMemoryPartitionStore("node-b");
    await bStore.registerPartition("tenant:jp");
    await bStore.applyRemote(ev1);

    const nodeB = new SyncNode("node-b", bStore, transport);
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    // Cursor for node-a = ev1 = floor → no gap → catchup should succeed.
    const applied = await nodeB.catchUpFrom("node-a", "tenant:jp");
    expect(applied).toBe(1); // only ev2 was missing

    const events = await bStore.listEventsAfterCursors("tenant:jp", new Map());
    // ev1 is pruned on A so it's no longer served; B still has it from before.
    const payloads = events.map((e) => (e.payload as { n: number }).n).sort();
    expect(payloads).toContain(2);

    await nodeA.stop();
    await nodeB.stop();
  });
});

// ---------------------------------------------------------------------------
// 5. reconcileFrom against a pruned peer throws SnapshotRequiredError
// ---------------------------------------------------------------------------

describe("SyncNode compaction: reconcileFrom against pruned peer", () => {
  it("throws SnapshotRequiredError when the server has any pruned floor (cursors start empty)", async () => {
    const transport = new MemoryTransport();
    const aIds = new UuidV7Factory();

    const aStore = new InMemoryPartitionStore("node-a");
    await aStore.registerPartition("tenant:jp");

    const ev1 = {
      eventId: aIds.next(),
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 1 },
    };
    await aStore.applyRemote(ev1);
    // Prune ev1 — now the floor for node-a is set.
    await aStore.pruneEvents("tenant:jp", hwm({ "node-a": ev1.eventId }));

    const nodeA = new SyncNode("node-a", aStore, transport);
    await nodeA.start();
    await nodeA.subscribe("tenant:jp");

    const bTelemetry = makeSink();
    const bStore = new InMemoryPartitionStore("node-b");
    const nodeB = new SyncNode("node-b", bStore, transport, { telemetry: bTelemetry.sink });
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    // reconcileFrom sends empty cursors → always a gap when floor is non-empty.
    await expect(nodeB.reconcileFrom("node-a", "tenant:jp")).rejects.toThrow(SnapshotRequiredError);

    // B must have received snapshot-required telemetry with direction "received".
    const snapEvents = bTelemetry.events.filter((e) => e.type === "snapshot-required");
    expect(snapEvents.length).toBeGreaterThan(0);
    expect(snapEvents[0]).toMatchObject({
      type: "snapshot-required",
      direction: "received",
      peerNodeId: "node-a",
    });

    await nodeA.stop();
    await nodeB.stop();
  });

  it("carries the correct prunedThrough map in the thrown SnapshotRequiredError", async () => {
    const transport = new MemoryTransport();
    const aIds = new UuidV7Factory();

    const aStore = new InMemoryPartitionStore("node-a");
    await aStore.registerPartition("tenant:jp");

    const ev1 = {
      eventId: aIds.next(),
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { n: 1 },
    };
    await aStore.applyRemote(ev1);
    await aStore.pruneEvents("tenant:jp", hwm({ "node-a": ev1.eventId }));

    const nodeA = new SyncNode("node-a", aStore, transport);
    await nodeA.start();
    await nodeA.subscribe("tenant:jp");

    const bStore = new InMemoryPartitionStore("node-b");
    const nodeB = new SyncNode("node-b", bStore, transport);
    await nodeB.start();
    await nodeB.subscribe("tenant:jp");

    let caught: unknown;
    try {
      await nodeB.reconcileFrom("node-a", "tenant:jp");
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(SnapshotRequiredError);
    const sre = caught as SnapshotRequiredError;
    expect(sre.name).toBe("SnapshotRequiredError");
    expect(sre.partitionId).toBe("tenant:jp");
    expect(sre.peerNodeId).toBe("node-a");
    expect(sre.prunedThrough.size).toBe(1);
    expect(sre.prunedThrough.get("node-a")).toBe(ev1.eventId);

    await nodeA.stop();
    await nodeB.stop();
  });
});
