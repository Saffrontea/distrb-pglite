import { describe, expect, it } from "vitest";
import {
  emitTelemetry,
  InMemoryPartitionStore,
  MemoryTransport,
  SingleWriterRegistry,
  SyncNode,
} from "../src/index.js";
import type { SyncTelemetryEvent, SyncTelemetrySink } from "../src/index.js";

// Collect all emitted events into an array for assertion.
function makeSink(): { events: SyncTelemetryEvent[]; sink: SyncTelemetrySink } {
  const events: SyncTelemetryEvent[] = [];
  return { events, sink: (e) => events.push(e) };
}

describe("emitTelemetry", () => {
  it("does nothing when the sink is undefined", () => {
    // Should not throw and should have no observable side effects.
    expect(() =>
      emitTelemetry(undefined, { type: "sync-started", nodeId: "node-a" }),
    ).not.toThrow();
  });

  it("calls the sink with the event when one is provided", () => {
    const { events, sink } = makeSink();
    emitTelemetry(sink, { type: "sync-started", nodeId: "node-a" });
    expect(events).toHaveLength(1);
    expect(events[0]).toEqual({ type: "sync-started", nodeId: "node-a" });
  });

  it("swallows errors thrown by the sink so replication is never broken", () => {
    const throwing: SyncTelemetrySink = () => {
      throw new Error("sink exploded");
    };
    expect(() => emitTelemetry(throwing, { type: "sync-started", nodeId: "node-a" })).not.toThrow();
  });
});

describe("SyncNode telemetry: sync-started, local-write, remote-event (live/applied), sync-stopped", () => {
  it("emits sync-started, local-write, remote-event applied/live, and sync-stopped", async () => {
    const transport = new MemoryTransport();

    const writerTelemetry = makeSink();
    const writer = new SyncNode("writer", new InMemoryPartitionStore("writer"), transport, {
      telemetry: writerTelemetry.sink,
    });

    const subTelemetry = makeSink();
    const subStore = new InMemoryPartitionStore("subscriber");
    const subscriber = new SyncNode("subscriber", subStore, transport, {
      telemetry: subTelemetry.sink,
    });

    await writer.start();
    await subscriber.start();
    await subscriber.subscribe("tenant:jp");

    const written = await writer.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { msg: "hello" },
    });

    await writer.stop();
    await subscriber.stop();

    // Writer: sync-started then local-write then sync-stopped
    const writerTypes = writerTelemetry.events.map((e) => e.type);
    expect(writerTypes).toContain("sync-started");
    expect(writerTypes).toContain("local-write");
    expect(writerTypes).toContain("sync-stopped");

    const writerStartIdx = writerTypes.indexOf("sync-started");
    const writerWriteIdx = writerTypes.indexOf("local-write");
    const writerStopIdx = writerTypes.indexOf("sync-stopped");
    expect(writerStartIdx).toBeLessThan(writerWriteIdx);
    expect(writerWriteIdx).toBeLessThan(writerStopIdx);

    const localWriteEvent = writerTelemetry.events[writerWriteIdx];
    expect(localWriteEvent).toMatchObject({
      type: "local-write",
      nodeId: "writer",
      partitionId: "tenant:jp",
      eventId: written.eventId,
    });

    // Subscriber: should see a remote-event with outcome "applied" and context "live"
    const remoteEvents = subTelemetry.events.filter((e) => e.type === "remote-event");
    expect(remoteEvents).toHaveLength(1);
    expect(remoteEvents[0]).toMatchObject({
      type: "remote-event",
      nodeId: "subscriber",
      partitionId: "tenant:jp",
      sourceNodeId: "writer",
      eventId: written.eventId,
      outcome: "applied",
      context: "live",
    });

    // Subscriber: sync-started and sync-stopped
    const subTypes = subTelemetry.events.map((e) => e.type);
    expect(subTypes).toContain("sync-started");
    expect(subTypes).toContain("sync-stopped");
  });
});

describe("SyncNode telemetry: duplicate delivery and catchup event", () => {
  it("emits remote-event outcome=duplicate context=replay and a catchup event after re-applying via catchUpFrom", async () => {
    const transport = new MemoryTransport();

    const sourceStore = new InMemoryPartitionStore("source");
    await sourceStore.registerPartition("tenant:jp");
    const source = new SyncNode("source", sourceStore, transport);
    await source.start();
    await source.subscribe("tenant:jp");

    // Write two events on the source
    await source.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 1 } });
    await source.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 2 } });

    const targetTelemetry = makeSink();
    const targetStore = new InMemoryPartitionStore("target");
    const target = new SyncNode("target", targetStore, transport, {
      telemetry: targetTelemetry.sink,
    });
    await target.start();
    await target.subscribe("tenant:jp");

    // First catchup: both events are new → applied
    await target.catchUpFrom("source", "tenant:jp");

    // Second catchup from zero (reconcileFrom): same events are already stored → duplicates
    await target.reconcileFrom("source", "tenant:jp");

    await source.stop();
    await target.stop();

    const allEvents = targetTelemetry.events;
    const remoteEvents = allEvents.filter((e) => e.type === "remote-event");
    const catchupEvents = allEvents.filter((e) => e.type === "catchup");

    // After reconcileFrom there should be remote-event events with outcome duplicate and context replay
    const duplicates = remoteEvents.filter(
      (e) => e.type === "remote-event" && e.outcome === "duplicate" && e.context === "replay",
    );
    expect(duplicates.length).toBeGreaterThan(0);

    // There should be at least one catchup event with mode "catchup" and one with "reconcile"
    const catchupModes = catchupEvents
      .filter((e) => e.type === "catchup")
      .map((e) => (e.type === "catchup" ? e.mode : null));
    expect(catchupModes).toContain("catchup");
    expect(catchupModes).toContain("reconcile");

    // Validate the reconcile catchup event fields
    const reconcileEvent = catchupEvents.find(
      (e) => e.type === "catchup" && e.mode === "reconcile",
    );
    expect(reconcileEvent).toBeDefined();
    if (reconcileEvent?.type === "catchup") {
      expect(reconcileEvent.nodeId).toBe("target");
      expect(reconcileEvent.peerNodeId).toBe("source");
      expect(reconcileEvent.partitionId).toBe("tenant:jp");
      expect(reconcileEvent.received).toBe(2);
      expect(reconcileEvent.applied).toBe(0); // all duplicates
      expect(reconcileEvent.durationMs).toBeGreaterThanOrEqual(0);
    }

    // Validate the first catchup event fields
    const catchupEvent = catchupEvents.find((e) => e.type === "catchup" && e.mode === "catchup");
    expect(catchupEvent).toBeDefined();
    if (catchupEvent?.type === "catchup") {
      expect(catchupEvent.nodeId).toBe("target");
      expect(catchupEvent.peerNodeId).toBe("source");
      expect(catchupEvent.partitionId).toBe("tenant:jp");
      expect(catchupEvent.received).toBe(2);
      expect(catchupEvent.applied).toBe(2);
      expect(catchupEvent.durationMs).toBeGreaterThanOrEqual(0);
    }
  });
});

describe("SyncNode telemetry: replay-served", () => {
  it("emits replay-served on the serving node with the correct event count", async () => {
    const transport = new MemoryTransport();

    const sourceTelemetry = makeSink();
    const sourceStore = new InMemoryPartitionStore("source");
    const source = new SyncNode("source", sourceStore, transport, {
      telemetry: sourceTelemetry.sink,
    });
    await source.start();
    await source.subscribe("tenant:jp");

    await source.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 1 } });
    await source.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 2 } });
    await source.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { n: 3 } });

    const targetStore = new InMemoryPartitionStore("target");
    const target = new SyncNode("target", targetStore, transport);
    await target.start();
    await target.subscribe("tenant:jp");

    // Request catchup from source — source must serve the replay
    await target.catchUpFrom("source", "tenant:jp");

    await source.stop();
    await target.stop();

    const replayServedEvents = sourceTelemetry.events.filter((e) => e.type === "replay-served");
    expect(replayServedEvents).toHaveLength(1);
    expect(replayServedEvents[0]).toMatchObject({
      type: "replay-served",
      nodeId: "source",
      partitionId: "tenant:jp",
      events: 3,
    });
  });
});

describe("SyncNode telemetry: rejected-single-writer", () => {
  it("emits remote-event outcome=rejected-single-writer when a violation is caught via onSingleWriterViolation", async () => {
    const transport = new MemoryTransport();

    // Writer A legitimately owns the partition
    const writerA = new SyncNode("writer-a", new InMemoryPartitionStore("writer-a"), transport);
    await writerA.start();
    await writerA.subscribe("tenant:jp");
    await writerA.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { from: "a" } });

    // Writer B also writes to the same partition (violation)
    const writerB = new SyncNode("writer-b", new InMemoryPartitionStore("writer-b"), transport);
    await writerB.start();
    await writerB.subscribe("tenant:jp");
    await writerB.write({ partitionId: "tenant:jp", schemaVersion: 1, payload: { from: "b" } });

    // Target uses a shared registry; writer-a is observed first (via catchup) so it
    // becomes the owner; writer-b events are violations.
    const registry = new SingleWriterRegistry();
    const violations: unknown[] = [];
    const targetTelemetry = makeSink();
    const targetStore = new InMemoryPartitionStore("target");
    const target = new SyncNode("target", targetStore, transport, {
      singleWriter: registry,
      onSingleWriterViolation: (err) => violations.push(err),
      telemetry: targetTelemetry.sink,
    });
    await target.start();
    await target.subscribe("tenant:jp");

    // Catchup from writer-a first so the registry learns writer-a as owner
    await target.catchUpFrom("writer-a", "tenant:jp");
    // Catchup from writer-b: its event violates the registry
    await target.catchUpFrom("writer-b", "tenant:jp");

    await writerA.stop();
    await writerB.stop();
    await target.stop();

    const rejected = targetTelemetry.events.filter(
      (e) => e.type === "remote-event" && e.outcome === "rejected-single-writer",
    );
    expect(rejected.length).toBeGreaterThan(0);
    expect(violations.length).toBeGreaterThan(0);

    // Confirm that the rejected event's sourceNodeId is writer-b (the violator)
    expect(rejected[0]).toMatchObject({
      type: "remote-event",
      nodeId: "target",
      partitionId: "tenant:jp",
      sourceNodeId: "writer-b",
      outcome: "rejected-single-writer",
      context: "replay",
    });
  });
});

describe("SyncNode telemetry: throwing sink does not break replication", () => {
  it("continues replicating events even when the telemetry sink throws on every call", async () => {
    const throwingSink: SyncTelemetrySink = () => {
      throw new Error("sink always throws");
    };

    const transport = new MemoryTransport();
    const writerStore = new InMemoryPartitionStore("writer");
    const writer = new SyncNode("writer", writerStore, transport, { telemetry: throwingSink });

    const subStore = new InMemoryPartitionStore("subscriber");
    const subscriber = new SyncNode("subscriber", subStore, transport, {
      telemetry: throwingSink,
    });

    await writer.start();
    await subscriber.start();
    await subscriber.subscribe("tenant:jp");

    const written = await writer.write({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: 42 },
    });

    await writer.stop();
    await subscriber.stop();

    // The event must have been replicated into the subscriber's store despite the throwing sink
    const stored = await subStore.listEventsAfterCursors("tenant:jp", new Map());
    expect(stored).toHaveLength(1);
    expect(stored[0]?.eventId).toBe(written.eventId);
    expect(stored[0]?.payload).toEqual({ value: 42 });

    // The writer's store should also contain the event
    const watermark = await writerStore.getHighWatermark("tenant:jp");
    expect(watermark.has("writer")).toBe(true);
  });
});
