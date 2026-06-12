import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  MemoryTransport,
  SingleWriterRegistry,
  SingleWriterViolationError,
  SnapshotRequiredError,
} from "@distrb-pglite/core";
import type { SyncTelemetryEvent } from "@distrb-pglite/core";
import { DistributedPGlite, SharedDirSnapshotSource, WebSocketHub } from "../src/index.js";
import { getFreePort, waitFor } from "./transport-contract.js";
import { makeTempDir } from "./temp-dir.js";

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS sensor_readings (
    source_node text NOT NULL,
    event_id text NOT NULL,
    sensor_id text NOT NULL,
    reading integer NOT NULL,
    PRIMARY KEY (source_node, event_id)
  );
`;

type SensorPayload = {
  sensorId: string;
  reading: number;
};

describe("DistributedPGlite", () => {
  it("opens a synchronized PGlite facade and projects catchup events transactionally", async () => {
    const port = await getFreePort();
    const hub = new WebSocketHub({ port });
    const root = await makeTempDir("distributed-pglite");
    let nodeA: DistributedPGlite | undefined;
    let nodeB: DistributedPGlite | undefined;

    try {
      const url = `ws://127.0.0.1:${port}`;
      nodeA = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport: { type: "websocket", url },
        partitions: [sensorPartition("tenant:jp")],
      });
      nodeB = await DistributedPGlite.open({
        nodeId: "node-b",
        dataDir: join(root, "node-b"),
        schema: SCHEMA,
        transport: { type: "websocket", url },
        partitions: [sensorPartition("tenant:jp")],
      });

      const event = await nodeA.write<SensorPayload>("tenant:jp", {
        sensorId: "tokyo-temperature",
        reading: 21,
      });

      await expect(readSensors(nodeA)).resolves.toEqual([
        {
          event_id: event.eventId,
          reading: 21,
          sensor_id: "tokyo-temperature",
          source_node: "node-a",
        },
      ]);

      await waitFor(async () => {
        await expect(readSensors(nodeB!)).resolves.toEqual([
          {
            event_id: event.eventId,
            reading: 21,
            sensor_id: "tokyo-temperature",
            source_node: "node-a",
          },
        ]);
      });
    } finally {
      await nodeA?.close();
      await nodeB?.close();
      await hub.close();
    }
  });

  it("lets a late node catch up without constructing low-level sync pieces", async () => {
    const port = await getFreePort();
    const hub = new WebSocketHub({ port });
    const root = await makeTempDir("distributed-pglite-late");
    let nodeA: DistributedPGlite | undefined;
    let nodeB: DistributedPGlite | undefined;

    try {
      const url = `ws://127.0.0.1:${port}`;
      nodeA = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport: { type: "websocket", url },
        partitions: [sensorPartition("tenant:jp")],
      });

      const first = await nodeA.write<SensorPayload>("tenant:jp", {
        sensorId: "tokyo-temperature",
        reading: 21,
      });

      nodeB = await DistributedPGlite.open({
        nodeId: "node-b",
        dataDir: join(root, "node-b"),
        schema: SCHEMA,
        transport: { type: "websocket", url },
        partitions: [sensorPartition("tenant:jp")],
      });

      await expect(readSensors(nodeB)).resolves.toEqual([]);

      await expect(nodeB.catchUpFrom("node-a", "tenant:jp")).resolves.toBe(1);
      await expect(readSensors(nodeB)).resolves.toEqual([
        {
          event_id: first.eventId,
          reading: 21,
          sensor_id: "tokyo-temperature",
          source_node: "node-a",
        },
      ]);
    } finally {
      await nodeA?.close();
      await nodeB?.close();
      await hub.close();
    }
  });

  it("creates storage snapshots through the high-level handle", async () => {
    const port = await getFreePort();
    const hub = new WebSocketHub({ port });
    const root = await makeTempDir("distributed-pglite-snapshot");
    let node: DistributedPGlite | undefined;

    try {
      node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "data"),
        casDir: join(root, "cas"),
        schema: SCHEMA,
        transport: { type: "websocket", url: `ws://127.0.0.1:${port}` },
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      await node.write<SensorPayload>("tenant:jp", {
        sensorId: "tokyo-temperature",
        reading: 21,
      });

      const snapshot = await node.createSnapshot("tenant:jp");
      const catalog = await node.loadCatalog("sensors");
      const entries = await node.listSnapshotEntries("sensors");

      expect(snapshot.snapshot.partitionId).toBe("tenant:jp");
      expect(snapshot.snapshot.manifest.files.length).toBeGreaterThan(0);
      expect(catalog?.snapshots).toEqual([
        expect.objectContaining({
          partitionId: "tenant:jp",
          snapshotId: snapshot.snapshot.snapshotId,
          manifestKind: "cas",
        }),
      ]);
      expect(entries).toEqual([
        expect.objectContaining({
          partitionId: "tenant:jp",
          snapshotId: snapshot.snapshot.snapshotId,
          manifestKind: "cas",
        }),
      ]);
    } finally {
      await node?.close();
      await hub.close();
    }
  });

  describe("telemetry option", () => {
    it("emits local-write and remote-event{applied} events when a write is replicated", async () => {
      const root = await makeTempDir("distributed-pglite-telemetry");
      const transport = new MemoryTransport();
      const sinkA: SyncTelemetryEvent[] = [];
      const sinkB: SyncTelemetryEvent[] = [];

      const nodeA = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport,
        partitions: [sensorPartition("tenant:jp")],
        telemetry: (e) => sinkA.push(e),
      });
      const nodeB = await DistributedPGlite.open({
        nodeId: "node-b",
        dataDir: join(root, "node-b"),
        schema: SCHEMA,
        transport,
        partitions: [sensorPartition("tenant:jp")],
        telemetry: (e) => sinkB.push(e),
      });

      try {
        await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 1 });

        await waitFor(async () => {
          await expect(readSensors(nodeB)).resolves.toHaveLength(1);
        });

        expect(sinkA.some((e) => e.type === "local-write")).toBe(true);
        expect(sinkB.some((e) => e.type === "remote-event" && e.outcome === "applied")).toBe(true);
      } finally {
        await nodeA.close();
        await nodeB.close();
      }
    });
  });

  describe("ownership helpers without singleWriter option", () => {
    it("partitionOwner, claimPartition, and transferPartition throw the expected error", async () => {
      const root = await makeTempDir("distributed-pglite-owner-no-sw");
      const transport = new MemoryTransport();
      const node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport,
      });
      try {
        expect(() => node.partitionOwner("tenant:jp")).toThrow(
          "single-writer ownership is not enabled",
        );
        expect(() => node.claimPartition("tenant:jp")).toThrow(
          "single-writer ownership is not enabled",
        );
        expect(() => node.transferPartition("tenant:jp", "node-b")).toThrow(
          "single-writer ownership is not enabled",
        );
      } finally {
        await node.close();
      }
    });
  });

  describe("ownership helpers with singleWriter option", () => {
    it("claimPartition defaults to own nodeId and partitionOwner reflects it", async () => {
      const root = await makeTempDir("distributed-pglite-claim");
      const transport = new MemoryTransport();
      const node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport,
        singleWriter: new SingleWriterRegistry(),
      });
      try {
        expect(node.partitionOwner("tenant:jp")).toBeUndefined();
        node.claimPartition("tenant:jp");
        expect(node.partitionOwner("tenant:jp")).toBe("node-a");
      } finally {
        await node.close();
      }
    });

    it("transferPartition moves ownership and partitionOwner reflects the new owner", async () => {
      const root = await makeTempDir("distributed-pglite-transfer");
      const transport = new MemoryTransport();
      const node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport,
        singleWriter: new SingleWriterRegistry(),
      });
      try {
        node.claimPartition("tenant:jp");
        node.transferPartition("tenant:jp", "node-b");
        expect(node.partitionOwner("tenant:jp")).toBe("node-b");
      } finally {
        await node.close();
      }
    });

    it("transferPartition throws when the partition has no owner", async () => {
      const root = await makeTempDir("distributed-pglite-transfer-noowner");
      const transport = new MemoryTransport();
      const node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport,
        singleWriter: new SingleWriterRegistry(),
      });
      try {
        expect(() => node.transferPartition("tenant:jp", "node-b")).toThrow(
          "has no writer to transfer from",
        );
      } finally {
        await node.close();
      }
    });

    it("write throws SingleWriterViolationError after ownership is transferred away", async () => {
      const root = await makeTempDir("distributed-pglite-violation");
      const transport = new MemoryTransport();
      const node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a"),
        schema: SCHEMA,
        transport,
        singleWriter: new SingleWriterRegistry(),
        partitions: [{ id: "tenant:jp" }],
      });
      try {
        node.claimPartition("tenant:jp", "node-a");
        node.transferPartition("tenant:jp", "node-b");

        // node-a still holds the sync connection but no longer owns the partition
        await expect(
          node.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 99 }),
        ).rejects.toThrow(SingleWriterViolationError);
      } finally {
        await node.close();
      }
    });
  });
});

describe("pruneEvents", () => {
  it("happy path: prunes events at/below snapshot HWM, preserves newer events, emits telemetry, HWM covers all", async () => {
    const root = await makeTempDir("distributed-pglite-prune-happy");
    const transport = new MemoryTransport();
    const telemetryEvents: SyncTelemetryEvent[] = [];
    let node: DistributedPGlite | undefined;

    try {
      node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "data"),
        casDir: join(root, "cas"),
        schema: SCHEMA,
        transport,
        telemetry: (e) => telemetryEvents.push(e),
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      // Write 3 events before snapshot
      await node.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 1 });
      await node.write<SensorPayload>("tenant:jp", { sensorId: "s2", reading: 2 });
      await node.write<SensorPayload>("tenant:jp", { sensorId: "s3", reading: 3 });

      // Create a bootstrap-capable snapshot (anchors HWM at 3 events)
      await node.createSnapshot("tenant:jp");

      // Write 2 more events after snapshot
      const e4 = await node.write<SensorPayload>("tenant:jp", { sensorId: "s4", reading: 4 });
      const e5 = await node.write<SensorPayload>("tenant:jp", { sensorId: "s5", reading: 5 });

      // pruneEvents defaults to snapshot HWM → should prune the first 3 events
      const result = await node.pruneEvents("tenant:jp");

      expect(result.pruned).toBe(3);
      expect(typeof result.anchorSnapshotId).toBe("string");
      expect(result.through.size).toBeGreaterThan(0);

      // Only the 2 post-snapshot events should remain
      const remaining = await node.listEventsAfter("tenant:jp");
      expect(remaining).toHaveLength(2);
      expect(remaining.map((e) => e.eventId)).toEqual(
        expect.arrayContaining([e4.eventId, e5.eventId]),
      );
      expect(remaining.map((e) => e.eventId)).not.toContain(expect.stringMatching(/.+/));

      // HWM must still cover all 5 events (floor seeding keeps it moving forward)
      const hwm = await node.highWatermark("tenant:jp");
      expect(hwm?.get("node-a")).toBe(e5.eventId);

      // Telemetry must include an events-pruned event
      const pruneEvent = telemetryEvents.find((e) => e.type === "events-pruned");
      expect(pruneEvent).toBeDefined();
      expect(pruneEvent).toMatchObject({
        type: "events-pruned",
        nodeId: "node-a",
        partitionId: "tenant:jp",
        pruned: 3,
        durationMs: expect.any(Number),
      });
    } finally {
      await node?.close();
    }
  }, 120_000);

  it("rejects with 'no prune boundary' when no snapshot exists", async () => {
    const root = await makeTempDir("distributed-pglite-prune-no-snapshot");
    const transport = new MemoryTransport();
    let node: DistributedPGlite | undefined;

    try {
      node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "data"),
        casDir: join(root, "cas"),
        schema: SCHEMA,
        transport,
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      await node.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 1 });

      await expect(node.pruneEvents("tenant:jp")).rejects.toThrow("no prune boundary");
    } finally {
      await node?.close();
    }
  }, 120_000);

  it("end-to-end gap + recovery: node-b catchUpFrom rejects with SnapshotRequiredError, then recovers via pullSnapshot + catchUpFrom", async () => {
    const root = await makeTempDir("distributed-pglite-prune-gap-recovery");
    const transport = new MemoryTransport();
    let nodeA: DistributedPGlite | undefined;
    let nodeB: DistributedPGlite | undefined;

    try {
      // node-a: writer, creates snapshot, prunes
      nodeA = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a-data"),
        casDir: join(root, "node-a-cas"),
        schema: SCHEMA,
        transport,
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      // Write 3 events, snapshot, prune
      await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 10 });
      await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s2", reading: 20 });
      const e3 = await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s3", reading: 30 });

      await nodeA.createSnapshot("tenant:jp");
      const { pruned } = await nodeA.pruneEvents("tenant:jp");
      expect(pruned).toBe(3);

      // Write 1 more event after prune so node-b will catch up to something
      const e4 = await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s4", reading: 40 });

      // node-b: fresh node, same transport, empty store, subscribed
      nodeB = await DistributedPGlite.open({
        nodeId: "node-b",
        dataDir: join(root, "node-b-data"),
        casDir: join(root, "node-b-cas"),
        schema: SCHEMA,
        transport,
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      // catchUpFrom should fail with SnapshotRequiredError because node-a pruned
      await expect(nodeB.catchUpFrom("node-a", "tenant:jp")).rejects.toThrow(SnapshotRequiredError);

      // Recovery: pull the snapshot from node-a's casDir, then catch up
      const snapshotEntries = await nodeA.listSnapshotEntries("sensors");
      expect(snapshotEntries.length).toBeGreaterThan(0);

      await nodeB.pullSnapshot({
        partitionId: "tenant:jp",
        snapshotEntries,
        sourceCasDir: join(root, "node-a-cas"),
      });

      // After pullSnapshot, catchUpFrom should succeed and pick up e4 (post-prune event)
      const caught = await nodeB.catchUpFrom("node-a", "tenant:jp");
      expect(caught).toBeGreaterThanOrEqual(1);

      // node-b query results should match node-a
      const rowsA = await readSensors(nodeA);
      const rowsB = await readSensors(nodeB);
      expect(rowsB).toEqual(rowsA);

      // Verify e4 is present in node-b
      const eventIds = rowsB.map((r) => r.event_id);
      expect(eventIds).toContain(e4.eventId);
      // e3 may or may not be present depending on snapshot scope, but the total rows must match
      expect(rowsB.length).toBeGreaterThanOrEqual(1);
      void e3; // used above
    } finally {
      await nodeA?.close();
      await nodeB?.close();
    }
  }, 120_000);
});

describe("runCasGc", () => {
  it("runs dryRun GC on a group with a snapshot and emits a cas-gc telemetry event", async () => {
    const root = await makeTempDir("distributed-pglite-cas-gc");
    const transport = new MemoryTransport();
    const telemetryEvents: SyncTelemetryEvent[] = [];
    let node: DistributedPGlite | undefined;

    try {
      node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "data"),
        casDir: join(root, "cas"),
        schema: SCHEMA,
        transport,
        telemetry: (e) => telemetryEvents.push(e),
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      await node.write<SensorPayload>("tenant:jp", { sensorId: "tokyo-temperature", reading: 25 });
      await node.createSnapshot("tenant:jp");

      const execution = await node.runCasGc("sensors", { dryRun: true });

      expect(execution).toBeDefined();
      expect(Array.isArray(execution!.gc.deleted)).toBe(true);
      expect(Array.isArray(execution!.gc.retained)).toBe(true);
      // The snapshot we just created is recent and should be retained
      expect(execution!.gc.retained.length).toBeGreaterThan(0);

      const gcEvent = telemetryEvents.find((e) => e.type === "cas-gc");
      expect(gcEvent).toBeDefined();
      expect(gcEvent).toMatchObject({
        type: "cas-gc",
        groupId: "sensors",
        dryRun: true,
        durationMs: expect.any(Number),
      });
      expect(
        (gcEvent as Extract<SyncTelemetryEvent, { type: "cas-gc" }>).durationMs,
      ).toBeGreaterThanOrEqual(0);
    } finally {
      await node?.close();
    }
  }, 120_000);

  it("returns undefined and emits no cas-gc event for a nonexistent groupId", async () => {
    const root = await makeTempDir("distributed-pglite-cas-gc-missing");
    const transport = new MemoryTransport();
    const telemetryEvents: SyncTelemetryEvent[] = [];
    let node: DistributedPGlite | undefined;

    try {
      node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "data"),
        casDir: join(root, "cas"),
        schema: SCHEMA,
        transport,
        telemetry: (e) => telemetryEvents.push(e),
      });

      const execution = await node.runCasGc("nonexistent-group");

      expect(execution).toBeUndefined();
      expect(telemetryEvents.some((e) => e.type === "cas-gc")).toBe(false);
    } finally {
      await node?.close();
    }
  }, 120_000);

  it("rejects with casDir error when opened without casDir", async () => {
    const root = await makeTempDir("distributed-pglite-cas-gc-no-casdir");
    const transport = new MemoryTransport();
    let node: DistributedPGlite | undefined;

    try {
      node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "data"),
        schema: SCHEMA,
        transport,
      });

      await expect(node.runCasGc("sensors")).rejects.toThrow("casDir is required");
    } finally {
      await node?.close();
    }
  }, 120_000);
});

describe("snapshotSource auto-recovery", () => {
  it("catchUpFrom recovers automatically via snapshotSource when events were pruned", async () => {
    const root = await makeTempDir("distributed-pglite-snapshot-source-auto");
    const transport = new MemoryTransport();
    const telemetryB: SyncTelemetryEvent[] = [];
    let nodeA: DistributedPGlite | undefined;
    let nodeB: DistributedPGlite | undefined;

    try {
      // node-a: storage-bound, writes, snapshot, prune
      nodeA = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a-data"),
        casDir: join(root, "node-a-cas"),
        schema: SCHEMA,
        transport,
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 1 });
      await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s2", reading: 2 });
      await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s3", reading: 3 });

      await nodeA.createSnapshot("tenant:jp");
      const { pruned } = await nodeA.pruneEvents("tenant:jp");
      expect(pruned).toBe(3);

      // Write one more event after prune
      await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s4", reading: 4 });

      // node-b: fresh, with snapshotSource pointing at node-a's CAS
      nodeB = await DistributedPGlite.open({
        nodeId: "node-b",
        dataDir: join(root, "node-b-data"),
        casDir: join(root, "node-b-cas"),
        schema: SCHEMA,
        transport,
        telemetry: (e) => telemetryB.push(e),
        snapshotSource: new SharedDirSnapshotSource({
          sourceCasDir: join(root, "node-a-cas"),
          targetCasDir: join(root, "node-b-cas"),
          listEntries: (_peer, _pid) => nodeA!.listSnapshotEntries("sensors"),
          loadCatalogManifest: (_peer, gid) => nodeA!.loadCatalog(gid),
        }),
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      // catchUpFrom should NOT throw — auto-recovery via snapshotSource
      const applied = await nodeB.catchUpFrom("node-a", "tenant:jp");
      expect(applied).toBeGreaterThan(0);

      // Projected rows must match node-a
      const rowsA = await readSensors(nodeA);
      const rowsB = await readSensors(nodeB);
      expect(rowsB).toEqual(rowsA);

      // Telemetry on node-b must include snapshot-required (received) and snapshot-pulled
      expect(telemetryB.some((e) => e.type === "snapshot-required")).toBe(true);
      const snapRequired = telemetryB.find((e) => e.type === "snapshot-required");
      expect(snapRequired).toMatchObject({ direction: "received" });
      expect(telemetryB.some((e) => e.type === "snapshot-pulled")).toBe(true);
    } finally {
      await nodeA?.close();
      await nodeB?.close();
    }
  }, 120_000);

  it("without snapshotSource, same scenario rejects with SnapshotRequiredError", async () => {
    const root = await makeTempDir("distributed-pglite-snapshot-source-no-source");
    const transport = new MemoryTransport();
    let nodeA: DistributedPGlite | undefined;
    let nodeB: DistributedPGlite | undefined;

    try {
      nodeA = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "node-a-data"),
        casDir: join(root, "node-a-cas"),
        schema: SCHEMA,
        transport,
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      await nodeA.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 1 });
      await nodeA.createSnapshot("tenant:jp");
      await nodeA.pruneEvents("tenant:jp");

      // node-b: no snapshotSource
      nodeB = await DistributedPGlite.open({
        nodeId: "node-b",
        dataDir: join(root, "node-b-data"),
        casDir: join(root, "node-b-cas"),
        schema: SCHEMA,
        transport,
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
            },
          },
        ],
      });

      await expect(nodeB.catchUpFrom("node-a", "tenant:jp")).rejects.toThrow(SnapshotRequiredError);
    } finally {
      await nodeA?.close();
      await nodeB?.close();
    }
  }, 120_000);
});

describe("runCasGc autoPruneEvents", () => {
  it("prunes events through oldest retained bootstrap anchor when autoPruneEvents is true", async () => {
    const root = await makeTempDir("distributed-pglite-auto-prune-events");
    const transport = new MemoryTransport();
    let node: DistributedPGlite | undefined;

    try {
      node = await DistributedPGlite.open({
        nodeId: "node-a",
        dataDir: join(root, "data"),
        casDir: join(root, "cas"),
        schema: SCHEMA,
        transport,
        partitions: [
          {
            ...sensorPartition("tenant:jp"),
            storage: {
              groupId: "sensors",
              rootRelation: "sensor_readings",
              relations: ["sensor_readings"],
              snapshotScope: "catalog-and-data",
              vacuumPolicy: {
                retainSnapshotGenerations: 1,
                gcAfterGeneration: 0,
                autoPruneEvents: true,
              },
            },
          },
        ],
      });

      // Write 3 events, create snapshot (anchor)
      await node.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 1 });
      await node.write<SensorPayload>("tenant:jp", { sensorId: "s2", reading: 2 });
      const e3 = await node.write<SensorPayload>("tenant:jp", { sensorId: "s3", reading: 3 });

      await node.createSnapshot("tenant:jp");

      // Write 2 more events after the snapshot anchor
      const e4 = await node.write<SensorPayload>("tenant:jp", { sensorId: "s4", reading: 4 });
      const e5 = await node.write<SensorPayload>("tenant:jp", { sensorId: "s5", reading: 5 });

      // dryRun: prunedEvents must be undefined and no events removed
      const dryResult = await node.runCasGc("sensors", { dryRun: true });
      expect(dryResult).toBeDefined();
      expect(dryResult!.prunedEvents).toBeUndefined();

      // All 5 events still present
      const beforePrune = await node.listEventsAfter("tenant:jp");
      expect(beforePrune).toHaveLength(5);

      // Real GC: prunedEvents["tenant:jp"] equals the 3 events at/below anchor HWM
      const gcResult = await node.runCasGc("sensors");
      expect(gcResult).toBeDefined();
      expect(gcResult!.prunedEvents).toBeDefined();
      expect(gcResult!.prunedEvents!["tenant:jp"]).toBe(3);

      // Only the 2 post-anchor events remain
      const remaining = await node.listEventsAfter("tenant:jp");
      expect(remaining).toHaveLength(2);
      const remainingIds = remaining.map((e) => e.eventId);
      expect(remainingIds).toContain(e4.eventId);
      expect(remainingIds).toContain(e5.eventId);
      expect(remainingIds).not.toContain(e3.eventId);
    } finally {
      await node?.close();
    }
  }, 120_000);

  it("vacuumPolicy persists across reopen: autoPruneEvents survives close/open without vacuumPolicy", async () => {
    const root = await makeTempDir("distributed-pglite-vacuum-policy-persist");
    const transport = new MemoryTransport();

    // Open with vacuumPolicy
    let node = await DistributedPGlite.open({
      nodeId: "node-a",
      dataDir: join(root, "data"),
      casDir: join(root, "cas"),
      schema: SCHEMA,
      transport,
      partitions: [
        {
          ...sensorPartition("tenant:jp"),
          storage: {
            groupId: "sensors",
            rootRelation: "sensor_readings",
            relations: ["sensor_readings"],
            snapshotScope: "catalog-and-data",
            vacuumPolicy: {
              retainSnapshotGenerations: 1,
              gcAfterGeneration: 0,
              autoPruneEvents: true,
            },
          },
        },
      ],
    });

    await node.write<SensorPayload>("tenant:jp", { sensorId: "s1", reading: 1 });
    await node.createSnapshot("tenant:jp");
    await node.write<SensorPayload>("tenant:jp", { sensorId: "s2", reading: 2 });

    await node.close();

    // Reopen WITHOUT vacuumPolicy in storage options
    node = await DistributedPGlite.open({
      nodeId: "node-a",
      dataDir: join(root, "data"),
      casDir: join(root, "cas"),
      schema: SCHEMA,
      transport,
      partitions: [
        {
          ...sensorPartition("tenant:jp"),
          storage: {
            groupId: "sensors",
            rootRelation: "sensor_readings",
            relations: ["sensor_readings"],
            snapshotScope: "catalog-and-data",
            // no vacuumPolicy here — should survive from persisted state
          },
        },
      ],
    });

    try {
      // Verify the policy survived by querying the binding group directly
      const result = await node.query<{ vacuum_policy: unknown }>(
        `SELECT vacuum_policy FROM distrb_storage_binding_groups WHERE group_id = $1`,
        ["sensors"],
      );
      expect(result.rows).toHaveLength(1);
      const policy = result.rows[0]!.vacuum_policy as {
        autoPruneEvents?: boolean;
        retainSnapshotGenerations?: number;
      } | null;
      expect(policy).not.toBeNull();
      expect(policy!.autoPruneEvents).toBe(true);
      expect(policy!.retainSnapshotGenerations).toBe(1);

      // runCasGc should also report prunedEvents (policy is live)
      const gcResult = await node.runCasGc("sensors");
      expect(gcResult).toBeDefined();
      expect(gcResult!.prunedEvents).toBeDefined();
    } finally {
      await node.close();
    }
  }, 120_000);
});

function sensorPartition(id: string) {
  return {
    id,
    project: async (db, event) => {
      const payload = event.payload as SensorPayload;
      await db.query(
        `
          INSERT INTO sensor_readings (source_node, event_id, sensor_id, reading)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (source_node, event_id) DO NOTHING
        `,
        [event.sourceNodeId, event.eventId, payload.sensorId, payload.reading],
      );
    },
  };
}

async function readSensors(db: DistributedPGlite) {
  const result = await db.query<{
    source_node: string;
    event_id: string;
    sensor_id: string;
    reading: number;
  }>(
    `
      SELECT source_node, event_id, sensor_id, reading
      FROM sensor_readings
      ORDER BY source_node, event_id
    `,
  );
  return result.rows;
}
