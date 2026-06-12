import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  MemoryTransport,
  SingleWriterRegistry,
  SingleWriterViolationError,
} from "@distrb-pglite/core";
import type { SyncTelemetryEvent } from "@distrb-pglite/core";
import { DistributedPGlite, WebSocketHub } from "../src/index.js";
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
