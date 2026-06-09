import { join } from "node:path";
import { describe, expect, it } from "vitest";
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
