import {
  MemoryTransport,
  SingleWriterRegistry,
  SingleWriterViolationError,
} from "@distrb-pglite/core";
import type { SyncTelemetryEvent } from "@distrb-pglite/core";
import { describe, expect, it } from "vitest";
import { DistributedBrowserPGlite, type BrowserCasBlockStore } from "../src/index.js";

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS readings (
    source_node text NOT NULL,
    event_id text NOT NULL,
    sensor_id text NOT NULL,
    reading integer NOT NULL,
    PRIMARY KEY (source_node, event_id)
  );
`;

type ReadingPayload = {
  sensorId: string;
  reading: number;
};

describe("DistributedBrowserPGlite", () => {
  it("opens a browser PGlite facade and projects replicated events", async () => {
    const transport = new MemoryTransport();
    const nodeA = await openNode("browser-a", transport);
    const nodeB = await openNode("browser-b", transport);

    try {
      const event = await nodeA.write<ReadingPayload>("tenant:jp", {
        sensorId: "tokyo-temperature",
        reading: 21,
      });

      await expect(readings(nodeA)).resolves.toEqual([
        {
          event_id: event.eventId,
          reading: 21,
          sensor_id: "tokyo-temperature",
          source_node: "browser-a",
        },
      ]);
      await expect(readings(nodeB)).resolves.toEqual([
        {
          event_id: event.eventId,
          reading: 21,
          sensor_id: "tokyo-temperature",
          source_node: "browser-a",
        },
      ]);
    } finally {
      await nodeA.close();
      await nodeB.close();
    }
  });

  it("lets late browser nodes catch up through the high-level handle", async () => {
    const transport = new MemoryTransport();
    const nodeA = await openNode("browser-a", transport);
    const first = await nodeA.write<ReadingPayload>("tenant:jp", {
      sensorId: "tokyo-temperature",
      reading: 21,
    });
    const nodeB = await openNode("browser-b", transport);

    try {
      await expect(readings(nodeB)).resolves.toEqual([]);
      await expect(nodeB.catchUpFrom("browser-a", "tenant:jp")).resolves.toBe(1);
      await expect(readings(nodeB)).resolves.toEqual([
        {
          event_id: first.eventId,
          reading: 21,
          sensor_id: "tokyo-temperature",
          source_node: "browser-a",
        },
      ]);
    } finally {
      await nodeA.close();
      await nodeB.close();
    }
  });

  it("restores a browser filesystem snapshot and reopens the high-level handle", async () => {
    const transport = new MemoryTransport();
    const nodeA = await openNode("browser-a", transport);
    let nodeB = await openNode("browser-b", transport);
    const sourceCasStore = new TestBrowserCasBlockStore();
    const targetCasStore = new TestBrowserCasBlockStore();

    try {
      await nodeA.write<ReadingPayload>("tenant:jp", {
        sensorId: "tokyo-temperature",
        reading: 21,
      });
      const manifest = await nodeA.createSnapshot({
        casStore: sourceCasStore,
        snapshotId: "snapshot-browser-a",
      });

      const restored = await nodeB.restoreSnapshot({
        manifest,
        sourceCasStore,
        targetCasStore,
      });
      nodeB = restored.handle;

      await expect(readings(nodeB)).resolves.toEqual([
        {
          event_id: expect.any(String),
          reading: 21,
          sensor_id: "tokyo-temperature",
          source_node: "browser-a",
        },
      ]);
      const second = await nodeA.write<ReadingPayload>("tenant:jp", {
        sensorId: "tokyo-humidity",
        reading: 55,
      });
      await nodeB.start();
      await expect(nodeB.catchUpFrom("browser-a", "tenant:jp")).resolves.toBe(1);
      await expect(readings(nodeB)).resolves.toEqual([
        {
          event_id: expect.any(String),
          reading: 21,
          sensor_id: "tokyo-temperature",
          source_node: "browser-a",
        },
        {
          event_id: second.eventId,
          reading: 55,
          sensor_id: "tokyo-humidity",
          source_node: "browser-a",
        },
      ]);
      expect(restored.transferredBlocks.length).toBeGreaterThan(0);
    } finally {
      await nodeA.close();
      await nodeB.close();
    }
  });

  it("rolls back browser events when local SQL projection fails", async () => {
    const transport = new MemoryTransport();
    const node = await openNode("browser-a", transport);

    try {
      await expect(
        node.write<ReadingPayload>({
          partitionId: "tenant:jp",
          payload: { sensorId: "tokyo-temperature", reading: 21 },
          project: async () => {
            throw new Error("projection failed");
          },
        }),
      ).rejects.toThrow("projection failed");

      await expect(readings(node)).resolves.toEqual([]);
      await expect(node.highWatermark("tenant:jp")).resolves.toEqual(new Map());

      const event = await node.write<ReadingPayload>("tenant:jp", {
        sensorId: "tokyo-temperature",
        reading: 22,
      });
      await expect(readings(node)).resolves.toEqual([
        {
          event_id: event.eventId,
          reading: 22,
          sensor_id: "tokyo-temperature",
          source_node: "browser-a",
        },
      ]);
    } finally {
      await node.close();
    }
  });

  describe("telemetry option", () => {
    it("emits local-write and remote-event{applied} events when a write is replicated", async () => {
      const transport = new MemoryTransport();
      const sinkA: SyncTelemetryEvent[] = [];
      const sinkB: SyncTelemetryEvent[] = [];

      const nodeA = await openNode("browser-a", transport, { telemetry: (e) => sinkA.push(e) });
      const nodeB = await openNode("browser-b", transport, { telemetry: (e) => sinkB.push(e) });

      try {
        await nodeA.write<ReadingPayload>("tenant:jp", { sensorId: "s1", reading: 1 });

        // MemoryTransport delivers synchronously so nodeB already has the event
        await expect(readings(nodeB)).resolves.toHaveLength(1);

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
      const transport = new MemoryTransport();
      const node = await openNode("browser-a", transport);
      try {
        expect(() => node.partitionOwner("tenant:jp")).toThrow(
          "single-writer ownership is not enabled",
        );
        expect(() => node.claimPartition("tenant:jp")).toThrow(
          "single-writer ownership is not enabled",
        );
        expect(() => node.transferPartition("tenant:jp", "browser-b")).toThrow(
          "single-writer ownership is not enabled",
        );
      } finally {
        await node.close();
      }
    });
  });

  describe("ownership helpers with singleWriter option", () => {
    it("claimPartition defaults to own nodeId and partitionOwner reflects it", async () => {
      const transport = new MemoryTransport();
      const node = await openNode("browser-a", transport, {
        singleWriter: new SingleWriterRegistry(),
      });
      try {
        expect(node.partitionOwner("tenant:jp")).toBeUndefined();
        node.claimPartition("tenant:jp");
        expect(node.partitionOwner("tenant:jp")).toBe("browser-a");
      } finally {
        await node.close();
      }
    });

    it("transferPartition moves ownership and partitionOwner reflects the new owner", async () => {
      const transport = new MemoryTransport();
      const node = await openNode("browser-a", transport, {
        singleWriter: new SingleWriterRegistry(),
      });
      try {
        node.claimPartition("tenant:jp");
        node.transferPartition("tenant:jp", "browser-b");
        expect(node.partitionOwner("tenant:jp")).toBe("browser-b");
      } finally {
        await node.close();
      }
    });

    it("transferPartition throws when the partition has no owner", async () => {
      const transport = new MemoryTransport();
      const node = await openNode("browser-a", transport, {
        singleWriter: new SingleWriterRegistry(),
      });
      try {
        expect(() => node.transferPartition("tenant:jp", "browser-b")).toThrow(
          "has no writer to transfer from",
        );
      } finally {
        await node.close();
      }
    });

    it("write throws SingleWriterViolationError after ownership is transferred away", async () => {
      const transport = new MemoryTransport();
      const node = await openNode("browser-a", transport, {
        singleWriter: new SingleWriterRegistry(),
      });
      try {
        node.claimPartition("tenant:jp", "browser-a");
        node.transferPartition("tenant:jp", "browser-b");

        // browser-a still holds the sync connection but no longer owns the partition
        await expect(
          node.write<ReadingPayload>("tenant:jp", { sensorId: "s1", reading: 99 }),
        ).rejects.toThrow(SingleWriterViolationError);
      } finally {
        await node.close();
      }
    });
  });
});

function openNode(
  nodeId: string,
  transport: MemoryTransport,
  extra?: Partial<
    Pick<
      Parameters<typeof DistributedBrowserPGlite.open>[0],
      "telemetry" | "singleWriter" | "onSingleWriterViolation"
    >
  >,
): Promise<DistributedBrowserPGlite> {
  return DistributedBrowserPGlite.open({
    nodeId,
    schema: SCHEMA,
    transport,
    partitions: [
      {
        id: "tenant:jp",
        project: async (db, event) => {
          const payload = event.payload as ReadingPayload;
          await db.query(
            `
              INSERT INTO readings (source_node, event_id, sensor_id, reading)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (source_node, event_id) DO NOTHING
            `,
            [event.sourceNodeId, event.eventId, payload.sensorId, payload.reading],
          );
        },
      },
    ],
    ...extra,
  });
}

async function readings(db: DistributedBrowserPGlite) {
  const result = await db.query<{
    source_node: string;
    event_id: string;
    sensor_id: string;
    reading: number;
  }>(
    `
      SELECT source_node, event_id, sensor_id, reading
      FROM readings
      ORDER BY source_node, event_id
    `,
  );
  return result.rows;
}

class TestBrowserCasBlockStore implements BrowserCasBlockStore {
  private readonly blocks = new Map<string, Uint8Array>();

  async readBlock(hash: string): Promise<Uint8Array> {
    const block = this.blocks.get(hash);
    if (!block) {
      throw new Error(`block not found: ${hash}`);
    }
    return new Uint8Array(block);
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    this.blocks.set(hash, new Uint8Array(content));
  }

  async listBlockHashes(): Promise<Set<string>> {
    return new Set(this.blocks.keys());
  }

  async deleteBlock(hash: string): Promise<void> {
    this.blocks.delete(hash);
  }
}
