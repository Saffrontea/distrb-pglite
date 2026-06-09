import { MemoryTransport } from "@distrb-pglite/core";
import { describe, expect, it } from "vitest";
import {
  DistributedBrowserPGlite,
  snapshotBrowserFilesystemToCas,
  type BrowserCasBlockStore,
} from "../src/index.js";

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
      const manifest = await snapshotBrowserFilesystemToCas({
        fs: nodeA.fs.asSnapshotDataFilesystem(),
        casStore: sourceCasStore,
        snapshotId: "snapshot-browser-a",
      });

      const restored = await nodeB.restoreFromCasSnapshot({
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
      expect(restored.transferredBlocks.length).toBeGreaterThan(0);
    } finally {
      await nodeA.close();
      await nodeB.close();
    }
  });
});

function openNode(nodeId: string, transport: MemoryTransport): Promise<DistributedBrowserPGlite> {
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
