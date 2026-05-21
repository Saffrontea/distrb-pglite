import { InMemoryPartitionStore } from "@distrb-pglite/core";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  createPartitionCasSnapshot,
  highWatermarkMap,
  listEventsAfterPartitionSnapshot,
} from "../src/index.js";

describe("partition CAS snapshots", () => {
  it("binds a CAS manifest to a partition high-watermark vector", async () => {
    const root = await mkdtemp(join(tmpdir(), "distrb-pglite-partition-snapshot-"));
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "db-file"), "snapshot-state");

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

    const snapshot = await createPartitionCasSnapshot({
      store,
      partitionId: "tenant:jp",
      dataDir,
      casDir,
      chunkSize: 8,
    });

    expect(snapshot.version).toBe(1);
    expect(snapshot.partitionId).toBe("tenant:jp");
    expect(snapshot.highWatermark).toEqual([
      ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
      ["node-b", "018bcfe5-6800-7001-8000-000000000003"],
    ]);
    expect(snapshot.manifest.files).toHaveLength(1);
    expect(highWatermarkMap(snapshot)).toEqual(new Map(snapshot.highWatermark));
  });

  it("uses snapshot high-watermarks as replay cursors for the same partition", async () => {
    const root = await mkdtemp(join(tmpdir(), "distrb-pglite-partition-replay-"));
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "db-file"), "snapshot-state");

    const snapshotStore = new InMemoryPartitionStore("node-snapshot");
    await snapshotStore.registerPartition("tenant:jp");
    await snapshotStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });

    const snapshot = await createPartitionCasSnapshot({
      store: snapshotStore,
      partitionId: "tenant:jp",
      dataDir,
      casDir,
    });

    const peerStore = new InMemoryPartitionStore("node-peer");
    await peerStore.registerPartition("tenant:jp");
    await peerStore.registerPartition("tenant:us");
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a2" },
    });
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000004",
      sourceNodeId: "node-a",
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { value: "us" },
    });

    const replay = await listEventsAfterPartitionSnapshot(peerStore, snapshot);

    expect(replay.map((event) => event.payload)).toEqual([
      { value: "a2" },
      { value: "b1" },
    ]);
  });
});
